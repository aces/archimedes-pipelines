#!/usr/bin/env php
<?php
/**
 * BIDS Import Pipeline Runner
 *
 * Runs POST /cbigr_api/script/bidsimport (async via SPM) on
 * deidentified-lorisid/bids/, polls until complete, sends email notification.
 * Run AFTER run_bids_participant_sync.php and run_bids_reidentifier.php.
 *
 * Usage:
 *   php scripts/run_bids_import.php [OPTIONS]
 *
 * Examples:
 *   php scripts/run_bids_import.php --all
 *   php scripts/run_bids_import.php --collection=archimedes
 *   php scripts/run_bids_import.php --collection=archimedes --project=FDG-PET
 *   php scripts/run_bids_import.php --collection=archimedes --project=FDG-PET --dry-run
 */

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\BidsImportPipeline;

// ── Parse CLI options (same pattern as run_clinical_pipeline.php) ─────────────
$opts = getopt('', [
    'collection::',
    'project::',
    'all::',
    'dry-run::',
    'verbose::',
    'confirm::',
    'no-validate::',
    'help::',
]);

if (isset($opts['help'])) {
    echo <<<'HELP'

BIDS Import Pipeline

Imports BIDS data into LORIS via bidsimport endpoint (async via SPM).
Run AFTER run_bids_participant_sync.php and run_bids_reidentifier.php.

Usage:
  php scripts/run_bids_import.php [OPTIONS]

Options:
  --collection=NAME    Process specific collection
  --project=NAME       Process specific project (requires --collection)
  --all                Process all enabled projects
  --no-validate        Skip BIDS validation (--nobidsvalidation)
  --dry-run            Test mode (no changes)
  --confirm            Skip confirmation prompt
  --verbose            Debug output
  --help               Show this help

Examples:
  # Process all projects
  php scripts/run_bids_import.php --all

  # Process specific project (dry run first)
  php scripts/run_bids_import.php --collection=archimedes --project=FDG-PET --dry-run

  # Run with confirmation
  php scripts/run_bids_import.php --collection=archimedes --project=FDG-PET --confirm

  # Skip BIDS validation (useful for first run)
  php scripts/run_bids_import.php --collection=archimedes --project=FDG-PET --no-validate --confirm

Full BIDS ingestion workflow (in order):
  1. php scripts/run_bids_participant_sync.php --collection=X --project=Y --dry-run
  2. php scripts/run_bids_participant_sync.php --collection=X --project=Y --confirm
  3. php scripts/run_bids_reidentifier.php     --collection=X --project=Y --dry-run
  4. php scripts/run_bids_reidentifier.php     --collection=X --project=Y --confirm
  5. php scripts/run_bids_import.php      --collection=X --project=Y --dry-run
  6. php scripts/run_bids_import.php      --collection=X --project=Y --confirm

HELP;
    exit(0);
}

try {
    // Load configuration
    $configFile = __DIR__ . '/../config/loris_client_config.json';
    if (!file_exists($configFile)) {
        throw new Exception("Configuration file not found: {$configFile}");
    }
    $config = json_decode(file_get_contents($configFile), true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new Exception("Invalid JSON in config: " . json_last_error_msg());
    }

    // Parse filters
    $filters = [];
    if (isset($opts['collection'])) {
        $filters['collection'] = $opts['collection'];
    }
    if (isset($opts['project'])) {
        if (!isset($opts['collection'])) {
            throw new Exception("--project requires --collection");
        }
        $filters['project'] = $opts['project'];
    }

    if (!isset($opts['all']) && empty($filters)) {
        echo "Error: Must specify --all, --collection, or --collection + --project\n";
        echo "Run with --help for usage information\n";
        exit(1);
    }

    // Pipeline is gated per-project by project.json modalities (see BidsImportPipeline::isEnabled())
    // No global enable flag needed — projects with "Imaging" in modalities are processed automatically

    $dryRun  = isset($opts['dry-run']);
    $verbose = isset($opts['verbose']);
    $confirm = isset($opts['confirm']);

    $pipelineOptions = [

        'no_bids_validation' => isset($opts['no-validate']),
        'verbose'            => $verbose,
        'dry_run'            => $dryRun,
    ];

    // Find projects
    // Resolve data root from config.
// Priority: data_access.base_path → data_path → default /data
    $dataPath = $config['data_access']['base_path']
        ?? $config['data_path']
        ?? '/data';
    $projects = _findProjects($dataPath, $filters);

    if (empty($projects)) {
        echo "No projects found matching the specified filters.\n";
        exit(0);
    }

    // Show plan and confirm
    echo "\nBIDS Import Pipeline\n";
    echo str_repeat("─", 60) . "\n";
    echo "Mode     : " . ($dryRun ? "DRY RUN" : "LIVE") . "\n";
    echo "Projects : " . count($projects) . "\n";
    foreach ($projects as $p) {
        echo "  • {$p['collection']}/{$p['project']}\n";
        echo "    → {$p['bids_dir']}\n";
    }
    echo str_repeat("─", 60) . "\n";

    if (!$dryRun && !$confirm) {
        echo "Proceed? [y/N] ";
        $answer = trim(fgets(STDIN));
        if (strtolower($answer) !== 'y') {
            echo "Aborted.\n";
            exit(0);
        }
    }

    // Authenticate
    $pipeline = new BidsImportPipeline($config, $verbose);
    $username = $config['loris']['username']
        ?? $config['api']['username']
        ?? $config['loris_user']
        ?? getenv('LORIS_USER');
    $password = $config['loris']['password']
        ?? $config['api']['password']
        ?? $config['loris_pass']
        ?? getenv('LORIS_PASS');

    if (!$dryRun) {
        echo "Authenticating with LORIS...\n";
        if (!$pipeline->authenticate($username, $password)) {
            throw new Exception("Authentication failed");
        }
        echo "Authentication successful.\n\n";
    }

    // Run pipeline for each project
    $totalStats = [
        'projects_processed' => 0,
        'scans_found'        => 0,
        'scans_processed'    => 0,
        'scans_skipped'      => 0,
        'scans_failed'       => 0,
        'errors'             => [],
    ];

    foreach ($projects as $projectInfo) {
        echo "\n";
        echo str_repeat("─", 60) . "\n";
        echo "Collection : {$projectInfo['collection']}\n";
        echo "Project    : {$projectInfo['project']}\n";
        echo "BIDS dir   : {$projectInfo['bids_dir']}\n";
        echo str_repeat("─", 60) . "\n";

        $stats = $pipeline->run($projectInfo['path'], $pipelineOptions);

        // Notification is dispatched by BidsImportPipeline::run() itself.
        // Do NOT call $pipeline->sendNotification() here — the class owns
        // the notification lifecycle and a second call would either send a
        // duplicate email (older versions) or log a "skipping duplicate"
        // line on every run (current version with idempotency guard).

        echo "  Scans found     : {$stats['scans_found']}\n";
        echo "  Scans processed : {$stats['scans_processed']}\n";
        echo "  Scans skipped   : {$stats['scans_skipped']}\n";
        echo "  Scans failed    : {$stats['scans_failed']}\n";
        if (!empty($stats['job_id'])) {
            echo "  SPM job_id      : {$stats['job_id']}"
                . (!empty($stats['pid']) ? " (pid={$stats['pid']})" : "") . "\n";
        }

        $totalStats['projects_processed']++;
        $totalStats['scans_found']     += $stats['scans_found'];
        $totalStats['scans_processed'] += $stats['scans_processed'];
        $totalStats['scans_skipped']   += $stats['scans_skipped'];
        $totalStats['scans_failed']    += $stats['scans_failed'];
        $totalStats['errors']           = array_merge($totalStats['errors'], $stats['errors']);
    }

    // Summary
    echo "\n" . str_repeat("═", 60) . "\n";
    echo $dryRun ? "  DRY RUN SUMMARY\n" : "  IMAGING PIPELINE SUMMARY\n";
    echo str_repeat("═", 60) . "\n";
    echo "  Projects processed : {$totalStats['projects_processed']}\n";
    echo "  Scans found        : {$totalStats['scans_found']}\n";
    echo "  Scans processed    : {$totalStats['scans_processed']}\n";
    echo "  Scans skipped      : {$totalStats['scans_skipped']}\n";
    echo "  Scans failed       : {$totalStats['scans_failed']}\n";
    echo "  Errors             : " . count($totalStats['errors']) . "\n";

    if (!empty($totalStats['errors'])) {
        echo "\n  Errors:\n";
        foreach ($totalStats['errors'] as $err) {
            echo "    - {$err}\n";
        }
    }

    echo str_repeat("═", 60) . "\n";

    $exitCode = ($totalStats['scans_failed'] > 0 || !empty($totalStats['errors'])) ? 1 : 0;
    exit($exitCode);

} catch (Exception $e) {
    fwrite(STDERR, "FATAL ERROR: {$e->getMessage()}\n");
    exit(1);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function _findProjects(string $dataPath, array $filters): array
{
    $projects = [];
    $warnings = [];

    if (!is_dir($dataPath)) {
        fwrite(STDERR, "ERROR: data_path not found: {$dataPath}\n");
        fwrite(STDERR, "  Check 'data_path' in loris_client_config.json\n");
        exit(1);
    }

    $collections = glob($dataPath . '/*', GLOB_ONLYDIR) ?: [];

    if (empty($collections)) {
        fwrite(STDERR, "WARNING: No collection directories found in {$dataPath}\n");
        return [];
    }

    foreach ($collections as $collectionPath) {
        $collectionName = basename($collectionPath);
        if (str_starts_with($collectionName, '.')) continue;
        if (isset($filters['collection']) && $collectionName !== $filters['collection']) continue;

        $projectDirs = glob($collectionPath . '/*', GLOB_ONLYDIR) ?: [];

        if (empty($projectDirs)) {
            $warnings[] = "No project directories found in collection: {$collectionName}";
            continue;
        }

        foreach ($projectDirs as $projectPath) {
            $projectName = basename($projectPath);
            if (str_starts_with($projectName, '.')) continue;
            if (isset($filters['project']) && $projectName !== $filters['project']) continue;

            $label = "{$collectionName}/{$projectName}";

            // project.json required
            if (!file_exists("{$projectPath}/project.json")) {
                $warnings[] = "  [{$label}] SKIPPED — missing project.json at {$projectPath}/project.json";
                continue;
            }

            $projectJson = json_decode(file_get_contents("{$projectPath}/project.json"), true) ?? [];

            // Use data_access.mount_path from project.json when available.
            // Prevents double-collection paths when data_path already contains
            // the collection name (e.g. data_path=/data/archimedes but
            // mount_path=/data/archimedes/FDG-PET).
            $resolvedPath = (!empty($projectJson['data_access']['mount_path'])
                && is_dir($projectJson['data_access']['mount_path']))
                ? rtrim($projectJson['data_access']['mount_path'], '/')
                : $projectPath;

            // Check imaging modality enabled in project.json
            $modalities = array_map('strtolower', array_map('trim',
                $projectJson['modalities'] ?? []
            ));
            if (!in_array('imaging', $modalities)) {
                $warnings[] = "  [{$label}] SKIPPED — 'Imaging' not listed in project.json modalities";
                continue;
            }

            // Find BIDS lorisid directory (reidentified output)
            $bidsDir     = null;
            $bidsTried   = [];
            foreach ([
                         "{$resolvedPath}/deidentified-lorisid/bids",
                         "{$resolvedPath}/deidentified-lorisid/imaging/bids",
                     ] as $candidate) {
                $bidsTried[] = $candidate;
                if (is_dir($candidate)) {
                    $bidsDir = $candidate;
                    break;
                }
            }

            if (!$bidsDir) {
                $warnings[] = "  [{$label}] SKIPPED — no reidentified BIDS directory found. Tried:\n"
                    . "    - " . implode("\n    - ", $bidsTried) . "\n"
                    . "    Run run_bids_reidentifier.php first to populate deidentified-lorisid/bids/.";
                continue;
            }

            // Warn if no sub-* directories (empty dataset)
            $subjects = glob("{$bidsDir}/sub-*", GLOB_ONLYDIR) ?: [];
            if (empty($subjects)) {
                $warnings[] = "  [{$label}] WARNING — no sub-* directories found in {$bidsDir}\n"
                    . "    Reidentification may have failed or produced no output.";
            }

            // Warn if dataset_description.json is missing (bidsimport may fail)
            if (!file_exists("{$bidsDir}/dataset_description.json")) {
                $warnings[] = "  [{$label}] WARNING — missing dataset_description.json in {$bidsDir}\n"
                    . "    bidsimport requires dataset_description.json. Add it before importing.";
            }

            $projects[] = [
                'collection' => $collectionName,
                'project'    => $projectName,
                'path'       => $resolvedPath,
                'bids_dir'   => $bidsDir,
            ];
        }
    }

    // Print all warnings
    if (!empty($warnings)) {
        echo "\n";
        foreach ($warnings as $w) {
            echo "⚠  " . $w . "\n";
        }
        echo "\n";
    }

    if (isset($filters['project']) && empty($projects)) {
        $col  = $filters['collection'] ?? '?';
        $proj = $filters['project'];
        fwrite(STDERR, "ERROR: Project not found or not eligible: {$col}/{$proj}\n");
        fwrite(STDERR, "  Check:\n");
        fwrite(STDERR, "    1. Directory exists: {$dataPath}/{$col}/{$proj}\n");
        fwrite(STDERR, "    2. project.json exists and has 'Imaging' in modalities\n");
        fwrite(STDERR, "    3. deidentified-lorisid/bids/ exists with sub-* directories\n");
        fwrite(STDERR, "    4. Run run_bids_reidentifier.php first\n");
    }

    return $projects;
}