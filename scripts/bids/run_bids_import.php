#!/usr/bin/env php
<?php
/**
 * BIDS Import Pipeline Runner
 *
 * Runs POST /cbigr_api/script/bidsimport (async via SPM) on
 * deidentified-lorisid/bids/, polls until complete, sends email notification.
 * Run AFTER run_bids_participant_sync.php and run_bids_reidentifier.php.
 *
 * Dry run is the default; --confirm executes (same as the DICOM scripts).
 *
 * Usage:
 *   php scripts/bids/run_bids_import.php --collection=archimedes --project=FDG-PET            # dry run
 *   php scripts/bids/run_bids_import.php --collection=archimedes --project=FDG-PET --confirm
 *   php scripts/bids/run_bids_import.php --all --confirm -v
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\Cli;
use LORIS\Pipelines\Bids\BidsImportPipeline;

// ── CLI options (same as the DICOM scripts) ──────────────────────────────────
// Dry run unless --confirm. --dry-run wins over --confirm.
$shortOpts = 'v';
$longOpts  = [
    'all',
    'collection:',
    'project:',
    'confirm',
    'dry-run',
    'no-validate',
    'config:',
    'verbose',
    'help',
];
$opts = getopt($shortOpts, $longOpts);
// getopt() ignores options it does not know; a typo or a removed option
// must not run with the default instead.
Cli::exitOnUnknownOptions($shortOpts, $longOpts);

if ($opts === false) {
    fwrite(STDERR, "Invalid options. Run with --help.\n");
    exit(2);
}

if (isset($opts['help'])) {
    echo <<<HELP
Usage: php scripts/bids/run_bids_import.php [options]

Imports the reidentified BIDS data into LORIS.
Run after run_bids_reidentifier.php.

Selection:
  --all                All enabled projects
  --collection=NAME    All enabled projects in a collection
  --project=NAME       One project (requires --collection)

Execution:
  --confirm            Execute (without it, nothing is changed)
  --dry-run            Report only (the default)
  --no-validate        Skip BIDS validation before import
  --config=FILE        Config file (default: config/loris_client_config.json)
  -v, --verbose        Detailed output
  --help               Show this message

Exit codes: 0 success, 1 any failures, 2 usage error

HELP;
    exit(0);
}

try {
    // Load configuration
    $configFile = $opts['config'] ?? __DIR__ . '/../../config/loris_client_config.json';
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
            fwrite(STDERR, "--project requires --collection\n");
            exit(2);
        }
        $filters['project'] = $opts['project'];
    }

    if (!isset($opts['all']) && empty($filters)) {
        fwrite(STDERR, "Specify --all or --collection=NAME (run with --help)\n");
        exit(2);
    }

    // Pipeline is gated per-project by project.json modalities (see BidsImportPipeline::isEnabled())
    // No global enable flag needed — projects with "Imaging" in modalities are processed automatically

    $dryRun  = !isset($opts['confirm']) || isset($opts['dry-run']);
    $verbose = isset($opts['verbose']) || isset($opts['v']);

    $pipelineOptions = [

        'no_bids_validation' => isset($opts['no-validate']),
        'verbose'            => $verbose,
        'dry_run'            => $dryRun,
    ];

    // Find projects
    $projects = _findProjects($config['collections'] ?? [], $filters);

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

    if ($dryRun) {

        echo "DRY RUN - nothing will be changed. Pass --confirm to execute.\n";

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
    if ($dryRun) {
        $cmd = array_values(array_filter($argv, static fn ($a) => $a !== '--dry-run'));
        echo "\nDry run complete. Nothing was changed. To execute, run:\n\n  php "
            . implode(' ', array_map('escapeshellarg', $cmd)) . " --confirm\n\n";
    }

    exit($exitCode);

} catch (Exception $e) {
    fwrite(STDERR, "FATAL ERROR: {$e->getMessage()}\n");
    exit(1);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function _findProjects(array $collections, array $filters): array
{
    $projects = [];
    $warnings = [];

    if (empty($collections)) {
        fwrite(STDERR, "ERROR: no 'collections' defined in loris_client_config.json\n");
        exit(1);
    }

    // Only collections and projects listed in loris_client_config.json with
    // enabled=true are processed, the same rule as the clinical pipeline.
    // A disabled project is skipped even when named with --project.
    foreach ($collections as $coll) {
        if (!($coll['enabled'] ?? true)) continue;
        $collectionName = $coll['name'] ?? '';
        if (isset($filters['collection']) && $collectionName !== $filters['collection']) continue;

        $basePath = rtrim($coll['base_path'] ?? '', '/');

        foreach ($coll['projects'] ?? [] as $pc) {
            if (!($pc['enabled'] ?? true)) continue;
            $projectName = $pc['name'] ?? '';
            if (isset($filters['project']) && $projectName !== $filters['project']) continue;

            $projectPath = "{$basePath}/{$projectName}";
            $label       = "{$collectionName}/{$projectName}";

            if (!file_exists("{$projectPath}/project.json")) {
                $warnings[] = "  [{$label}] SKIPPED — missing project.json at {$projectPath}/project.json";
                continue;
            }

            $projectJson = json_decode(file_get_contents("{$projectPath}/project.json"), true) ?? [];

            // Project root comes from loris_client_config.json (collections[].base_path/<name>).
            $resolvedPath = $projectPath;

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
        fwrite(STDERR, "    1. Listed under collections in loris_client_config.json with enabled=true,\n"
            . "       and its directory exists at <collections[].base_path>/{$proj}\n");
        fwrite(STDERR, "    2. project.json exists and has 'Imaging' in modalities\n");
        fwrite(STDERR, "    3. deidentified-lorisid/bids/ exists with sub-* directories\n");
        fwrite(STDERR, "    4. Run run_bids_reidentifier.php first\n");
    }

    return $projects;
}