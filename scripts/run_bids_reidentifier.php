#!/usr/bin/env php
<?php
/**
 * BIDS Reidentifier Runner
 *
 * Calls POST /cbigr_api/script/bidsreidentifier (async via SPM) and
 * polls until complete. Relabels sub-{ExtStudyID} → sub-{PSCID}.
 * Run AFTER run_bids_participant_sync.php, BEFORE run_imaging_pipeline.php.
 *
 * Usage:
 *   php scripts/run_bids_reidentifier.php [OPTIONS]
 *
 * Examples:
 *   php scripts/run_bids_reidentifier.php --all
 *   php scripts/run_bids_reidentifier.php --collection=archimedes --project=FDG-PET
 *   php scripts/run_bids_reidentifier.php --collection=archimedes --project=FDG-PET --dry-run
 *   php scripts/run_bids_reidentifier.php --collection=archimedes --project=FDG-PET --confirm
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\BidsReidentifier;

// ── Parse CLI options (same pattern as run_clinical_pipeline.php) ─────────────
$opts = getopt('', [
    'collection::',
    'project::',
    'all::',
    'dry-run::',
    'verbose::',
    'confirm::',
    'force::',
    'help::',
]);

if (isset($opts['help'])) {
    echo <<<'HELP'

BIDS Reidentifier

Relabels BIDS data from ExtStudyIDs to LORIS PSCIDs via CBIGR Script API (async).
Run AFTER run_bids_participant_sync.php, BEFORE run_imaging_pipeline.php.

Usage:
  php scripts/run_bids_reidentifier.php [OPTIONS]

Options:
  --collection=NAME    Process specific collection
  --project=NAME       Process specific project (requires --collection)
  --all                Process all enabled projects
  --force              Overwrite existing target directory
  --dry-run            Test mode (no changes)
  --confirm            Skip confirmation prompt
  --verbose            Debug output
  --help               Show this help

Project name resolution (in order):
  1. project.json → candidate_defaults.project  (exact LORIS Project.Name)
  2. project.json → loris_project_name
  3. project.json → project_common_name
  4. project.json → project_full_name
  5. --project flag (overrides all above)

Source/target directories resolved automatically:
  source: {mount_path}/deidentified-raw/bids/
  target: {mount_path}/deidentified-lorisid/bids/

Examples:
  # Dry run first (recommended)
  php scripts/run_bids_reidentifier.php \
      --collection=archimedes --project=FDG-PET --dry-run -v

  # Run with confirmation
  php scripts/run_bids_reidentifier.php \
      --collection=archimedes --project=FDG-PET --confirm

  # Force overwrite existing target
  php scripts/run_bids_reidentifier.php \
      --collection=archimedes --project=FDG-PET --force --confirm

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

    $dryRun  = isset($opts['dry-run']);
    $verbose = isset($opts['verbose']);
    $confirm = isset($opts['confirm']);
    $force   = isset($opts['force']);

    // Find projects
    $dataPath = $config['data_access']['base_path'] ?? $config['data_path'] ?? '/data';
    $projects = _findProjects($dataPath, $filters);

    if (empty($projects)) {
        echo "No projects found matching the specified filters.\n";
        exit(0);
    }

    // Show plan and confirm
    echo "\nBIDS Reidentifier\n";
    echo str_repeat("─", 60) . "\n";
    echo "Mode     : " . ($dryRun ? "DRY RUN" : "LIVE") . "\n";
    echo "Projects : " . count($projects) . "\n";
    foreach ($projects as $p) {
        echo "  • {$p['collection']}/{$p['project']}\n";
        echo "    source → {$p['source_dir']}\n";
        echo "    target → {$p['target_dir']}\n";
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

    // Run reidentifier for each project
    $totalErrors = [];

    foreach ($projects as $projectInfo) {
        echo "\n";
        echo str_repeat("─", 60) . "\n";
        echo "Collection : {$projectInfo['collection']}\n";
        echo "Project    : {$projectInfo['project']}\n";
        echo "Source     : {$projectInfo['source_dir']}\n";
        echo "Target     : {$projectInfo['target_dir']}\n";
        echo str_repeat("─", 60) . "\n";

        $reidentifier = new BidsReidentifier($config, $verbose);
        $stats        = $reidentifier->run(
            $projectInfo['source_dir'],
            $projectInfo['target_dir'],
            [
                'dry_run' => $dryRun,
                'force'   => $force,
                'verbose' => $verbose,
            ]
        );

        echo "  Subjects found    : {$stats['subjects_found']}\n";
        echo "  Subjects mapped   : {$stats['subjects_mapped']}\n";
        echo "  Subjects unmapped : {$stats['subjects_unmapped']}\n";
        echo "  Errors            : " . count($stats['errors']) . "\n";

        if (!empty($stats['unmapped_subjects'])) {
            echo "  Unmapped:\n";
            foreach ($stats['unmapped_subjects'] as $s) {
                echo "    ⚠ {$s}\n";
            }
        }

        $totalErrors = array_merge($totalErrors, $stats['errors']);
    }

    // Summary
    echo "\n" . str_repeat("═", 60) . "\n";
    echo $dryRun ? "  DRY RUN SUMMARY\n" : "  REIDENTIFIER SUMMARY\n";
    echo str_repeat("═", 60) . "\n";
    echo "  Projects processed : " . count($projects) . "\n";
    echo "  Total errors       : " . count($totalErrors) . "\n";
    if (!empty($totalErrors)) {
        echo "\n  Errors:\n";
        foreach ($totalErrors as $err) {
            echo "    - {$err}\n";
        }
    }
    echo str_repeat("═", 60) . "\n";

    exit(count($totalErrors) > 0 ? 1 : 0);

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
        fwrite(STDERR, "  Check 'data_access.base_path' in loris_client_config.json\n");
        exit(1);
    }

    $collections = glob($dataPath . '/*', GLOB_ONLYDIR) ?: [];

    foreach ($collections as $collectionPath) {
        $collectionName = basename($collectionPath);
        if (str_starts_with($collectionName, '.')) continue;
        if (isset($filters['collection']) && $collectionName !== $filters['collection']) continue;

        $projectDirs = glob($collectionPath . '/*', GLOB_ONLYDIR) ?: [];

        foreach ($projectDirs as $projectPath) {
            $projectName = basename($projectPath);
            if (str_starts_with($projectName, '.')) continue;
            if (isset($filters['project']) && $projectName !== $filters['project']) continue;

            $label = "{$collectionName}/{$projectName}";

            if (!file_exists("{$projectPath}/project.json")) {
                $warnings[] = "  [{$label}] SKIPPED — missing project.json";
                continue;
            }

            $projectJson = json_decode(file_get_contents("{$projectPath}/project.json"), true) ?? [];

            // Resolve actual project path via data_access.mount_path
            $resolvedPath = (!empty($projectJson['data_access']['mount_path'])
                && is_dir($projectJson['data_access']['mount_path']))
                ? rtrim($projectJson['data_access']['mount_path'], '/')
                : $projectPath;

            // Source: deidentified-raw/bids/
            $sourceDir   = null;
            $sourceTried = [];
            foreach ([
                         "{$resolvedPath}/deidentified-raw/bids",
                         "{$resolvedPath}/deidentified-raw/imaging/bids",
                     ] as $candidate) {
                $sourceTried[] = $candidate;
                if (is_dir($candidate)) {
                    $sourceDir = $candidate;
                    break;
                }
            }

            if (!$sourceDir) {
                $warnings[] = "  [{$label}] SKIPPED — no BIDS source directory found.\n"
                    . "    Tried:\n"
                    . "    - " . implode("\n    - ", $sourceTried) . "\n"
                    . "    Run run_bids_participant_sync.php first"
                    . " and place BIDS data in deidentified-raw/bids/";
                continue;
            }

            if (!file_exists("{$sourceDir}/participants.tsv")) {
                $warnings[] = "  [{$label}] SKIPPED — missing participants.tsv"
                    . " at {$sourceDir}/participants.tsv";
                continue;
            }

            // Target: deidentified-lorisid/bids/
            $targetDir = str_replace('/deidentified-raw/', '/deidentified-lorisid/', $sourceDir);

            if (is_dir($targetDir)) {
                $warnings[] = "  [{$label}] WARNING — target already exists: {$targetDir}\n"
                    . "    Use --force to overwrite.";
            }

            $projects[] = [
                'collection' => $collectionName,
                'project'    => $projectName,
                'path'       => $resolvedPath,
                'source_dir' => $sourceDir,
                'target_dir' => $targetDir,
            ];
        }
    }

    if (!empty($warnings)) {
        echo "\n";
        foreach ($warnings as $w) echo "⚠  {$w}\n";
        echo "\n";
    }

    if (isset($filters['project']) && empty($projects)) {
        $col  = $filters['collection'] ?? '?';
        $proj = $filters['project'];
        fwrite(STDERR, "ERROR: Project not found or not eligible: {$col}/{$proj}\n");
        fwrite(STDERR, "  Check:\n");
        fwrite(STDERR, "    1. Directory exists: {$dataPath}/{$col}/{$proj}\n");
        fwrite(STDERR, "    2. project.json exists with data_access.mount_path\n");
        fwrite(STDERR, "    3. deidentified-raw/bids/ exists with participants.tsv\n");
        fwrite(STDERR, "    4. Run run_bids_participant_sync.php first\n");
    }

    return $projects;
}