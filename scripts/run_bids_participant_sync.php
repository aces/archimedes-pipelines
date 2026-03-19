#!/usr/bin/env php
<?php
/**
 * BIDS Participant Sync Runner
 *
 * Reads participants.tsv, creates missing LORIS candidates, and maps
 * ExtStudyIDs. Run BEFORE run_bids_reidentifier.php.
 *
 * Usage:
 *   php scripts/run_bids_participant_sync.php [OPTIONS]
 *
 * Examples:
 *   php scripts/run_bids_participant_sync.php --all
 *   php scripts/run_bids_participant_sync.php --collection=archimedes
 *   php scripts/run_bids_participant_sync.php --collection=archimedes --project=FDG-PET
 *   php scripts/run_bids_participant_sync.php --collection=archimedes --project=FDG-PET --dry-run
 *   php scripts/run_bids_participant_sync.php --all --dry-run --verbose
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\BidsParticipantSync;

// ── Parse CLI options (same pattern as run_clinical_pipeline.php) ─────────────
$opts = getopt('', [
    'collection::',
    'project::',
    'all::',
    'dry-run::',
    'verbose::',
    'confirm::',
    'help::',
]);

if (isset($opts['help'])) {
    echo <<<'HELP'

BIDS Participant Sync

Reads participants.tsv, creates missing LORIS candidates, and maps ExtStudyIDs.
Run BEFORE run_bids_reidentifier.php.

Usage:
  php scripts/run_bids_participant_sync.php [OPTIONS]

Options:
  --collection=NAME    Process specific collection
  --project=NAME       Process specific project (requires --collection)
  --all                Process all enabled projects
  --dry-run            Test mode (no changes)
  --confirm            Skip confirmation prompt
  --verbose            Debug output
  --help               Show this help

Examples:
  # Process all projects
  php scripts/run_bids_participant_sync.php --all

  # Process specific collection
  php scripts/run_bids_participant_sync.php --collection=archimedes

  # Process specific project
  php scripts/run_bids_participant_sync.php --collection=archimedes --project=FDG-PET

  # Dry run first (recommended)
  php scripts/run_bids_participant_sync.php --collection=archimedes --project=FDG-PET --dry-run

  # Run and confirm
  php scripts/run_bids_participant_sync.php --collection=archimedes --project=FDG-PET --confirm

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

    // Show what will be processed and confirm
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

    echo "\nBIDS Participant Sync\n";
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

    // Run sync for each project
    $totalStats = [
        'created'            => 0,
        'already_exists'     => 0,
        'external_id_linked' => 0,
        'errors'             => [],
    ];

    foreach ($projects as $projectInfo) {
        echo "\n";
        echo str_repeat("─", 60) . "\n";
        echo "Collection : {$projectInfo['collection']}\n";
        echo "Project    : {$projectInfo['project']}\n";
        echo "BIDS dir   : {$projectInfo['bids_dir']}\n";
        echo str_repeat("─", 60) . "\n";

        $sync  = new BidsParticipantSync($config);
        $stats = $sync->run($projectInfo['bids_dir'], $dryRun, $projectInfo['path']);

        $totalStats['created']            += $stats['created'];
        $totalStats['already_exists']     += $stats['already_exists'];
        $totalStats['external_id_linked'] += $stats['external_id_linked'];
        $totalStats['errors']              = array_merge(
            $totalStats['errors'],
            $stats['errors']
        );
    }

    // Summary
    echo "\n" . str_repeat("═", 60) . "\n";
    echo $dryRun ? "  DRY RUN SUMMARY\n" : "  SYNC SUMMARY\n";
    echo str_repeat("═", 60) . "\n";
    echo "  Projects processed  : " . count($projects) . "\n";
    echo "  Already mapped      : {$totalStats['already_exists']}\n";
    echo "  Newly created       : {$totalStats['created']}\n";
    echo "  ExternalIDs linked  : {$totalStats['external_id_linked']}\n";
    echo "  Errors              : " . count($totalStats['errors']) . "\n";
    echo str_repeat("═", 60) . "\n";

    exit(count($totalStats['errors']) > 0 ? 1 : 0);

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

        // Filter by collection if specified
        if (isset($filters['collection']) && $collectionName !== $filters['collection']) continue;

        $projectDirs = glob($collectionPath . '/*', GLOB_ONLYDIR) ?: [];

        if (empty($projectDirs)) {
            $warnings[] = "No project directories found in collection: {$collectionName}";
            continue;
        }

        foreach ($projectDirs as $projectPath) {
            $projectName = basename($projectPath);
            if (str_starts_with($projectName, '.')) continue;

            // Filter by project if specified
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

            // Find BIDS source directory (deidentified-raw/bids/)
            $bidsDir    = null;
            $candidates = [
                "{$resolvedPath}/deidentified-raw/bids",
                "{$resolvedPath}/deidentified-raw/imaging/bids",
            ];
            foreach ($candidates as $candidate) {
                if (is_dir($candidate)) {
                    $bidsDir = $candidate;
                    break;
                }
            }

            if (!$bidsDir) {
                $warnings[] = "  [{$label}] SKIPPED — no BIDS source directory found. Tried:\n"
                    . "    - {$resolvedPath}/deidentified-raw/bids\n"
                    . "    - {$resolvedPath}/deidentified-raw/imaging/bids\n"
                    . "    Run BIDS data must be in deidentified-raw/bids/ before syncing candidates.";
                continue;
            }

            // participants.tsv required
            if (!file_exists("{$bidsDir}/participants.tsv")) {
                $warnings[] = "  [{$label}] SKIPPED — missing participants.tsv at {$bidsDir}/participants.tsv\n"
                    . "    participants.tsv is required to determine candidate list.";
                continue;
            }

            $projects[] = [
                'collection' => $collectionName,
                'project'    => $projectName,
                'path'       => $resolvedPath,
                'bids_dir'   => $bidsDir,
            ];
        }
    }

    // Print all warnings after scanning
    if (!empty($warnings)) {
        echo "\n";
        foreach ($warnings as $w) {
            echo "⚠  " . $w . "\n";
        }
        echo "\n";
    }

    // If a specific project was requested and not found, give clear error
    if (isset($filters['project']) && empty($projects)) {
        $col  = $filters['collection'] ?? '?';
        $proj = $filters['project'];
        fwrite(STDERR, "ERROR: Project not found or not eligible: {$col}/{$proj}\n");
        fwrite(STDERR, "  Check:\n");
        fwrite(STDERR, "    1. Directory exists: {$dataPath}/{$col}/{$proj}\n");
        fwrite(STDERR, "    2. project.json exists in that directory\n");
        fwrite(STDERR, "    3. deidentified-raw/bids/ exists with participants.tsv\n");
    }

    return $projects;
}
