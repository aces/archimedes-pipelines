#!/usr/bin/env php
<?php
/**
 * BIDS Participant Sync Runner
 *
 * Reads participants.tsv, creates missing LORIS candidates, and maps
 * ExtStudyIDs. Run BEFORE run_bids_reidentifier.php.
 *
 * Dry run is the default; --confirm executes (same as the DICOM scripts).
 *
 * Usage:
 *   php scripts/bids/run_bids_participant_sync.php --collection=archimedes --project=FDG-PET            # dry run
 *   php scripts/bids/run_bids_participant_sync.php --collection=archimedes --project=FDG-PET --confirm
 *   php scripts/bids/run_bids_participant_sync.php --all --confirm -v
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\Cli;
use LORIS\Pipelines\Bids\BidsParticipantSync;

// ── CLI options (same as the DICOM scripts) ──────────────────────────────────
// Dry run unless --confirm. --dry-run wins over --confirm.
$shortOpts = 'v';
$longOpts  = [
    'all',
    'collection:',
    'project:',
    'confirm',
    'dry-run',
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
Usage: php scripts/bids/run_bids_participant_sync.php [options]

Reads participants.tsv, creates missing LORIS candidates and maps their ExtStudyIDs.
Run before run_bids_reidentifier.php.

Selection:
  --all                All enabled projects
  --collection=NAME    All enabled projects in a collection
  --project=NAME       One project (requires --collection)

Execution:
  --confirm            Execute (without it, nothing is changed)
  --dry-run            Report only (the default)
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

    $dryRun  = !isset($opts['confirm']) || isset($opts['dry-run']);
    $verbose = isset($opts['verbose']) || isset($opts['v']);

    // Show what will be processed and confirm
    $projects = _findProjects($config['collections'] ?? [], $filters);

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

    if ($dryRun) {

        echo "DRY RUN - nothing will be changed. Pass --confirm to execute.\n";

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

    if ($dryRun) {
        $cmd = array_values(array_filter($argv, static fn ($a) => $a !== '--dry-run'));
        echo "\nDry run complete. Nothing was changed. To execute, run:\n\n  php "
            . implode(' ', array_map('escapeshellarg', $cmd)) . " --confirm\n\n";
    }

    exit(count($totalStats['errors']) > 0 ? 1 : 0);

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
        fwrite(STDERR, "    1. Listed under collections in loris_client_config.json with enabled=true,\n"
            . "       and its directory exists at <collections[].base_path>/{$proj}\n");
        fwrite(STDERR, "    2. project.json exists in that directory\n");
        fwrite(STDERR, "    3. deidentified-raw/bids/ exists with participants.tsv\n");
    }

    return $projects;
}
