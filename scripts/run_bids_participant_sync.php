#!/usr/bin/env php
<?php
/**
 * Direct BIDS Participant Sync Runner
 *
 * Simple runner that takes a BIDS directory and optional --project/--site overrides
 *
 * Usage:
 *   php run_bids_sync_direct.php /path/to/bids [OPTIONS]
 *   php run_bids_sync_direct.php /path/to/bids --project="FDG PET" --site="UOHI"
 *   php run_bids_sync_direct.php /path/to/bids --dry-run -v
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\BidsParticipantSync;

// Parse arguments
$bidsDir = null;
$project = null;
$dryRun = false;
$verbose = false;

for ($i = 1; $i < $argc; $i++) {
    $arg = $argv[$i];

    if ($arg === '--dry-run') {
        $dryRun = true;
    } elseif ($arg === '-v' || $arg === '--verbose') {
        $verbose = true;
    } elseif (strpos($arg, '--project=') === 0) {
        $project = substr($arg, 10);
    } elseif ($arg === '--help' || $arg === '-h') {
        printHelp();
        exit(0);
    } elseif ($arg[0] !== '-') {
        $bidsDir = $arg;
    }
}

if (!$bidsDir) {
    fwrite(STDERR, "Error: BIDS directory required\n\n");
    printHelp();
    exit(1);
}

if (!is_dir($bidsDir)) {
    fwrite(STDERR, "Error: BIDS directory not found: {$bidsDir}\n");
    exit(1);
}

// Load config
$configFile = __DIR__ . '/../config/loris_client_config.json';
if (!file_exists($configFile)) {
    // Try alternative paths
    $configFile = __DIR__ . '/../loris_client_config.json';
}
if (!file_exists($configFile)) {
    $configFile = '/opt/archimedes-pipelines/config/loris_client_config.json';
}
if (!file_exists($configFile)) {
    $configFile = '/opt/archimedes-pipelines/loris_client_config.json';
}
if (!file_exists($configFile)) {
    fwrite(STDERR, "Configuration file not found.\n");
    fwrite(STDERR, "Tried:\n");
    fwrite(STDERR, "  - " . __DIR__ . "/../config/loris_client_config.json\n");
    fwrite(STDERR, "  - " . __DIR__ . "/../loris_client_config.json\n");
    fwrite(STDERR, "  - /opt/archimedes-pipelines/config/loris_client_config.json\n");
    fwrite(STDERR, "  - /opt/archimedes-pipelines/loris_client_config.json\n");
    exit(1);
}

$config = json_decode(file_get_contents($configFile), true);
if (json_last_error() !== JSON_ERROR_NONE) {
    fwrite(STDERR, "Invalid JSON in configuration: " . json_last_error_msg() . "\n");
    exit(1);
}

// Add command-line overrides to config
if ($project !== null) {
    $config['cli_overrides']['project'] = $project;
}

// Run sync
try {
    $sync = new BidsParticipantSync($config);
    $stats = $sync->run($bidsDir, $dryRun);

    echo "\n";
    echo str_repeat("═", 70) . "\n";
    echo "  SUMMARY\n";
    echo str_repeat("═", 70) . "\n";
    echo "  Created          : {$stats['created']}\n";
    echo "  Already exists   : {$stats['already_exists']}\n";
    echo "  ExternalIDs linked : {$stats['external_id_linked']}\n";
    echo "  Errors           : " . count($stats['errors']) . "\n";
    echo str_repeat("═", 70) . "\n";

    exit(count($stats['errors']) > 0 ? 1 : 0);

} catch (\Throwable $e) {
    fwrite(STDERR, "FATAL ERROR: " . $e->getMessage() . "\n");
    if ($verbose) {
        fwrite(STDERR, $e->getTraceAsString() . "\n");
    }
    exit(1);
}

function printHelp(): void
{
    echo <<<HELP
Direct BIDS Participant Sync Runner

Usage:
    php run_bids_sync_direct.php <bids_directory> [OPTIONS]

Arguments:
    <bids_directory>        Path to BIDS root directory

Options:
    --project=NAME          Override project name (instead of project.json or CSV)
    --dry-run               Test mode - no actual changes
    -v, --verbose           Verbose output
    -h, --help              Show this help

Priority for project:
    1. CSV column (participants.tsv: project column)
    2. Command-line argument (--project)
    3. project.json file (project field)
    4. Error if none found

Site MUST be in participants.tsv:
    - site column is REQUIRED in participants.tsv
    - No fallback from CLI or project.json

Examples:
    # Use project from CSV or project.json
    php run_bids_sync_direct.php /data/bids

    # Override project via CLI
    php run_bids_sync_direct.php /data/bids --project="FDG PET"

    # Dry run with project override
    php run_bids_sync_direct.php /data/bids --project="FDG PET" --dry-run -v

participants.tsv must have:
    participant_id    BIDS subject ID
    external_id       LORIS external ID / PSCID
    sex               Male/Female
    site              LORIS site (REQUIRED - no fallback)
    project           LORIS project (optional if using --project or project.json)

HELP;
}