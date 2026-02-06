#!/usr/bin/env php
<?php
/**
 * BIDS Reidentifier Script
 *
 * Re-labels BIDS data from external IDs to LORIS PSCIDs (or vice versa)
 * by querying the CBIGR mapper and copying data to a new directory.
 *
 * Workflow:
 *   1. Read source BIDS directory structure
 *   2. For each sub-* directory:
 *      - Query CBIGR mapper: ExternalID → PSCID
 *      - Rename sub-{ExternalID}/ → sub-{PSCID}/
 *      - Update participants.tsv with PSCIDs
 *      - Copy to target directory
 *   3. Update dataset_description.json
 *
 * Usage:
 *   php scripts/run_bids_reidentifier.php SOURCE_DIR TARGET_DIR [OPTIONS]
 *   php scripts/run_bids_reidentifier.php /data/raw/bids /data/lorisid/bids --dry-run
 *
 * @package LORIS\Scripts
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\BidsReidentifier;

// Parse arguments
$sourceDir = null;
$targetDir = null;
$project = null;
$dryRun = false;
$verbose = false;
$force = false;
$help = false;

for ($i = 1; $i < $argc; $i++) {
    $arg = $argv[$i];
    
    if ($arg === '--dry-run') {
        $dryRun = true;
    } elseif ($arg === '-v' || $arg === '--verbose') {
        $verbose = true;
    } elseif ($arg === '--force') {
        $force = true;
    } elseif ($arg === '--help' || $arg === '-h') {
        $help = true;
    } elseif (strpos($arg, '--project=') === 0) {
        $project = substr($arg, 10);
    } elseif ($arg[0] !== '-') {
        if (!$sourceDir) {
            $sourceDir = $arg;
        } elseif (!$targetDir) {
            $targetDir = $arg;
        }
    }
}

if ($help || !$sourceDir || !$targetDir) {
    showHelp();
    exit($help ? 0 : 1);
}

// Validate directories
if (!is_dir($sourceDir)) {
    fwrite(STDERR, "Error: Source directory not found: {$sourceDir}\n");
    exit(1);
}

if (file_exists($targetDir) && !$force) {
    fwrite(STDERR, "Error: Target directory already exists: {$targetDir}\n");
    fwrite(STDERR, "Use --force to overwrite\n");
    exit(1);
}

// Load configuration
$configFile = __DIR__ . '/../config/loris_client_config.json';
if (!file_exists($configFile)) {
    $configFile = '/opt/archimedes-pipelines/config/loris_client_config.json';
}
if (!file_exists($configFile)) {
    fwrite(STDERR, "Error: Config file not found\n");
    exit(1);
}

$config = json_decode(file_get_contents($configFile), true);
if (json_last_error() !== JSON_ERROR_NONE) {
    fwrite(STDERR, "Error: Invalid config JSON: " . json_last_error_msg() . "\n");
    exit(1);
}

// Run reidentification
try {
    $reidentifier = new BidsReidentifier($config, $verbose);
    
    $stats = $reidentifier->run($sourceDir, $targetDir, [
        'dry_run' => $dryRun,
        'force' => $force,
        'verbose' => $verbose,
        'project' => $project,
    ]);
    
    // Print summary
    echo "\n";
    echo str_repeat("═", 70) . "\n";
    echo $dryRun ? "DRY RUN SUMMARY\n" : "REIDENTIFICATION SUMMARY\n";
    echo str_repeat("═", 70) . "\n";
    echo "Subjects found       : " . ($stats['subjects_found'] ?? 'N/A') . "\n";
    echo "Subjects mapped      : " . ($stats['subjects_mapped'] ?? 'N/A') . "\n";
    echo "Subjects unmapped    : " . ($stats['subjects_unmapped'] ?? 'N/A') . "\n";
    echo "Files copied         : " . ($stats['files_copied'] ?? 'N/A') . "\n";
    echo "Errors               : " . count($stats['errors']) . "\n";
    
    if (!empty($stats['errors'])) {
        echo "\nErrors:\n";
        foreach ($stats['errors'] as $error) {
            echo "  - {$error}\n";
        }
    }
    
    if (!empty($stats['unmapped_subjects'])) {
        echo "\nUnmapped subjects:\n";
        foreach ($stats['unmapped_subjects'] as $subject) {
            echo "  - {$subject}\n";
        }
    }
    
    echo str_repeat("═", 70) . "\n";
    
    exit(count($stats['errors']) > 0 ? 1 : 0);
    
} catch (\Throwable $e) {
    fwrite(STDERR, "FATAL ERROR: " . $e->getMessage() . "\n");
    if ($verbose) {
        fwrite(STDERR, $e->getTraceAsString() . "\n");
    }
    exit(1);
}

function showHelp(): void
{
    echo <<<HELP
BIDS Reidentifier

Re-labels BIDS data from external IDs to LORIS PSCIDs using CBIGR Script API.

Usage:
  php scripts/run_bids_reidentifier.php <source_dir> <target_dir> [OPTIONS]

Arguments:
  <source_dir>          Source BIDS directory (with external IDs)
  <target_dir>          Target directory for reidentified data (with PSCIDs)

Options:
  --project=NAME        Project name (overrides project.json)
  --dry-run             Test mode - show what would be done without execution
  --force               Overwrite target directory if exists
  -v, --verbose         Detailed output
  -h, --help            Show this help

Project Name Priority:
  1. CLI argument (--project)
  2. project.json → project_full_name
  3. project.json → project
  4. Error if none found

Examples:
  # Dry run (recommended first)
  php scripts/run_bids_reidentifier.php \\
    /data/archimedes/FDG-PET/deidentified-raw/bids \\
    /data/archimedes/FDG-PET/deidentified-lorisid/bids \\
    --dry-run -v

  # With explicit project name
  php scripts/run_bids_reidentifier.php \\
    /data/archimedes/FDG-PET/deidentified-raw/bids \\
    /data/archimedes/FDG-PET/deidentified-lorisid/bids \\
    --project="FDG PET"

  # Force overwrite existing target
  php scripts/run_bids_reidentifier.php \\
    /data/raw/bids /data/lorisid/bids \\
    --project="My Study" --force

Workflow:
  1. Load project name from CLI or project.json
  2. Extract ID pattern from participants.tsv (e.g., "FDGP\\d{6}")
  3. Call CBIGR Script API: /cbigr_api/script/bidsreidentifier
     - mode: INTERNAL (ExternalID → PSCID)
     - project_list: From project name
  4. Script executes server-side reidentification
  5. Creates target directory with LORIS PSCIDs

API Endpoint:
  POST /cbigr_api/script/bidsreidentifier
  {
    "args": {
      "source_dir": "/data/raw/bids",
      "target_dir": "/data/lorisid/bids",
      "id_pattern": "FDGP\\\\d{6}",
      "mode": "INTERNAL",
      "project_list": "'FDG PET'"
    }
  }

Requirements:
  - Source directory must be valid BIDS dataset
  - Participants must exist in LORIS with ExternalID mappings
  - Target parent directory must be writable
  - CBIGR Script API endpoint must be available

HELP;
}
