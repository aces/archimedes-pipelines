#!/usr/bin/env php
<?php
/**
 * LORIS Participant Metadata Pipeline Runner
 *
 * Scans BIDS, clinical, and custom-configured sources for participant
 * metadata updates (e.g., Date of Death) and applies them to LORIS via
 * the cbigr_api/candidatesPlus PUT endpoint.
 *
 * Uses LORIS API.
 */

require __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\ParticipantMetadataPipeline;

// Parse CLI options
$opts = getopt('', ['collection::', 'project::', 'all::', 'dry-run::', 'verbose::', 'force::', 'help::']);

if (isset($opts['help'])) {
    echo <<<'HELP'

LORIS Participant Metadata Pipeline

Usage:
  php scripts/run_participant_metadata_pipeline.php [OPTIONS]

Options:
  --collection=NAME    Process specific collection
  --project=NAME       Process specific project (requires --collection)
  --all                Process all enabled projects
  --dry-run            Test mode (no PUT calls)
  --force              Bypass hash check - reprocess every source file
                       even if unchanged. LORIS still skips unchanged
                       DoD values server-side (idempotency).
  --verbose            Per-row diagnostics
  --help               Show this help

What it does:
  Scans each project's:
    - deidentified-raw/bids/participants.tsv  (default)
    - deidentified-raw/clinical/*.csv          (default, every CSV)
    - any custom sources declared in project.json under participant_metadata.sources

  For each candidate, extracts configured fields (DoD today; extensible to Sex,
  DoB, etc.) and PUTs only the values that differ from current LORIS state.

Reingestion behaviour:
  Each source file's MD5 hash is stored in
    {projectDir}/processed/participant_metadata/.participant_metadata_tracking.json

  On every run the pipeline compares the current hash to the stored one:

    No entry in tracking  ->  PROCESS  (source is new)
    Hash differs          ->  PROCESS  (source changed - reprocess)
    Hash matches          ->  SKIP     (source unchanged - nothing to do)
    --force flag set      ->  PROCESS  (always reprocess, idempotent on LORIS)

  To reset one project's tracking (force reprocess all sources next run):
    rm {projectDir}/processed/participant_metadata/.participant_metadata_tracking.json

Idempotency & duplicate handling:
  - Skips PUT if LORIS already has the new value (per-row idempotency)
  - Skips duplicates within a run (same candidate+field+value across sources)
  - Logs conflicts when sources disagree (first wins, conflict not flagged as error)

Examples:
  # Dry run all enabled projects (recommended first)
  php scripts/run_participant_metadata_pipeline.php --all --dry-run --verbose

  # Dry run one collection
  php scripts/run_participant_metadata_pipeline.php --collection=archimedes --dry-run --verbose

  # Live run for one project
  php scripts/run_participant_metadata_pipeline.php --collection=archimedes --project=FDG-PET

  # Force re-run all sources even if unchanged (e.g. after a LORIS-side fix)
  php scripts/run_participant_metadata_pipeline.php --collection=archimedes --project=FDG-PET --force

Directory Structure:
  /data/{collection}/{project}/
  +-- project.json
  +-- deidentified-raw/
  |   +-- bids/
  |   |   +-- participants.tsv             <- default BIDS scan
  |   +-- clinical/
  |   |   +-- deaths.csv                   <- every *.csv scanned
  |   |   +-- demographics.csv
  |   +-- registry/                        <- optional custom sources location
  |       +-- weekly_deaths.csv
  +-- processed/
  |   +-- participant_metadata/
  |       +-- .participant_metadata_tracking.json   <- hash tracking (auto-managed)
  +-- logs/
      +-- participant_metadata/
          +-- participant_metadata_run_2026-05-12.log

Cron:
  # Weekly Monday 4 AM run
  0 4 * * 1 cd /opt/archimedes-pipelines && php scripts/run_participant_metadata_pipeline.php --all >> /var/log/archimedes/participant_metadata_cron.log 2>&1

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
        throw new Exception("Invalid JSON in config file: " . json_last_error_msg());
    }

    // Parse filters
    $filters = [];

    if (isset($opts['collection'])) {
        $filters['collection'] = $opts['collection'];
    }

    if (isset($opts['project'])) {
        if (!isset($opts['collection'])) {
            throw new Exception("--project requires --collection to be specified");
        }
        $filters['project'] = $opts['project'];
    }

    if (!isset($opts['all']) && empty($filters)) {
        echo "Error: Must specify --all, --collection, or --project\n";
        echo "Run with --help for usage information\n";
        exit(1);
    }

    // Initialize pipeline
    $dryRun  = isset($opts['dry-run']);
    $verbose = isset($opts['verbose']);
    $force   = isset($opts['force']);

    $pipeline = new ParticipantMetadataPipeline($config, $dryRun, $verbose, $force);

    // Run pipeline
    $exitCode = $pipeline->run($filters);

    exit($exitCode);

} catch (Exception $e) {
    fwrite(STDERR, "FATAL ERROR: {$e->getMessage()}\n");
    fwrite(STDERR, $e->getTraceAsString() . "\n");
    exit(1);
}