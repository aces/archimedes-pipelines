#!/usr/bin/env php
<?php
/**
 * LORIS Clinical Data Ingestion Pipeline Runner
 *
 * Uses LORIS API (priority) with database fallback
 */

require __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\ClinicalPipeline;

// Parse CLI options
$opts = getopt('', ['collection::', 'project::', 'instrument::', 'all::', 'dry-run::', 'verbose::', 'force::', 'help::']);

if (isset($opts['help'])) {
    echo <<<'HELP'

LORIS Clinical Data Ingestion Pipeline

Usage:
  php scripts/run_clinical_pipeline.php [OPTIONS]

Options:
  --collection=NAME    Process specific collection
  --project=NAME       Process specific project (requires --collection)
  --instrument=NAME    Process specific instrument (requires --collection and --project)
  --all                Process all enabled projects
  --dry-run            Test mode (no uploads)
  --force              Bypass hash check — re-upload all files even if unchanged
                       (LORIS will still skip rows that already exist)
  --verbose            Debug output
  --help               Show this help

Reingestion behaviour:
  Each file's MD5 hash is stored in processed/clinical/.clinical_tracking.json.
  On every run the pipeline compares the current hash to the stored one:

    No entry in tracking  →  FIRST UPLOAD   (all rows sent, LORIS inserts everything)
    Hash differs          →  RE-INGESTION   (full file sent, LORIS saves only new rows)
    Hash matches          →  SKIPPED        (file unchanged, nothing sent)
    --force flag set      →  RE-INGESTION   (always re-uploads, LORIS skips existing rows)

  To reset a single file's tracking entry (force it to re-upload next run):
    Delete its key from processed/clinical/.clinical_tracking.json

EviData privacy gate:
  When config/evidata_config.json exists AND its "enabled" field is true,
  every project run starts with a privacy check against the configured
  EviData service. Failures abort that project entirely (no LORIS write,
  no tracking update) and send a notification to the project's evidata
  recipients with the report ZIPs attached.

  When the file is missing OR enabled=false, the gate is bypassed silently
  and the pipeline behaves exactly as it did before EviData was added.

  Test the connection independently with:
    php scripts/test_evidata_connection.php

Examples:
  # Process all projects
  php scripts/run_clinical_pipeline.php --all

  # Process specific collection
  php scripts/run_clinical_pipeline.php --collection=archimedes

  # Process specific project
  php scripts/run_clinical_pipeline.php --collection=archimedes --project=FDG-PET

  # Dry run first (always recommended)
  php scripts/run_clinical_pipeline.php --all --dry-run --verbose

  # Force re-upload all files (LORIS still skips existing rows)
  php scripts/run_clinical_pipeline.php --all --force

  # Force re-upload for one project
  php scripts/run_clinical_pipeline.php --collection=archimedes --project=FDG-PET --force

Directory Structure:
  /data/{collection}/{project}/
  ├── project.json
  ├── deidentified-raw/
  │   └── clinical/                      ← Place CSV/TSV files here
  │       ├── instrument1.csv
  │       └── instrument2.csv
  └── processed/
      ├── .clinical_tracking.json        ← Hash tracking (auto-managed)
      └── clinical/2025-11-10/           ← Snapshots archived after each upload
          └── instrument1.csv

Cron:
  0 2 * * * cd /opt/archimedes-pipelines && php scripts/run_clinical_pipeline.php --all

HELP;
    exit(0);
}

try {
    // ── Load LORIS client config ────────────────────────────────────
    $configFile = __DIR__ . '/../config/loris_client_config.json';

    if (!file_exists($configFile)) {
        throw new Exception("Configuration file not found: {$configFile}");
    }

    $config = json_decode(file_get_contents($configFile), true);

    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new Exception("Invalid JSON in {$configFile}: " . json_last_error_msg());
    }

    // ── Optionally merge EviData config ─────────────────────────────
    // EviData lives in its own file so its settings can be edited
    // independently of LORIS API config. The pipeline still sees
    // $config['evidata'] exactly as if the block were inline — the
    // merge happens here at load time.
    //
    // Missing file = EviData is not configured for this run;
    // the preflight is bypassed (same as enabled=false). Bad JSON is
    // a hard fail so a typo doesn't silently disable the privacy gate.
    $evidataFile = __DIR__ . '/../config/evidata_config.json';
    if (file_exists($evidataFile)) {
        $evidataRaw     = file_get_contents($evidataFile);
        $evidataDecoded = json_decode($evidataRaw, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new Exception(
                "Invalid JSON in {$evidataFile}: " . json_last_error_msg()
            );
        }
        $config['evidata'] = $evidataDecoded;
    }

    // ── Parse filters ───────────────────────────────────────────────
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

    if (isset($opts['instrument'])) {
        if (!isset($opts['collection']) || !isset($opts['project'])) {
            throw new Exception("--instrument requires --collection and --project to be specified");
        }
        $filters['instrument'] = $opts['instrument'];
    }

    if (!isset($opts['all']) && empty($filters)) {
        echo "Error: Must specify --all, --collection, or --project\n";
        echo "Run with --help for usage information\n";
        exit(1);
    }

    // ── Initialize pipeline ─────────────────────────────────────────
    $dryRun  = isset($opts['dry-run']);
    $verbose = isset($opts['verbose']);
    $force   = isset($opts['force']);

    $pipeline = new ClinicalPipeline($config, $dryRun, $verbose, $force);

    // ── Run pipeline ────────────────────────────────────────────────
    $exitCode = $pipeline->run($filters);

    exit($exitCode);

} catch (Exception $e) {
    fwrite(STDERR, "FATAL ERROR: {$e->getMessage()}\n");
    fwrite(STDERR, $e->getTraceAsString() . "\n");
    exit(1);
}
