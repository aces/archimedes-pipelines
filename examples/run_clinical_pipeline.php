#!/usr/bin/env php
<?php
/**
 * LORIS Clinical Data Ingestion Pipeline Runner
 * 
 * Uses LORIS API (priority) with database fallback
 */

require __DIR__ . '/../vendor/autoload.php';

use LORIS\Client\Pipelines\ClinicalPipeline;

// Parse CLI options
$opts = getopt('', ['collection::', 'project::', 'all::', 'dry-run::', 'verbose::', 'help::']);

if (isset($opts['help'])) {
    echo <<<'HELP'

LORIS Clinical Data Ingestion Pipeline

Usage:
  php examples/run_clinical_pipeline.php [OPTIONS]

Options:
  --collection=NAME    Process specific collection
  --project=NAME       Process specific project (requires --collection)
  --all               Process all enabled projects
  --dry-run           Test mode (no uploads)
  --verbose           Debug output
  --help              Show this help

Examples:
  # Process all projects
  php examples/run_clinical_pipeline.php --all

  # Process specific collection
  php examples/run_clinical_pipeline.php --collection=archimedes

  # Process specific project
  php examples/run_clinical_pipeline.php --collection=archimedes --project=FDG-PET

  # Dry run
  php examples/run_clinical_pipeline.php --all --dry-run

  # Verbose
  php examples/run_clinical_pipeline.php --all --verbose

Directory Structure:
  /data/{collection}/{project}/
  ├── project.json
  ├── deidentified-lorisid/
  │   └── clinical/              ← Place CSV files here
  │       ├── instrument1.csv
  │       └── instrument2.csv
  └── processed/
      └── clinical/2025-11-10/   ← Archived after upload
          └── instrument1.csv

Cron:
  0 2 * * * cd /opt/loris-php-client && php examples/run_clinical_pipeline.php --all

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
    $dryRun = isset($opts['dry-run']);
    $verbose = isset($opts['verbose']);
    
    $pipeline = new ClinicalPipeline($config, $dryRun, $verbose);
    
    // Run pipeline
    $exitCode = $pipeline->run($filters);
    
    exit($exitCode);
    
} catch (Exception $e) {
    fwrite(STDERR, "FATAL ERROR: {$e->getMessage()}\n");
    fwrite(STDERR, $e->getTraceAsString() . "\n");
    exit(1);
}
