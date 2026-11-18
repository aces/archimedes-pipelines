<?php

/**
 * run_png_import.php
 *
 * Step 3 of the PNG pipeline: ingest the reidentified visits from
 * deidentified-lorisid/imaging/png/ through POST /cbigr_api/script/importpngdataset
 * (one async LORIS job per visit, polled until it ends). See PngImportPipeline.
 *
 * Dry run unless --confirm. --validate asks LORIS to check every visit
 * without inserting anything (no --confirm needed: it changes nothing).
 *
 * Exit: 0 ok, 1 a visit failed or the run stopped, 2 usage.
 *
 * PHP Version 8.1
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Png\PngCli;
use LORIS\Pipelines\Png\PngImportPipeline;
use LORIS\Pipelines\Shared\ProjectLock;
use LORIS\Pipelines\Png\PngPaths;
use LORIS\Pipelines\Png\PngPreflight;

$usage = <<<TXT
Usage: php scripts/png/run_png_import.php --collection=NAME [--project=NAME] [options]

Ingests deidentified-lorisid/imaging/png/ into LORIS, one visit per job.

  --confirm            execute (default: dry run, lists what would be sent)
  --validate           LORIS checks every visit (import_png_dataset.py --validate-only); inserts nothing
  --force              send every visit again (safe: images already in LORIS are skipped by hash)
  --no-pic             do not create the imaging browser preview pics
  --profile=NAME       Loris-MRI database config (default project.json png.profile, else database_config.py)
  --exclude-file=FILE  visits/participants to leave out (written by the bundle)
  --no-email           do not send the project email
  --config=FILE  -v

TXT;

$cli     = PngCli::options(['validate', 'force', 'no-pic', 'profile:', 'exclude-file:', 'no-email'], $usage);
$opts    = $cli['options'];
$confirm = $cli['confirm'];
$config  = PngCli::config($cli['config_path']);
$exit    = PngCli::EXIT_OK;
$validate = isset($opts['validate']);
$lock    = null;

if (!$confirm && !$validate) {
    PngCli::err("DRY RUN - nothing sent to LORIS. Pass --confirm to execute (or --validate to let LORIS check).\n");
}

foreach (PngCli::targets($config, $opts) as $t) {
    fwrite(STDOUT, "=== {$t['collection']}/{$t['project']} ===\n");
    $projectConfig = PngCli::projectConfig($t['path']);
    if ($projectConfig === null) {
        PngCli::err('  ERROR project.json missing or not valid JSON');
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }
    if (!is_file(PngPaths::of($t['path'], PngPaths::LORISID_MANIFEST))) {
        fwrite(STDOUT, "  SKIP nothing reidentified yet (" . PngPaths::LORISID_MANIFEST . " missing)\n\n");
        continue;
    }

    // One PNG run per project at a time; dry runs and --validate-only do not lock.
    if (!ProjectLock::enter($lock, $t['path'], 'png', basename(__FILE__), $confirm && !$validate)) {
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    $png = (array) ($projectConfig['png'] ?? []);
    $exclusions = PngPreflight::readExclusions($opts['exclude-file'] ?? null);
    $pipeline = new PngImportPipeline($config, $t['path'], !$confirm, $cli['verbose']);
    $result = $pipeline->run([
        'force'             => isset($opts['force']),
        'validate_only'     => $validate,
        'create_pic'        => !isset($opts['no-pic']) && ($png['create_pic'] ?? true) !== false,
        'profile'           => (string) ($opts['profile'] ?? $png['profile'] ?? 'database_config.py'),
        'excluded_visits'   => $exclusions['visits'],
        'excluded_subjects' => $exclusions['subjects'],
        'excluded_issues'   => $exclusions['issues'],
        'poll_interval'     => (int) ($png['poll_interval_seconds'] ?? 5),
        'poll_timeout'      => (int) ($png['poll_timeout_seconds'] ?? 3600),
        'notify'            => !isset($opts['no-email']),
    ]);
    fwrite(STDOUT, "\n");

    if (in_array($result['status'], ['FAILED', 'PARTIAL'], true)) {
        $exit = PngCli::EXIT_FAILURE;
    }
}

if (!$confirm && !$validate) {
    PngCli::err("Dry run complete. Nothing was sent.\nTo execute, run:\n\n  " . PngCli::confirmCommand($argv) . "\n");
}

exit($exit);
