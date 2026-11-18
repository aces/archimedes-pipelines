<?php

/**
 * run_png_reidentifier.php
 *
 * Step 2 of the PNG pipeline: for each linked visit, create the LORIS session
 * and copy its images (byte-identical, verified) to
 *   deidentified-lorisid/imaging/png/sub-<PSCID>/ses-<Visit>/
 * with participants.tsv and sessions.tsv in the layout the LORIS PNG import
 * reads. Provenance (LORIS copy <- the file the site sent) is written to
 * deidentified-lorisid/imaging/png_visits_lorisid.json and png_provenance.tsv.
 *
 * See PngReidentifier for the rules. Dry run unless --confirm.
 *
 * Exit: 0 ok (held-back visits are warnings), 1 a candidate/session/copy
 * error or a data issue (the bundle then stops before the import), 2 usage.
 *
 * PHP Version 8.1
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Png\PngCli;
use LORIS\Pipelines\Png\PngDelivery;
use LORIS\Pipelines\Shared\ProjectLock;
use LORIS\Pipelines\Png\PngPaths;
use LORIS\Pipelines\Png\PngPreflight;
use LORIS\Pipelines\Png\PngReidentifier;

$usage = <<<TXT
Usage: php scripts/png/run_png_reidentifier.php --collection=NAME [--project=NAME] [options]

Creates each visit's LORIS session and copies its images to
deidentified-lorisid/imaging/png/sub-<PSCID>/ses-<Visit>/.

  --confirm             execute (default: dry run)
  --force               redo every visit, and re-assign visits whose identifier changed
  --create-candidates   create a missing candidate here instead of in participant sync
  --exclude-file=FILE   visits/participants to leave out (written by the bundle)
  --config=FILE  -v

TXT;

$cli     = PngCli::options(['force', 'create-candidates', 'exclude-file:'], $usage);
$opts    = $cli['options'];
$confirm = $cli['confirm'];
$config  = PngCli::config($cli['config_path']);
$exit    = PngCli::EXIT_OK;
$lock    = null;

if (!$confirm) {
    PngCli::err("DRY RUN - no session created, no file written. Pass --confirm to execute.\n");
}

foreach (PngCli::targets($config, $opts) as $t) {
    PngCli::err("=== {$t['collection']}/{$t['project']} ===");
    $projectConfig = PngCli::projectConfig($t['path']);
    if ($projectConfig === null) {
        PngCli::err('  ERROR project.json missing or not valid JSON');
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }
    if (!is_dir(PngPaths::of($t['path'], PngPaths::RAW_SUBDIR))) {
        PngCli::err('  SKIP no PNG delivery (' . PngPaths::RAW_SUBDIR . "/ does not exist)\n");
        continue;
    }

    // One PNG run per project at a time; dry runs do not lock.
    if (!ProjectLock::enter($lock, $t['path'], 'png', basename(__FILE__), $confirm)) {
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    $manifest   = PngDelivery::scan($t['path'], $projectConfig);
    $exclusions = PngPreflight::readExclusions($opts['exclude-file'] ?? null);

    // Data issues not excluded by the operator: nothing is done (as DICOM).
    $pre = PngPreflight::check($manifest, $projectConfig, PngCli::visitLabels($projectConfig), $exclusions);
    if ($pre['errors'] !== []) {
        PngCli::err(sprintf('  STOP %d data issue(s) - nothing reidentified (run scripts/test/validate_png.php, or the '
            . 'bundle to continue with the good visits):', count($pre['errors'])));
        foreach ($pre['errors'] as $e) {
            PngCli::err("    ERROR {$e}");
        }
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    try {
        $api = PngCli::api($config, $projectConfig);
    } catch (\Throwable $e) {
        PngCli::err('  ERROR ' . $e->getMessage() . "\n");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    $r = new PngReidentifier($t['path'], $api);
    $r->setDryRun(!$confirm);
    $r->setForce(isset($opts['force']));
    $r->setCreateCandidates(isset($opts['create-candidates']));
    $r->setExcluded($exclusions['visits'], $exclusions['subjects']);

    try {
        $r->map($manifest, PngCli::rowLookup($manifest, $projectConfig));
    } catch (\Throwable $e) {
        PngCli::err('  ERROR ' . $e->getMessage() . "\n");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    $s = $r->getStats();
    PngCli::err(sprintf('  %d visit(s): %d %s, %d unchanged, %d held back, %d failed, %d unlinked (not processed), '
        . '%d excluded; %d image(s) copied, %d set aside', $s['visits_total'], $s['visits_mapped'],
        $confirm ? 'reidentified' : 'would be reidentified', $s['visits_unchanged'], $s['visits_held_back'],
        $s['visits_failed'], $s['visits_unlinked'], $s['visits_excluded'], $s['images_copied'], $s['images_set_aside']));
    if ($confirm) {
        PngCli::err('  output: ' . PngPaths::of($t['path'], PngPaths::LORISID_SUBDIR));
    }
    PngCli::err('');
    if ($r->hasErrors()) {
        $exit = PngCli::EXIT_FAILURE;
    }
}

if (!$confirm) {
    PngCli::err("Dry run complete. Nothing was written.\nTo execute, run:\n\n  " . PngCli::confirmCommand($argv) . "\n");
}

exit($exit);
