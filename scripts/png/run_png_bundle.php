<?php

/**
 * run_png_bundle.php
 *
 * The PNG pipeline in order, one project at a time, with the DICOM bundle's
 * rules. Three scripts (PNG needs no organize step: the delivery is already
 * one folder per visit):
 *
 *   -  scan + data issues (in the bundle itself): the delivery is read and
 *                  hashed, the scan manifest written, and every issue listed
 *                  BEFORE anything is created; whole-project issues abort;
 *                  otherwise continue with the good visits (asked, or
 *                  --continue-good) or abort
 *   1. sync        run_png_participant_sync.php candidates, linked by ExternalID
 *   2. reidentify  run_png_reidentifier.php     sessions + deidentified-lorisid/ copy
 *   3. import      run_png_import.php           POST /cbigr_api/script/importpngdataset per visit
 *
 * Stop rule: if sync or reidentify fails, nothing is ingested for the project
 * (FAILED email). One run per project at a time (lock). A delivery that changed
 * in the last imaging.delivery_quiet_minutes is left for the next run (--force
 * overrides). Dry run unless --confirm.
 *
 * Exit: 0 ok, 1 a step failed, 2 usage, 3 aborted on data issues.
 *
 * PHP Version 8.1
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\ImagingProject;
use LORIS\Pipelines\Png\PngCli;
use LORIS\Pipelines\Png\PngDelivery;
use LORIS\Pipelines\Shared\ProjectLock;
use LORIS\Pipelines\Png\PngPaths;
use LORIS\Pipelines\Png\PngPreflight;
use LORIS\Pipelines\Shared\BundleSupport;

$usage = <<<TXT
Usage: php scripts/png/run_png_bundle.php --all | --collection=NAME [--project=NAME] [options]

Runs participant sync -> reidentify -> import for PNG deliveries (after checking the delivery).

  --confirm          execute (default: dry run of every step)
  --dry-run          explicit dry run; wins over --confirm
  --continue-good    on data issues, go ahead with the good visits without asking (unattended runs)
  --force            ignore fingerprints and the upload quiet period: redo every visit
  --steps=LIST       only these of sync,reidentify,import
  --skip=LIST        all but these (--skip=import = prepare everything, ingest nothing)
  --no-pic           no imaging browser preview pics
  --config=FILE  -v

Exit codes: 0 ok, 1 a step failed, 2 usage, 3 aborted (data issues not continued)

TXT;

$cli     = PngCli::options(['continue-good', 'force', 'steps:', 'skip:', 'no-pic'], $usage);
$opts    = $cli['options'];
$confirm = $cli['confirm'];
$verbose = $cli['verbose'];
$force   = isset($opts['force']);
$config  = PngCli::config($cli['config_path']);

$allSteps = ['sync', 'reidentify', 'import'];
$steps    = isset($opts['steps']) ? array_values(array_filter(array_map('trim', explode(',', (string) $opts['steps'])))) : $allSteps;
$skip     = isset($opts['skip']) ? array_values(array_filter(array_map('trim', explode(',', (string) $opts['skip'])))) : [];
$unknown  = array_diff(array_merge($steps, $skip), $allSteps);
if ($unknown !== []) {
    PngCli::err('Unknown step(s): ' . implode(', ', $unknown) . "\nValid steps: " . implode(', ', $allSteps) . "\n");
    exit(PngCli::EXIT_USAGE);
}
$steps = array_values(array_diff($steps, $skip));
if ($steps === []) {
    PngCli::err('Every step is disabled - nothing to do.');
    exit(PngCli::EXIT_USAGE);
}
$on = static fn (string $s): bool => in_array($s, $steps, true);

// =============================================================================

if (!$confirm) {
    PngCli::err("DRY RUN - no files written, nothing ingested. Pass --confirm to execute.\n");
}

$exit = PngCli::EXIT_OK;
$lock = null;

foreach (PngCli::targets($config, $opts) as $t) {
    $label = "{$t['collection']}/{$t['project']}";
    $path  = $t['path'];
    $raw   = PngPaths::of($path, PngPaths::RAW_SUBDIR);

    if (!is_dir($raw)) {
        PngCli::err("  SKIP {$label}: no PNG delivery (" . PngPaths::RAW_SUBDIR . '/ does not exist)');
        continue;
    }
    // One PNG run per project at a time, shared with the step scripts (it is
    // handed to them); the mount is checked first. Released when the next
    // project starts or the script ends - also on a crash.
    if (!ProjectLock::enter($lock, $path, 'png', basename(__FILE__), $confirm, true, $label)) {
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }
    $wait = ImagingProject::uploadInProgress($raw, (int) ($config['imaging']['delivery_quiet_minutes'] ?? 5));
    if ($wait !== null && !$force) {
        PngCli::err("  WAIT {$label}: {$wait}");
        continue;
    }
    $projectConfig = PngCli::projectConfig($path);
    if ($projectConfig === null) {
        PngCli::err("  ERROR {$label}: project.json missing or not valid JSON");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    PngCli::err("\n========================================\n  {$label} (PNG)\n========================================");
    $logDir = PngCli::logDir($path, 'png');
    $log    = $logDir !== null ? $logDir . '/png_bundle_' . date('Y-m-d') . '.log' : null;
    BundleSupport::masterLog($log, str_repeat('=', 60));
    BundleSupport::masterLog($log, "RUN {$label} mode=" . ($confirm ? 'confirm' : 'dry-run') . ' steps=' . implode(',', $steps)
        . ($force ? ' force' : ''));

    $common = ['--collection=' . $t['collection'], '--project=' . $t['project'], '--config=' . $cli['config_path']];
    if ($confirm) {
        $common[] = '--confirm';
    }
    if ($verbose) {
        $common[] = '-v';
    }

    // --- scan + data issues: all of them, before anything is created -----------------
    $manifest = PngDelivery::scan($path, $projectConfig);
    $manifest['dry_run'] = !$confirm;
    try {
        PngPaths::writeJson(PngPaths::of($path, PngPaths::SCAN_MANIFEST), $manifest);
    } catch (\Throwable $e) {
        PngCli::err('  WARN cannot write the scan manifest: ' . $e->getMessage());
    }
    $pre      = PngPreflight::check($manifest, $projectConfig, PngCli::visitLabels($projectConfig));
    $plan     = PngPreflight::plan($manifest, $pre['issues']);
    $exFile   = null;
    $live     = PngPaths::of($path, PngPaths::EXCLUDED);

    if ($pre['issues'] === []) {
        if ($confirm && is_file($live)) {
            @unlink($live);
        }
    } else {
        $report = sprintf("\n  DATA ISSUES - found before any candidate is created (%d):\n", count($pre['issues']));
        foreach ($plan['abort'] as $m) {
            $report .= "    whole project: {$m}\n";
        }
        foreach ($plan['bad'] as $where => $reasons) {
            $report .= "    {$where}\n";
            foreach ($reasons as $r) {
                $report .= "      - {$r}\n";
            }
        }
        $report .= sprintf("  GOOD: %d visit(s) with no issue%s\n", count($plan['good']),
            $plan['good'] ? ': ' . implode(', ', array_slice($plan['good'], 0, 10)) . (count($plan['good']) > 10 ? ', ...' : '') : '');
        PngCli::err($report);
        BundleSupport::masterLog($log, $report);

        if ($plan['abort'] !== []) {
            BundleSupport::stopBeforeIngestion($label, 'data checks (whole-project issue)', $path, $config, $log, $confirm, 'png', 'images');
            $exit = PngCli::EXIT_FAILURE;
            continue;
        }
        if ($plan['good'] === []) {
            BundleSupport::stopBeforeIngestion($label, 'data checks (no visit without issues)', $path, $config, $log, $confirm, 'png', 'images');
            $exit = PngCli::EXIT_FAILURE;
            continue;
        }
        $q = sprintf("\n  Continue with the %d good visit(s)? The %d with issues are NOT created, reidentified or\n"
            . '  ingested in this run; fix them and re-run to add them.', count($plan['good']), count($plan['exclude_visits']));
        if (!$confirm) {
            PngCli::err($q . ' [dry run - not asked; the preview continues without them]');
        } elseif (!BundleSupport::confirmPrompt($q, isset($opts['continue-good']), '--continue-good')) {
            PngCli::err("\n  ABORTED - nothing created or ingested for {$label}.");
            BundleSupport::masterLog($log, 'ABORTED - data issues, not continued');
            BundleSupport::stopBeforeIngestion($label, 'data checks (not continued)', $path, $config, $log, $confirm, 'png', 'images');
            $exit = PngCli::EXIT_ABORTED;
            continue;
        }
        $exFile = $confirm ? $live : (tempnam(sys_get_temp_dir(), 'png_excl_') ?: $live);
        PngPaths::writeJson($exFile, ['generated_at' => date('c'), 'visits' => $plan['exclude_visits'],
            'subjects' => $plan['exclude_subjects'], 'issues' => $plan['bad']]);
        BundleSupport::masterLog($log, sprintf('continuing with %d good visit(s), %d excluded', count($plan['good']),
            count($plan['exclude_visits'])));
    }
    $exArgs = $exFile !== null ? ['--exclude-file=' . $exFile] : [];

    $c = $manifest['counts'];
    $line = sprintf('%d visit(s): %d matched to a participant, %d unlinked, %d blocked; %d image(s), %d set aside',
        $c['visits'], $c['linked'], $c['unlinked'], $c['blocked'], $c['images'], $c['set_aside']);
    PngCli::err("\n  {$line}");
    BundleSupport::masterLog($log, $line);
    foreach ($pre['warnings'] as $w) {
        PngCli::err('    ' . (str_starts_with($w, 'UNLINKED ') || str_contains($w, 'SET ASIDE') ? '' : 'warning ') . $w);
        BundleSupport::masterLog($log, "  {$w}");
    }

    $withForce = $force ? array_merge($common, ['--force']) : $common;

    if ($c['linked'] > 0) {
        // --- 1. participant sync -------------------------------------------------------
        if ($on('sync')) {
            if (BundleSupport::runStep(__DIR__, 'run_png_participant_sync.php', array_merge($common, $exArgs), $log, $lock) !== 0) {
                BundleSupport::stopBeforeIngestion($label, 'participant sync', $path, $config, $log, $confirm, 'png', 'images');
                $exit = PngCli::EXIT_FAILURE;
                continue;
            }
        }
        // --- 2. reidentify ---------------------------------------------------------------
        if ($on('reidentify')) {
            if (BundleSupport::runStep(__DIR__, 'run_png_reidentifier.php', array_merge($withForce, $exArgs), $log, $lock) !== 0) {
                BundleSupport::stopBeforeIngestion($label, 'reidentification', $path, $config, $log, $confirm, 'png', 'images');
                $exit = PngCli::EXIT_FAILURE;
                continue;
            }
        }
    } else {
        PngCli::err('  SKIP participant sync and reidentify - no visit is matched to a participant');
    }

    // --- 3. import (only from deidentified-lorisid/) -----------------------------------
    if (!$on('import')) {
        PngCli::err("  SKIP import (disabled) - {$label} prepared, not ingested");
        continue;
    }
    if (!is_file(PngPaths::of($path, PngPaths::LORISID_MANIFEST))) {
        PngCli::err($confirm ? "  nothing reidentified - nothing to import\n"
            : "  import (dry run): reidentify writes deidentified-lorisid/ with --confirm; the import follows it\n");
        continue;
    }
    if (!$confirm) {
        PngCli::err("  import (dry run): lists what is already reidentified; visits the reidentify preview above would\n"
            . "  (re)do are reidentified and ingested with --confirm");
    }
    $importArgs = array_merge($withForce, $exArgs, isset($opts['no-pic']) ? ['--no-pic'] : []);
    if (BundleSupport::runStep(__DIR__, 'run_png_import.php', $importArgs, $log, $lock) !== 0) {
        PngCli::err("\n  STOP import failed for {$label}");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }
    PngCli::err("\n  {$label} complete");
    BundleSupport::masterLog($log, "RESULT {$label} complete");
}

if (!$confirm) {
    PngCli::err("\nDry run complete. Nothing was written and nothing ingested.\nTo execute the chain, run:\n\n  "
        . PngCli::confirmCommand($argv) . "\n");
}

exit($exit);
