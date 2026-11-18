<?php

/**
 * run_png_scan.php
 *
 * OPTIONAL - the bundle does this scan itself (PNG needs no organize step).
 * Use it to look at a delivery's manifest on its own. Reads the
 * delivery, checks every folder, spreadsheet and image, hashes every image,
 * and writes the scan manifest processed/imaging/png_visits.json:
 *   each visit linked / unlinked / blocked, its images (md5), the images set
 *   aside and why, and every problem with its scope.
 *
 * Nothing is copied or moved: PNG deliveries are already one folder per visit.
 * The later steps re-scan on their own, so a manifest can never be stale.
 *
 * Exit: 0 no data issue, 1 data issues (manifest still written), 2 usage.
 *
 * PHP Version 8.1
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Png\PngCli;
use LORIS\Pipelines\Png\PngDelivery;
use LORIS\Pipelines\Png\PngPaths;
use LORIS\Pipelines\Png\PngPreflight;

$usage = <<<TXT
Usage: php scripts/png/run_png_scan.php --collection=NAME [--project=NAME] [--confirm] [-v]

Scans deidentified-raw/imaging/png/ and writes processed/imaging/png_visits.json.
Without --confirm the manifest is still written (marked dry_run): the scan reads only.

TXT;

$cli    = PngCli::options([], $usage);
$config = PngCli::config($cli['config_path']);
$exit   = PngCli::EXIT_OK;

foreach (PngCli::targets($config, $cli['options']) as $t) {
    PngCli::err("=== {$t['collection']}/{$t['project']} ===");
    $projectConfig = PngCli::projectConfig($t['path']);
    if ($projectConfig === null) {
        PngCli::err('  ERROR project.json missing or not valid JSON');
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }
    if (!is_dir(PngPaths::of($t['path'], PngPaths::RAW_SUBDIR))) {
        PngCli::err('  SKIP no PNG delivery (' . PngPaths::RAW_SUBDIR . '/ does not exist)');
        continue;
    }

    $manifest = PngDelivery::scan($t['path'], $projectConfig);
    $manifest['dry_run'] = !$cli['confirm'];
    $pre = PngPreflight::check($manifest, $projectConfig, PngCli::visitLabels($projectConfig));

    try {
        PngPaths::writeJson(PngPaths::of($t['path'], PngPaths::SCAN_MANIFEST), $manifest);
    } catch (\Throwable $e) {
        PngCli::err('  ERROR ' . $e->getMessage());
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    $c = $manifest['counts'];
    PngCli::err(sprintf('  %d visit(s): %d linked, %d unlinked (not matched), %d blocked; %d image(s) accepted, %d set aside',
        $c['visits'], $c['linked'], $c['unlinked'], $c['blocked'], $c['images'], $c['set_aside']));
    $types = [];
    foreach ($manifest['visits'] as $v) {
        foreach ($v['images'] as $i) {
            $types[$i['scan_type']] = ($types[$i['scan_type']] ?? 0) + 1;
        }
    }
    if ($types !== []) {
        ksort($types);
        PngCli::err('  image types: ' . implode('  ', array_map(static fn ($k, $n) => "{$k}:{$n}", array_keys($types), $types)));
    }
    foreach ($pre['errors'] as $e) {
        PngCli::err("    ERROR   {$e}");
    }
    foreach ($pre['warnings'] as $w) {
        PngCli::err("    warning {$w}");
    }
    if ($pre['errors'] !== []) {
        $exit = PngCli::EXIT_FAILURE;
    }
    PngCli::err('  manifest: ' . PngPaths::of($t['path'], PngPaths::SCAN_MANIFEST) . "\n");
}

exit($exit);
