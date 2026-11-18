<?php

/**
 * run_png_participant_sync.php
 *
 * Step 1 of the PNG pipeline: create the LORIS candidates for the participants
 * of a PNG delivery, linked to the site's identifier as their external ID
 * (CandidatesPlus: ProjectExternalName + ExtStudyID, legacy fallback) - the
 * shared ParticipantSync, exactly as DICOM uses it. Existing candidates are
 * matched by external ID and never changed (a different dob/sex is a warning).
 *
 * Only participants with at least one linked visit are synced (not every row
 * of participants.tsv), unless --all-participants.
 *
 * Pre-flight first: every data issue is listed before anything is created; any
 * issue that is not excluded (--exclude-file, written by the bundle) means
 * nothing is created for the project.
 *
 * Dry run unless --confirm.
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
use LORIS\Pipelines\Shared\ParticipantsTsv;
use LORIS\Pipelines\Shared\ParticipantSync;

$usage = <<<TXT
Usage: php scripts/png/run_png_participant_sync.php --collection=NAME [--project=NAME] [options]

Creates the LORIS candidates of a PNG delivery, linked by ExternalID.
Existing candidates are matched, never modified.

  --confirm            create candidates (default: dry run)
  --all-participants   every participants.tsv row, not only those with linked visits
  --exclude-file=FILE  visits/participants to leave out (written by the bundle)
  --config=FILE  -v

Exit codes: 0 success, 1 any failure or data issue, 2 usage

TXT;

$cli     = PngCli::options(['all-participants', 'exclude-file:'], $usage);
$opts    = $cli['options'];
$confirm = $cli['confirm'];
$config  = PngCli::config($cli['config_path']);
$exit    = PngCli::EXIT_OK;
$lock    = null;

if (!$confirm) {
    PngCli::err("DRY RUN - no candidates will be created. Pass --confirm to execute.\n");
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

    $manifest = PngDelivery::scan($t['path'], $projectConfig);
    $tsv      = $manifest['participants_tsv'];
    if ($tsv === null) {
        PngCli::err("  SKIP no participants.tsv - every visit is UNLINKED: reported only, nothing created\n");
        continue;
    }

    try {
        $loaded = ParticipantSync::load($tsv, $projectConfig);
    } catch (\Throwable $e) {
        PngCli::err('  ERROR ' . $e->getMessage() . "\n");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }
    /** @var ParticipantsTsv $roster */
    $roster = $loaded['roster'];
    PngCli::err(sprintf('  %d participant row(s) in %s', $roster->count(), basename($tsv)));
    if ($loaded['enriched'] !== []) {
        PngCli::err('  enriched from candidate_defaults: ' . implode(', ', $loaded['enriched']));
    }
    if ($loaded['missing'] !== []) {
        PngCli::err('  ERROR columns missing after enrichment: ' . implode(', ', $loaded['missing'])
            . " - add them to participants.tsv or to project.json candidate_defaults\n");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    $exclusions = PngPreflight::readExclusions($opts['exclude-file'] ?? null);
    $excludedSubjects = array_flip($exclusions['subjects']);
    if ($excludedSubjects !== []) {
        PngCli::err(sprintf('  %d participant(s) excluded by the operator (issues reported): %s',
            count($excludedSubjects), implode(', ', array_map(static fn ($s) => "sub-{$s}", array_keys($excludedSubjects)))));
    }

    // Only participants with a linked visit that is not excluded.
    $wanted = null;
    if (!isset($opts['all-participants'])) {
        $wanted = [];
        $exVisits = array_flip($exclusions['visits']);
        foreach ($manifest['visits'] as $v) {
            if ($v['link_status'] === 'linked' && $v['images'] !== [] && !isset($exVisits[$v['key']])) {
                $wanted[$v['subject']] = true;
            }
        }
        if ($wanted === []) {
            PngCli::err("  SKIP no linked visit with images - nothing to sync (see scripts/test/validate_png.php)\n");
            continue;
        }
        PngCli::err(sprintf('  %d participant(s) with linked visits', count($wanted)));
    }

    $logger = PngCli::logger($t['path'], 'png', 'participant_sync', $cli['verbose']);

    try {
        $api = PngCli::api($config, $projectConfig, $logger);
    } catch (\Throwable $e) {
        PngCli::err('  ERROR ' . $e->getMessage() . "\n");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    // --- Pre-flight: every issue before any candidate is created ---------------
    $pre = PngPreflight::check($manifest, $projectConfig, PngCli::visitLabels($projectConfig), $exclusions);
    PngCli::err(sprintf('  PRE-FLIGHT (before any candidate is created): %d error(s), %d warning(s)',
        count($pre['errors']), count($pre['warnings'])));
    foreach ($pre['errors'] as $line) {
        PngCli::err("    ERROR   {$line}");
        $logger->error("Pre-flight: {$line}");
    }
    if ($cli['verbose']) {
        foreach ($pre['warnings'] as $line) {
            PngCli::err("    warning {$line}");
        }
    }
    if ($pre['errors'] !== []) {
        PngCli::err("  STOP nothing created - fix the error(s) above and re-run\n");
        $exit = PngCli::EXIT_FAILURE;
        continue;
    }

    $n = ParticipantSync::sync($roster, $api, $wanted, $excludedSubjects, $confirm,
        static fn (string $line) => PngCli::err($line), $logger, $cli['verbose']);
    if ($n['failed'] > 0) {
        $exit = PngCli::EXIT_FAILURE;
    }

    if ($confirm) {
        try {
            PngPaths::writeJson(PngPaths::of($t['path'], PngPaths::SYNC_WARNINGS), [
                'generated_at' => date('c'),
                'problems' => array_map(static fn ($m) => ['level' => 'warning', 'message' => $m], $n['differs']),
            ]);
        } catch (\Throwable $e) {
            $logger->warning($e->getMessage());
        }
    }

    PngCli::err(sprintf("  %d matched, %d %s, %d skipped, %d failed%s\n", $n['matched'], $n['created'],
        $confirm ? 'created' : 'would create', $n['skipped'], $n['failed'],
        $n['differs'] === [] ? '' : sprintf(', %d differ from LORIS (warning)', count($n['differs']))));
}

if (!$confirm) {
    PngCli::err("Dry run complete. No candidates were created.\nTo create them, run:\n\n  "
        . PngCli::confirmCommand($argv) . "\n");
}

exit($exit);
