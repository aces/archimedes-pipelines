#!/usr/bin/env php
<?php
/**
 * ARCHIMEDES DICOM Import Pipeline Runner
 *
 * Calls POST /cbigr_api/script/importdicomstudy for each DICOM study
 * found under <base_path>/<project>/deidentified-lorisid/imaging/dicoms/
 *
 * Default is DRY RUN. Use --confirm to execute.
 *
 * Usage:
 *   php scripts/dicom/run_dicom_import.php [options]
 *
 * Scope:
 *   --all                       All enabled collections & projects from config
 *   --collection=NAME           All enabled projects in a collection
 *   --collection=NAME --project=NAME  Single project in a collection
 *
 * Options:
 *   --config=FILE     Config file (default: config/loris_client_config.json)
 *   --confirm         Actually execute (default is dry run)
 *   --dry-run         Explicit dry run; wins over --confirm
 *   --force           Reprocess already-processed studies
 *   --update          Use --update flag (default: --insert)
 *   --overwrite       Add --overwrite flag
 *   --profile=NAME    Python config file (default: database_config.py)
 *   --verbose         Verbose logging
 *   --help            Show this help
 *
 * Examples:
 *   # Dry run all projects
 *   php scripts/dicom/run_dicom_import.php --all --verbose
 *
 *   # Dry run one collection
 *   php scripts/dicom/run_dicom_import.php --collection=archimedes --verbose
 *
 *   # Confirm single project
 *   php scripts/dicom/run_dicom_import.php --collection=archimedes --project=FDG-PET --confirm
 *
 *   # Force reprocess across everything
 *   php scripts/dicom/run_dicom_import.php --all --confirm --force --verbose
 */

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\Cli;
use LORIS\Pipelines\Dicom\DicomImportPipeline;

// ── Parse CLI ──────────────────────────────────────────────────────────

$shortOpts = 'v';
$longOpts  = [
    'all',
    'collection:',
    'project:',
    'config:',
    'profile:',
    'source-subdir:',
    'confirm',
    'dry-run',
    'force',
    'update',
    'exclude-file:',
    'overwrite',
    'verbose',
    'help',
];
$options = getopt($shortOpts, $longOpts);
// getopt() ignores options it does not know; a typo or a removed option
// must not run with the default instead.
Cli::exitOnUnknownOptions($shortOpts, $longOpts);

if (isset($options['help'])) {
    echo <<<HELP
ARCHIMEDES DICOM Import Pipeline

Scope:
  --all                         All enabled collections & projects
  --collection=NAME             All enabled projects in a collection
  --collection=NAME --project=N Single project

Options:
  --config=FILE     Config file (default: config/loris_client_config.json)
  --profile=NAME    Python config name (default: database_config.py)
  --source-subdir=P Directory to scan, relative to the project root.
                    Default: deidentified-lorisid/imaging/dicoms (the
                    reidentified output). A project not reidentified yet is
                    skipped. Any other directory is deprecated and warned.
  --confirm         Execute (default: dry run)
  --dry-run         Explicit dry run; wins over --confirm
  --force           Reprocess already-done studies
  --update          --update flag (default: --insert)
  --exclude-file=F  Studies to leave out (written by the bundle when the
                    operator continues with the good studies only)
  --overwrite       --overwrite flag
  -v, --verbose     Verbose output
  --help            This help

Every study is imported with --session, attached to its LORIS visit (as BIDS,
which is organized by sub-/ses-). A study with no session - not reidentified, or
its session missing in LORIS - is a WARNING and is not imported; it is picked up
on a later run once the session exists. Unlinked studies are never imported.
Exit codes: 0 success, 1 any failure, 2 usage error

HELP;
    exit(0);
}

$verbose      = isset($options['verbose']) || isset($options['v']);
$dryRun       = !isset($options['confirm']) || isset($options['dry-run']);
$force        = isset($options['force']);
$useUpdate    = isset($options['update']);
$useOverwrite = isset($options['overwrite']);
$profile      = $options['profile'] ?? 'database_config.py';

// Where to look for study directories, relative to the project root. The
// bundle always passes it. Run alone, the default follows the pipeline: the
// reidentified output, else the organized output. The raw delivery is only a
// last resort - it is grouped however the site sent it, and a folder holding
// several StudyInstanceUIDs would be archived as ONE study (the importer takes
// the identity of the first file it reads).
$sourceSubdir = $options['source-subdir'] ?? null;

// ── Load config ────────────────────────────────────────────────────────

$configFile = $options['config'] ?? __DIR__ . '/../../config/loris_client_config.json';

if (!file_exists($configFile)) {
    fwrite(STDERR, "Config not found: {$configFile}\n");
    exit(1);
}

$config = json_decode(file_get_contents($configFile), true);
if (empty($config)) {
    fwrite(STDERR, "Invalid config: {$configFile}\n");
    exit(1);
}

// ── Build flags ────────────────────────────────────────────────────────

$flags = [];
$flags[] = $useUpdate ? 'update' : 'insert';
if ($useOverwrite) $flags[] = 'overwrite';
if ($verbose)      $flags[] = 'verbose';

// ── Resolve project directories to process ─────────────────────────────

$projectDirs = resolveProjectDirs($config, $options);

if (empty($projectDirs)) {
    fwrite(STDERR, "No projects to process. Use --all, --collection, or --collection + --project.\n");
    exit(1);
}

// ── Banner ─────────────────────────────────────────────────────────────

if ($dryRun) {
    echo "\n";
    echo "╔══════════════════════════════════════╗\n";
    echo "║         DRY RUN MODE                 ║\n";
    echo "║  No changes will be made.            ║\n";
    echo "║  Use --confirm to execute.           ║\n";
    echo "╚══════════════════════════════════════╝\n";
    echo "\n";
}

echo "Projects to process: " . count($projectDirs) . "\n";
foreach ($projectDirs as $info) {
    echo "  • {$info['collection']}/{$info['project']} → {$info['path']}\n";
}
echo "\n";

// ── Run pipeline per project ───────────────────────────────────────────

$totalStats = [
    'studies_found' => 0, 'studies_processed' => 0,
    'studies_skipped' => 0, 'studies_already_exist' => 0, 'studies_failed' => 0,
    'studies_no_session' => 0,
];
$anyFailed = false;

foreach ($projectDirs as $info) {
    echo "────────────────────────────────────────\n";
    echo "Collection: {$info['collection']}  Project: {$info['project']}\n";
    echo "────────────────────────────────────────\n";

    $subdir = $sourceSubdir;
    if ($subdir === null) {
        // Default: reidentified output only. Anything else must be asked for
        // with --source-subdir, so nothing non-reidentified is ingested by accident.
        if (!is_file($info['path'] . '/deidentified-lorisid/imaging/dicom_studies_lorisid.json')) {
            fwrite(STDERR, "  SKIP {$info['collection']}/{$info['project']}: not reidentified yet - "
                . "run run_dicom_reidentifier.php first (or pass --source-subdir explicitly)\n");
            $anyFailed = true;
            continue;
        }
        $subdir = 'deidentified-lorisid/imaging/dicoms';
    } elseif (trim($subdir, '/') !== 'deidentified-lorisid/imaging/dicoms') {
        fwrite(STDERR, "WARNING: importing from {$subdir}, not the reidentified output - "
            . "PatientName keeps the site identifier (deprecated; use only for one-off cases).\n");
    }

    $pipeline = new DicomImportPipeline($config, $dryRun, $verbose);
    $pipeline->setSessionForLinked(true);
    $pipeline->setCollection($info['collection']);
    $exclusions = \LORIS\Pipelines\Dicom\DicomPreflight::readExclusions($options['exclude-file'] ?? null);
    $pipeline->setExcluded($exclusions['study_uids'], $exclusions['labels']);

    $stats = $pipeline->run(
        projectDir:   $info['path'],
        force:        $force,
        flags:        $flags,
        profile:      $profile,
        sourceSubdir: $subdir
    );

    foreach ($totalStats as $key => &$val) {
        $val += $stats[$key] ?? 0;
    }
    unset($val);

    if (($stats['studies_failed'] ?? 0) > 0) {
        $anyFailed = true;
    }

    // A run that aborted did not fail any study — it never reached one. Without
    // this the exit code is 0 and cron reports success on a run that ingested
    // nothing.
    if (!empty($stats['aborted'])) {
        $anyFailed = true;
        fwrite(STDERR, "ABORTED: " . ($stats['abort_reason'] ?? 'unknown') . "\n");
    }
}

// ── Grand total ────────────────────────────────────────────────────────

if (count($projectDirs) > 1) {
    echo "\n";
    echo "════════════════════════════════════════\n";
    echo "GRAND TOTAL across " . count($projectDirs) . " projects:\n";
    echo "  Found:           {$totalStats['studies_found']}\n";
    echo "  Imported:        {$totalStats['studies_processed']}\n";
    echo "  Already existed: {$totalStats['studies_already_exist']}\n";
    echo "  Skipped:         {$totalStats['studies_skipped']}\n";
    echo "  Failed:          {$totalStats['studies_failed']}\n";
    echo "  No session:      {$totalStats['studies_no_session']} (warning, not ingested)\n";
    echo "════════════════════════════════════════\n";
}

exit($anyFailed ? 1 : 0);

// ── Helper: resolve project dirs from config ───────────────────────────

function resolveProjectDirs(array $config, array $options): array
{
    $collections   = $config['collections'] ?? [];
    $wantAll       = isset($options['all']);
    $wantColl      = $options['collection'] ?? null;
    $wantProject   = $options['project'] ?? null;

    $dirs = [];

    foreach ($collections as $coll) {
        $collName  = $coll['name'] ?? '';
        $basePath  = rtrim($coll['base_path'] ?? '', '/');
        $enabled   = !empty($coll['enabled']);   // same rule as the other DICOM scripts
        $projects  = $coll['projects'] ?? [];

        if (!$wantAll && $wantColl !== null && $wantColl !== $collName) {
            continue;
        }

        if (!$enabled && $wantColl !== $collName) {
            continue;
        }

        if (empty($basePath) || empty($projects)) {
            continue;
        }

        foreach ($projects as $proj) {
            $projName    = $proj['name'] ?? '';
            $projEnabled = !empty($proj['enabled']);

            if ($wantProject !== null && $wantProject !== $projName) {
                continue;
            }

            $projectPath = "{$basePath}/{$projName}";

            if (!\LORIS\Pipelines\Dicom\DicomStudyOrganizer::acceptProject(
                "{$collName}/{$projName}",
                $projectPath,
                $enabled && $projEnabled,
                $wantProject === $projName || (!$enabled && $wantColl === $collName)
            )) {
                continue;
            }

            if (!is_dir($projectPath)) {
                fwrite(STDERR, "  Warning: project dir not found: {$projectPath} — skipping\n");
                continue;
            }

            $dirs[] = [
                'collection' => $collName,
                'project'    => $projName,
                'path'       => $projectPath,
            ];
        }
    }

    if (empty($dirs) && !$wantAll && $wantColl === null) {
        return [];
    }

    return $dirs;
}