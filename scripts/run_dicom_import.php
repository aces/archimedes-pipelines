#!/usr/bin/env php
<?php
/**
 * ARCHIMEDES DICOM Import Pipeline Runner
 *
 * Calls POST /cbigr_api/script/importdicomstudy for each DICOM study
 * found under <base_path>/<project>/deidentified-raw/imaging/dicoms/
 *
 * Default is DRY RUN. Use --confirm to execute.
 *
 * Usage:
 *   php scripts/run_dicom_import.php [options]
 *
 * Scope:
 *   --all                       All enabled collections & projects from config
 *   --collection=NAME           All enabled projects in a collection
 *   --collection=NAME --project=NAME  Single project in a collection
 *
 * Options:
 *   --config=FILE     Config file (default: config/loris_client_config.json)
 *   --confirm         Actually execute (default is dry run)
 *   --force           Reprocess already-processed studies
 *   --update          Use --update flag (default: --insert)
 *   --session         Add --session flag
 *   --overwrite       Add --overwrite flag
 *   --profile=NAME    Python config file (default: database_config.py)
 *   --verbose         Verbose logging
 *   --help            Show this help
 *
 * Examples:
 *   # Dry run all projects
 *   php scripts/run_dicom_import.php --all --verbose
 *
 *   # Dry run one collection
 *   php scripts/run_dicom_import.php --collection=archimedes --verbose
 *
 *   # Confirm single project
 *   php scripts/run_dicom_import.php --collection=archimedes --project=FDG-PET --confirm
 *
 *   # Force reprocess across everything
 *   php scripts/run_dicom_import.php --all --confirm --force --verbose
 */

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\DicomImportPipeline;

// ── Parse CLI ──────────────────────────────────────────────────────────

$options = getopt('', [
    'all',
    'collection:',
    'project:',
    'config:',
    'profile:',
    'confirm',
    'force',
    'update',
    'session',
    'overwrite',
    'verbose',
    'help',
]);

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
  --confirm         Execute (default: dry run)
  --force           Reprocess already-done studies
  --update          --update flag (default: --insert)
  --session         --session flag
  --overwrite       --overwrite flag
  --verbose         Verbose output
  --help            This help

HELP;
    exit(0);
}

$verbose      = isset($options['verbose']);
$dryRun       = !isset($options['confirm']);
$force        = isset($options['force']);
$useUpdate    = isset($options['update']);
$useSession   = isset($options['session']);
$useOverwrite = isset($options['overwrite']);
$profile      = $options['profile'] ?? 'database_config.py';

// ── Load config ────────────────────────────────────────────────────────

$configFile = $options['config'] ?? __DIR__ . '/../config/loris_client_config.json';

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
if ($useSession)   $flags[] = 'session';
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
];
$anyFailed = false;

foreach ($projectDirs as $info) {
    echo "────────────────────────────────────────\n";
    echo "Collection: {$info['collection']}  Project: {$info['project']}\n";
    echo "────────────────────────────────────────\n";

    $pipeline = new DicomImportPipeline($config, $dryRun, $verbose);

    $stats = $pipeline->run(
        projectDir: $info['path'],
        force:      $force,
        flags:      $flags,
        profile:    $profile
    );

    foreach ($totalStats as $key => &$val) {
        $val += $stats[$key] ?? 0;
    }
    unset($val);

    if (($stats['studies_failed'] ?? 0) > 0) {
        $anyFailed = true;
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
        $enabled   = $coll['enabled'] ?? true;
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
            $projEnabled = $proj['enabled'] ?? true;

            if ($wantProject !== null && $wantProject !== $projName) {
                continue;
            }

            if (!$projEnabled && $wantProject !== $projName) {
                continue;
            }

            $projectPath = "{$basePath}/{$projName}";

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