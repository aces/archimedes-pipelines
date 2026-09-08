<?php

/**
 * run_dicom_organize.php
 *
 * Groups delivered DICOM uploads by StudyInstanceUID into one directory per
 * study, ready for run_dicom_import.php.
 *
 * Runs upstream of the DICOM import pipeline. Reads each project's
 * deidentified-raw/imaging/dicoms/ tree, validates it against the ARCHIMEDES
 * submission guide, and writes organised studies plus a manifest under
 * processed/imaging/.
 *
 * No database connection. DICOM headers are read via get_dicom_info.pl.
 *
 * Dry run is the default, consistent with the other pipelines. Pass --confirm
 * to write.
 *
 * Usage:
 *   php scripts/run_dicom_organize.php --all --verbose
 *   php scripts/run_dicom_organize.php --collection=archimedes --project=FDG-PET --confirm -v
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\DicomEviDataExtract;
use LORIS\Pipelines\DicomOrganizeClient;
use LORIS\Pipelines\DicomStudyOrganizer;
use LORIS\Pipelines\StudyFingerprint;
use Monolog\Handler\RotatingFileHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;

const EXIT_OK      = 0;
const EXIT_FAILURE = 1;
const EXIT_USAGE   = 2;

const DEFAULT_CONFIG = __DIR__ . '/../config/loris_client_config.json';

/**
 * Print usage and exit.
 */
function usage(int $code = EXIT_USAGE): void
{
    fwrite(
        $code === EXIT_OK ? STDOUT : STDERR,
        <<<TXT
Usage: php scripts/run_dicom_organize.php [options]

Groups delivered DICOMs by StudyInstanceUID into one directory per study.
Runs before run_dicom_import.php.

Selection:
  --all                Process all enabled collections & projects
  --collection=NAME    Process all enabled projects in a collection
  --project=NAME       Process a specific project (requires --collection)

Execution:
  --confirm            Execute
  --dry-run            Report without changing anything (the default)
  --force              Reprocess studies already in the tracking file
  --move               Move files instead of copying (default: copy)
  --phantom            Treat every subject in this run as a phantom
  --no-unlinked        Skip studies that cannot be resolved to a participant,
                       instead of archiving them unlinked
  --evidata            Run the EviData header extract even when project.json
                       has not enabled it. Read-only; writes a CSV.
                       When enabled it is a GATE: if the extract fails, the
                       project is skipped and nothing is organised.
  --no-evidata         Skip it even when project.json enables it
  --no-api             Do not use the dicomorganize endpoint. Off by default:
                       organising needs only the data and pydicom, so it runs
                       where the data is. Set imaging.organize_via_api to opt in.
  --config=FILE        Config file (default: config/loris_client_config.json)
  -v, --verbose        Detailed output
  --help               Show this message

Exit codes: 0 success, 1 any failures, 2 usage error

TXT
    );
    exit($code);
}

/**
 * Read and decode a JSON file.
 *
 * @throws RuntimeException on missing file or invalid JSON.
 */
function readJson(string $path): array
{
    if (!file_exists($path)) {
        throw new RuntimeException("File not found: {$path}");
    }

    $raw = file_get_contents($path);
    if ($raw === false) {
        throw new RuntimeException("Could not read {$path}");
    }

    $data = json_decode($raw, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        throw new RuntimeException(
            "Invalid JSON in {$path}: " . json_last_error_msg()
        );
    }

    return $data;
}

/**
 * Resolve which collection/project pairs to process.
 *
 * @return array<int, array{collection: string, project: string, path: string}>
 */
function resolveTargets(array $config, array $options): array
{
    $targets = [];

    $wantAll        = isset($options['all']);
    $wantCollection = $options['collection'] ?? null;
    $wantProject    = $options['project'] ?? null;

    if ($wantProject !== null && $wantCollection === null) {
        fwrite(STDERR, "--project requires --collection\n\n");
        usage();
    }

    // Collection and project names are identifiers, not data, so match them
    // case-insensitively. --collection=ARCHIMEDES finding nothing because the
    // config says "archimedes" is a frustrating way to spend ten minutes.
    $matches = static function (?string $wanted, string $actual): bool {
        return $wanted === null || strcasecmp($wanted, $actual) === 0;
    };

    foreach ($config['collections'] ?? [] as $collection) {
        $collectionName = $collection['name'] ?? null;

        if ($collectionName === null) {
            continue;
        }

        $collectionWanted = $matches($wantCollection, $collectionName);

        if (empty($collection['enabled']) && !$collectionWanted) {
            continue;
        }

        if (!$wantAll && $wantCollection !== null && !$collectionWanted) {
            continue;
        }

        $basePath = rtrim($collection['base_path'] ?? '', '/');

        foreach ($collection['projects'] ?? [] as $project) {
            $projectName = $project['name'] ?? null;

            if ($projectName === null) {
                continue;
            }

            $projectWanted = $matches($wantProject, $projectName);

            if ($wantProject !== null && !$projectWanted) {
                continue;
            }

            if (empty($project['enabled']) && !$projectWanted) {
                continue;
            }

            $targets[] = [
                'collection' => $collectionName,
                'project'    => $projectName,
                'path'       => $basePath . '/' . $projectName,
            ];
        }
    }

    return $targets;
}

/**
 * Build a per-project logger writing to logs/dicom/.
 */
function buildLogger(string $projectPath, bool $verbose): Logger
{
    $logDir = $projectPath . '/logs/dicom';

    if (!is_dir($logDir)) {
        @mkdir($logDir, 0775, true);
    }

    $logger = new Logger('dicom_organize');

    if (is_dir($logDir)) {
        $logger->pushHandler(
            new RotatingFileHandler($logDir . '/dicom_organize.log', 30, Level::Info)
        );
    }

    $logger->pushHandler(
        new StreamHandler('php://stderr', $verbose ? Level::Debug : Level::Warning)
    );

    return $logger;
}

/**
 * The command just run, with --confirm added and --dry-run removed.
 *
 * Printed at the end of a dry run so the operator can copy the exact line
 * rather than reconstructing it, which is where selection arguments get
 * mistyped and the wrong project gets written.
 */
function confirmCommand(array $argv): string
{
    $args = array_slice($argv, 1);

    $args = array_values(array_filter(
        $args,
        static fn (string $a): bool => $a !== '--dry-run' && $a !== '--confirm'
    ));

    $args[] = '--confirm';

    return 'php ' . $argv[0] . ' ' . implode(' ', array_map(
        // Quote anything outside the plain set, so a path with a space in it
        // still produces a line that can be pasted and run.
            static fn (string $a): string => preg_match('~^[A-Za-z0-9=_./-]+$~', $a) === 1
                ? $a
                : escapeshellarg($a),
            $args
        ));
}

// -----------------------------------------------------------------------------
//  Arguments
// -----------------------------------------------------------------------------

$options = getopt(
    'v',
    [
        'all',
        'collection:',
        'project:',
        'confirm',
        'dry-run',
        'force',
        'move',
        'phantom',
        'no-unlinked',
        'evidata',
        'no-evidata',
        'no-api',
        'config:',
        'verbose',
        'help',
    ]
);

if ($options === false || isset($options['help'])) {
    usage(isset($options['help']) ? EXIT_OK : EXIT_USAGE);
}

if (!isset($options['all']) && !isset($options['collection'])) {
    fwrite(STDERR, "Specify --all or --collection=NAME\n\n");
    usage();
}

// --confirm executes; --dry-run is the default and is accepted
// explicitly for symmetry with the other pipelines. If both are
// given, --dry-run wins - the safe reading of a contradiction.
$confirm    = isset($options['confirm'])
    && !isset($options['dry-run']);

if (isset($options['confirm']) && isset($options['dry-run'])) {
    fwrite(STDERR, "Both --confirm and --dry-run given - treating as dry run.\n\n");
}
$force      = isset($options['force']);
$move       = isset($options['move']);
$phantom    = isset($options['phantom']);
$unlinked   = !isset($options['no-unlinked']);
$forceAudit = isset($options['evidata']);
$skipAudit  = isset($options['no-evidata']);
$noApi      = isset($options['no-api']);
$verbose    = isset($options['v']) || isset($options['verbose']);
$configPath = $options['config'] ?? DEFAULT_CONFIG;

try {
    $config = readJson($configPath);
} catch (RuntimeException $e) {
    fwrite(STDERR, 'ERROR ' . $e->getMessage() . "\n");
    exit(EXIT_USAGE);
}

$targets = resolveTargets($config, $options);

if (empty($targets)) {
    fwrite(STDERR, "No enabled projects matched the selection.\n\n");

    // Say what IS available. A bare "no match" with no hint is the most
    // common way to lose time to a typo or a disabled flag.
    $available = [];

    foreach ($config['collections'] ?? [] as $collection) {
        $name     = $collection['name'] ?? '?';
        $enabled  = !empty($collection['enabled']) ? '' : '  [collection disabled]';
        $projects = [];

        foreach ($collection['projects'] ?? [] as $project) {
            $projects[] = ($project['name'] ?? '?')
                . (!empty($project['enabled']) ? '' : ' [disabled]');
        }

        $available[] = sprintf(
            "  --collection=%s%s\n      projects: %s",
            $name,
            $enabled,
            empty($projects) ? '(none)' : implode(', ', $projects)
        );
    }

    if (empty($available)) {
        fwrite(STDERR, "No collections are defined in {$configPath}.\n");
    } else {
        fwrite(STDERR, "Available:\n" . implode("\n", $available) . "\n\n");
        fwrite(STDERR, "Names are matched case-insensitively.\n");
    }

    exit(EXIT_USAGE);
}

$dicomInfoPath = $config['imaging']['get_dicom_info_path']
    ?? rtrim($config['imaging']['mri_code_path'] ?? '', '/')
    . '/dicom-archive/get_dicom_info.pl';

// Only the get_dicom_info reader needs the Perl script. dcmdump and pydicom
// check their own tools in the organiser's preflight, so demanding this one
// up front would fail a run that never intends to use it.
if (($config['imaging']['header_reader'] ?? 'dcmdump') === 'get_dicom_info'
    && !is_executable($dicomInfoPath)
) {
    fwrite(
        STDERR,
        "ERROR header_reader is 'get_dicom_info' but the script is not "
        . "executable: {$dicomInfoPath}\n"
        . "Set imaging.get_dicom_info_path, or use the default dcmdump reader.\n"
    );
    exit(EXIT_USAGE);
}

// Global EviData service config, shared by every project on this host.
$evidataGlobal = [];
$evidataPath   = __DIR__ . '/../config/evidata_config.json';

if (file_exists($evidataPath)) {
    try {
        $evidataGlobal = readJson($evidataPath);
    } catch (RuntimeException $e) {
        fwrite(STDERR, 'ERROR ' . $e->getMessage() . "\n");
        exit(EXIT_USAGE);
    }
}

if (!$confirm) {
    fwrite(STDERR, "DRY RUN - no files will be written. Pass --confirm to execute.\n\n");
}

// -----------------------------------------------------------------------------
//  Run
// -----------------------------------------------------------------------------

$exitCode = EXIT_OK;
$summary  = [];

foreach ($targets as $target) {
    $projectPath = $target['path'];
    $label       = "{$target['collection']}/{$target['project']}";

    fwrite(STDERR, "=== {$label} ===\n");

    // Gate on project.json, same as the other imaging pipelines.
    $projectJson = $projectPath . '/project.json';

    if (!file_exists($projectJson)) {
        fwrite(STDERR, "  SKIP no project.json at {$projectJson}\n\n");
        continue;
    }

    try {
        $projectConfig = readJson($projectJson);
    } catch (RuntimeException $e) {
        fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n\n");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    $modalities = $projectConfig['modalities'] ?? [];
    if (!empty($modalities) && !in_array('Imaging', $modalities, true)) {
        fwrite(STDERR, "  SKIP project does not declare the Imaging modality\n\n");
        continue;
    }

    $sourceDir = $projectPath . '/deidentified-raw/imaging/dicoms';
    $targetDir = $projectPath . '/processed/imaging';

    if (!is_dir($sourceDir)) {
        fwrite(STDERR, "  SKIP no DICOM directory at {$sourceDir}\n\n");
        continue;
    }

    // --- Delivery-level short circuit --------------------------------------
    // Stat every file - nothing is opened - and compare against the last
    // successful run. If nothing has been added, removed or resized, there is
    // nothing to re-ingest: skip the EviData pass, the header reads and the
    // copy, and say so. This is the difference between a repeat run costing
    // two minutes and costing two seconds.
    $stateFile = $projectPath . '/processed/imaging/.dicom_delivery_state.json';

    $delivery = StudyFingerprint::deliveryHash($sourceDir);

    $previousState = null;

    if (file_exists($stateFile)) {
        try {
            $previousState = readJson($stateFile);
        } catch (RuntimeException $e) {
            // A corrupt state file must not stop a run; treat it as absent.
            fwrite(STDERR, '  WARN unreadable delivery state, re-processing: '
                . $e->getMessage() . "\n");
        }
    }

    if (!$force
        && $previousState !== null
        && ($previousState['delivery_hash'] ?? null) === $delivery['hash']
    ) {
        fwrite(STDERR, sprintf(
            "  UNCHANGED since %s - %d file(s), nothing to re-ingest\n",
            $previousState['completed_at'] ?? 'the last run',
            $delivery['file_count']
        ));
        fwrite(
            STDERR,
            "  Pass --force to re-organise anyway.\n\n"
        );
        continue;
    }

    if ($previousState !== null) {
        fwrite(STDERR, sprintf(
            "  Delivery changed since %s (%d file(s) now, %d then)\n",
            $previousState['completed_at'] ?? '?',
            $delivery['file_count'],
            $previousState['file_count'] ?? 0
        ));
    }

    $logger = buildLogger($projectPath, $verbose);

    // De-identification audit runs BEFORE organising, on the original
    // delivery - that is what the site actually sent, and it is the version
    // worth auditing. Read-only, so it runs in dry mode too. Never blocks.
    $evidataState = DicomEviDataExtract::explainEnabled(
        $projectConfig,
        $evidataGlobal,
        'imaging'
    );

    $auditWanted = !$skipAudit && ($forceAudit || $evidataState['enabled']);

    // Always say whether the check ran and why. A skipped privacy check should
    // never be silent - "it was off and nobody noticed" is the failure mode.
    if (!$auditWanted) {
        $reason = $skipAudit
            ? 'disabled by --no-evidata'
            : $evidataState['decided_by'];

        fwrite(STDERR, "  evidata: SKIPPED ({$reason})\n");
        $logger->info("EviData check skipped: {$reason}");
    } elseif ($verbose) {
        fwrite(STDERR, sprintf(
            "  evidata: on (%s)%s\n",
            $forceAudit && !$evidataState['enabled']
                ? 'forced by --evidata'
                : $evidataState['decided_by'],
            ''
        ));
    }

    if ($auditWanted) {
        // EviData check runs on the original delivery, before anything is
        // grouped or copied. When enabled it is a gate: if the extract cannot
        // be produced, this project is not organised or ingested.
        $checkFailure = null;

        try {
            $evidata = DicomEviDataExtract::mergeConfig($projectConfig, $evidataGlobal);

            $extract = new DicomEviDataExtract(
                $config['imaging']['dcmdump_path'] ?? '/usr/bin/dcmdump',
                $evidata,
                $logger
            );

            $reportName = sprintf(
                'evidata_imaging_%s_%s.csv',
                $target['project'],
                date('Y-m-d_His')
            );

            $reportPath = $projectPath . '/logs/evidata/' . $reportName;

            $extract->extract($sourceDir, $reportPath);
            $extractSummary = $extract->summary();

            fwrite(STDERR, sprintf(
                "  evidata extract (%s): %d file(s) dumped, %d unreadable, "
                . "%d study row(s), %d column(s), %d QI(s)\n",
                $extractSummary['scan_mode'],
                $extractSummary['files'],
                $extractSummary['unreadable'],
                $extractSummary['studies'],
                $extractSummary['columns'],
                $extractSummary['qis']
            ));

            $populated = $extract->populatedColumns();

            if (!empty($populated)) {
                fwrite(STDERR, sprintf(
                    "  populated header fields: %s\n",
                    implode(', ', array_slice($populated, 0, 12))
                    . (count($populated) > 12 ? ' ...' : '')
                ));
            }

            if (!file_exists($reportPath)) {
                $checkFailure = "extract was not written: {$reportPath}";
            } else {
                fwrite(STDERR, "  extract: {$reportPath}\n");
                fwrite(
                    STDERR,
                    "  NOTE not yet submitted to EviData - see README\n"
                );
            }
        } catch (Throwable $e) {
            $checkFailure = 'evidata extract failed: ' . $e->getMessage();
        }

        if ($checkFailure !== null) {
            fwrite(STDERR, "  ERROR {$checkFailure}\n");
            fwrite(STDERR, "  ABORT EviData check did not complete - skipping "
                . "{$label}, nothing organised or ingested\n\n");
            $logger->error("EviData check failed, aborting {$label}: {$checkFailure}");
            $exitCode = EXIT_FAILURE;
            continue;
        }
    }

    // --- Script API first, direct run as fallback --------------------------
    // The endpoint runs inside LORIS as www-data, on the host that owns the
    // mounts and the virtualenv - the same reason bidsimport and
    // importdicomstudy go through the Script API rather than shelling out.
    //
    // Fallback happens ONLY when the endpoint is absent (404). An auth
    // failure, a timeout or a job that failed are real problems and must
    // surface: quietly running somewhere else would hide a broken endpoint
    // for as long as the two hosts happen to be the same machine.
    $ranViaApi = false;

    if (($config['imaging']['organize_via_api'] ?? false) && !$noApi) {
        try {
            $apiClient = new DicomOrganizeClient($config, $logger);

            if ($apiClient->available()) {
                $logger->info('Organising via the dicomorganize endpoint');

                $apiClient->organize([
                    'source_dir'  => $sourceDir,
                    'target_dir'  => $targetDir,
                    'move'        => $move,
                    'phantom'     => $phantom,
                    'no_unlinked' => !$unlinked,
                    'force'       => $force,
                    'dry_run'     => !$confirm,
                ]);

                $ranViaApi = true;
            } else {
                fwrite(
                    STDERR,
                    "  dicomorganize endpoint not deployed - running directly\n"
                );
            }
        } catch (Throwable $e) {
            if ($e->getMessage() === DicomOrganizeClient::NOT_DEPLOYED) {
                fwrite(
                    STDERR,
                    "  dicomorganize endpoint not deployed - running directly\n"
                );
            } else {
                // Not a deployment gap. Surface it.
                fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n\n");
                $logger->error('dicomorganize failed: ' . $e->getMessage());
                $exitCode = EXIT_FAILURE;
                continue;
            }
        }
    }

    if ($ranViaApi) {
        $manifestPath = $targetDir . '/dicom_studies.json';

        if (file_exists($manifestPath)) {
            $manifest = readJson($manifestPath);
            fwrite(STDERR, sprintf(
                "  %d study/studies organised via API\n",
                $manifest['study_count'] ?? 0
            ));
        }

        fwrite(STDERR, "\n");
        continue;
    }

    try {
        $organizer = new DicomStudyOrganizer($dicomInfoPath, $targetDir, $logger);
        $organizer->setPerl5Lib($config['imaging']['perl5lib'] ?? null);

        // pydicom by default. get_dicom_info.pl remains selectable, but it
        // needs the LORIS-MRI Perl environment, which a pipeline host running
        // as www-data does not necessarily have.
        // dcmdump by default: DCMTK is already required for dcmodify and the
        // EviData extract, so this adds no dependency and keeps the repo
        // PHP-only. pydicom is available for exact parity with
        // import_dicom_study.py; get_dicom_info.pl needs the LORIS-MRI Perl
        // environment and is a last resort.
        switch ($config['imaging']['header_reader'] ?? 'dcmdump') {
            case 'get_dicom_info':
                $organizer->useGetDicomInfoReader();
                break;

            case 'pydicom':
                $organizer->usePydicomReader(
                    $config['imaging']['python_bin']
                    ?? '/opt/LorisMRI/bin/mri/python_virtualenvs/loris-mri-python/bin/python',
                    $config['imaging']['header_dump_script']
                    ?? dirname(__DIR__) . '/bin/dicom_header_dump.py'
                );
                break;

            default:
                $organizer->useDcmdumpReader(
                    $config['imaging']['dcmdump_path'] ?? '/usr/bin/dcmdump'
                );
        }
        $organizer->setDryRun(!$confirm);
        $organizer->setMove($move);
        $organizer->setPhantomRun($phantom);
        $organizer->setFallbackToUnlinked($unlinked);
        $organizer->setForce($force);

        $manifest = $organizer->organize($sourceDir);
        $stats    = $organizer->getStats();

        $summary[$label] = $stats;

        fwrite(STDERR, sprintf(
            "  %d study/studies, %d linked, %d unlinked, %d phantom\n",
            $stats['studies_found'],
            $stats['studies_found'] - $stats['unlinked_studies'] - $stats['phantom_studies'],
            $stats['unlinked_studies'],
            $stats['phantom_studies']
        ));

        // Modality breakdown - the quick answer to "is there CT in here".
        $byModality = [];
        foreach ($manifest as $study) {
            foreach ($study['series'] as $series) {
                $key              = $series['modality'] ?: 'UNKNOWN';
                $byModality[$key] = ($byModality[$key] ?? 0) + 1;
            }
        }

        if (!empty($byModality)) {
            ksort($byModality);
            $parts = [];
            foreach ($byModality as $modality => $count) {
                $parts[] = "{$modality}:{$count}";
            }
            fwrite(STDERR, '  modalities: ' . implode('  ', $parts) . "\n");
        }

        // Recorded only on a clean, non-dry run, so the invariant is
        // "this delivery was fully processed" rather than "we looked at it".
        if (!$organizer->hasErrors() && $confirm) {
            $stateDir = dirname($stateFile);

            if (is_dir($stateDir) || @mkdir($stateDir, 0775, true)) {
                @file_put_contents(
                    $stateFile,
                    json_encode(
                        [
                            'delivery_hash' => $delivery['hash'],
                            'file_count'    => $delivery['file_count'],
                            'source_dir'    => $sourceDir,
                            'completed_at'  => date('c'),
                        ],
                        JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES
                    )
                );
            }
        }

        if ($organizer->hasErrors()) {
            $exitCode = EXIT_FAILURE;
        }
    } catch (Throwable $e) {
        $logger->error($e->getMessage());
        fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n");
        $exitCode = EXIT_FAILURE;
    }

    fwrite(STDERR, "\n");
}

// -----------------------------------------------------------------------------
//  Summary
// -----------------------------------------------------------------------------

if (count($summary) > 1) {
    fwrite(STDERR, "=== Summary ===\n");
    foreach ($summary as $label => $stats) {
        fwrite(STDERR, sprintf(
            "  %-32s %d studies, %d files\n",
            $label,
            $stats['studies_found'],
            $stats['files_organized']
        ));
    }
}

if (!$confirm) {
    fwrite(STDERR, "\nDry run complete. No studies were written.\n");
    fwrite(STDERR, "To organise for real, run:\n\n");
    fwrite(STDERR, '  ' . confirmCommand($argv) . "\n\n");
}

exit($exitCode);