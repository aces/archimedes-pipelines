<?php

/**
 * run_dicom_participant_sync.php
 *
 * Step 3 of the DICOM pipeline: create LORIS candidates for the participants
 * in a delivery, linking each to its site identifier via ExternalID.
 *
 * Reads participants.tsv from deidentified-raw/imaging/dicoms/, enriches the
 * LORIS-internal columns from project.json candidate_defaults, and creates any
 * candidate that does not already exist. Existing candidates are matched, never
 * modified - this step only adds.
 *
 * Runs after run_dicom_organize.php and before run_dicom_reidentifier.php.
 * Only participants that actually have organized studies are synced, so a
 * delivery is not used to create candidates for subjects who sent no imaging.
 *
 * Dry run is the default; --confirm creates.
 *
 * Usage:
 *   php scripts/dicom/run_dicom_participant_sync.php --collection=archimedes --project=FDG-PET -v
 *   php scripts/dicom/run_dicom_participant_sync.php --collection=archimedes --project=FDG-PET --confirm
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\Cli;
use LORIS\Endpoints\LorisApiClientAdapter;
use LORIS\Pipelines\Shared\ParticipantsTsv;
use Monolog\Handler\RotatingFileHandler;
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;

const EXIT_OK      = 0;
const EXIT_FAILURE = 1;
const EXIT_USAGE   = 2;

const DEFAULT_CONFIG = __DIR__ . '/../../config/loris_client_config.json';

/**
 * Print usage and exit.
 */
function usage(int $code = EXIT_USAGE): void
{
    fwrite(
        $code === EXIT_OK ? STDOUT : STDERR,
        <<<TXT
Usage: php scripts/dicom/run_dicom_participant_sync.php [options]

Creates LORIS candidates for participants in a DICOM delivery, linked by
ExternalID. Existing candidates are matched, never modified.

Selection:
  --all                Process all enabled collections & projects
  --collection=NAME    Process all enabled projects in a collection
  --project=NAME       Process a specific project (requires --collection)

Execution:
  --confirm            Create candidates
  --dry-run            Report without creating anything (the default)
  --exclude-file=FILE  Participants to leave out (written by the bundle when the
                       operator continues with the good studies only)
  --all-participants   Sync every row in participants.tsv, not only those
                       with organized studies
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

    foreach ($config['collections'] ?? [] as $collection) {
        $collectionName = $collection['name'] ?? null;

        if ($collectionName === null) {
            continue;
        }

        if (empty($collection['enabled']) && $wantCollection !== $collectionName) {
            continue;
        }

        if (!$wantAll && $wantCollection !== null && $wantCollection !== $collectionName) {
            continue;
        }

        $basePath = rtrim($collection['base_path'] ?? '', '/');

        foreach ($collection['projects'] ?? [] as $project) {
            $projectName = $project['name'] ?? null;

            if ($projectName === null) {
                continue;
            }

            if ($wantProject !== null && $wantProject !== $projectName) {
                continue;
            }

            if (!\LORIS\Pipelines\Dicom\DicomStudyOrganizer::acceptProject(
                "{$collectionName}/{$projectName}",
                $basePath . '/' . $projectName,
                !empty($collection['enabled']) && !empty($project['enabled']),
                $wantProject === $projectName
                    || (empty($collection['enabled']) && $wantCollection === $collectionName)
            )) {
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

    $logger = new Logger('dicom_participant_sync');

    if (is_dir($logDir)) {
        $logger->pushHandler(
            new RotatingFileHandler(
                $logDir . '/dicom_participant_sync.log',
                30,
                Level::Info
            )
        );
    }

    $logger->pushHandler(
        new StreamHandler('php://stderr', $verbose ? Level::Debug : Level::Warning)
    );

    return $logger;
}

/**
 * Subjects that actually have organized studies, from the stage 2 manifest.
 *
 * @return array<string, bool> subject identifier => true
 */
function subjectsWithStudies(string $manifestPath): array
{
    $subjects = [];

    if (!file_exists($manifestPath)) {
        return $subjects;
    }

    try {
        $manifest = readJson($manifestPath);
    } catch (RuntimeException) {
        return $subjects;
    }

    foreach ($manifest['studies'] ?? [] as $study) {
        // Only matched studies need a candidate. Unlinked studies are only
        // reported, never synced or ingested.
        if (($study['link_status'] ?? 'linked') !== 'linked') {
            continue;
        }

        $subject = $study['subject'] ?? null;

        if ($subject !== null && $subject !== '') {
            $subjects[$subject] = true;
        }
    }

    return $subjects;
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

$shortOpts = 'v';
$longOpts  = [
    'all',
    'collection:',
    'project:',
    'confirm',
    'dry-run',
    'all-participants',
    'exclude-file:',
    'config:',
    'verbose',
    'help',
];
$options = getopt($shortOpts, $longOpts);
// getopt() ignores options it does not know; a typo or a removed option
// must not run with the default instead.
Cli::exitOnUnknownOptions($shortOpts, $longOpts);

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
$confirm         = isset($options['confirm'])
    && !isset($options['dry-run']);

if (isset($options['confirm']) && isset($options['dry-run'])) {
    fwrite(STDERR, "Both --confirm and --dry-run given - treating as dry run.\n\n");
}
$allParticipants = isset($options['all-participants']);
$verbose         = isset($options['v']) || isset($options['verbose']);
$configPath      = $options['config'] ?? DEFAULT_CONFIG;

try {
    $config = readJson($configPath);
} catch (RuntimeException $e) {
    fwrite(STDERR, 'ERROR ' . $e->getMessage() . "\n");
    exit(EXIT_USAGE);
}

$targets = resolveTargets($config, $options);

if (empty($targets)) {
    fwrite(STDERR, "No enabled projects matched the selection.\n");
    exit(EXIT_USAGE);
}

if (!$confirm) {
    fwrite(STDERR, "DRY RUN - no candidates will be created. Pass --confirm to execute.\n\n");
}

// -----------------------------------------------------------------------------
//  Run
// -----------------------------------------------------------------------------

$exitCode = EXIT_OK;

foreach ($targets as $target) {
    $projectPath = $target['path'];
    $label       = "{$target['collection']}/{$target['project']}";

    fwrite(STDERR, "=== {$label} ===\n");

    $tsvPath = ParticipantsTsv::locate($projectPath, 'dicom');

    if ($tsvPath === null) {
        fwrite(
            STDERR,
            "  SKIP no participants.tsv - its studies are UNLINKED: reported only, nothing created\n\n"
        );
        continue;
    }

    try {
        $projectConfig = readJson($projectPath . '/project.json');
    } catch (RuntimeException $e) {
        fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n\n");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    $defaults = $projectConfig['candidate_defaults'] ?? [];

    try {
        $loaded = \LORIS\Pipelines\Shared\ParticipantSync::load($tsvPath, $projectConfig);
        $roster = $loaded['roster'];
    } catch (RuntimeException $e) {
        fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n\n");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    fwrite(STDERR, sprintf(
        "  %d participant row(s) in %s\n",
        $roster->count(),
        basename($tsvPath)
    ));

    if (!empty($loaded['enriched'])) {
        fwrite(STDERR, sprintf(
            "  enriched from candidate_defaults: %s\n",
            implode(', ', $loaded['enriched'])
        ));
    }

    $missing = $loaded['missing'];

    if (!empty($missing)) {
        fwrite(STDERR, sprintf(
            "  ERROR columns missing after enrichment: %s\n",
            implode(', ', $missing)
        ));
        fwrite(
            STDERR,
            "  Add them to participants.tsv or to project.json candidate_defaults.\n\n"
        );
        $exitCode = EXIT_FAILURE;
        continue;
    }

    // By default only sync participants that actually sent imaging, so a
    // delivery does not create candidates for subjects with no studies.
    $wanted = null;

    if (!$allParticipants) {
        $manifestPath = $projectPath . '/processed/imaging/dicom_studies.json';
        $wanted       = subjectsWithStudies($manifestPath);

        if (empty($wanted)) {
            fwrite(
                STDERR,
                "  SKIP no study matches a participant in the manifest - run run_dicom_organize.php "
                . "first, or pass --all-participants\n\n"
            );
            continue;
        }

        fwrite(STDERR, sprintf(
            "  %d participant(s) with matched studies\n",
            count($wanted)
        ));
    }

    // Participants the operator chose to leave out (bundle: issues reported,
    // "continue with the good studies"): never created in this run.
    $exclusions = \LORIS\Pipelines\Dicom\DicomPreflight::readExclusions($options['exclude-file'] ?? null);
    $excludedSubjects = array_flip($exclusions['subjects']);
    if (!empty($excludedSubjects)) {
        fwrite(STDERR, sprintf(
            "  %d participant(s) excluded by the operator (issues reported): %s\n",
            count($excludedSubjects),
            implode(', ', array_map(static fn ($x) => 'sub-' . $x, array_keys($excludedSubjects)))
        ));
    }

    // --- Orphan reporting -------------------------------------------------
    // This step owns participants.tsv, so it is the only place that sees both
    // sides. Neither direction is fatal - a delivery can legitimately be
    // partial - but both are worth naming before candidates are created.
    $orphans    = \LORIS\Pipelines\Shared\ParticipantSync::orphans(
        $roster,
        $projectPath . '/deidentified-raw/imaging/dicoms'
    );
    $orphanDirs = $orphans['dirs'];
    $orphanRows = $orphans['rows'];

    $nestedDirs = \LORIS\Pipelines\Shared\ParticipantSync::nestedSubjectDirs(
        $projectPath . '/deidentified-raw/imaging/dicoms'
    );
    if (!empty($nestedDirs)) {
        fwrite(STDERR, sprintf(
            "  WARN %d sub- folder(s) inside another participant's folder (copied one level too deep?): %s\n",
            count($nestedDirs),
            implode(', ', array_slice($nestedDirs, 0, 8)) . (count($nestedDirs) > 8 ? ' ...' : '')
        ));
    }

    if (!empty($orphanDirs)) {
        fwrite(STDERR, sprintf(
            "  WARN %d directory/directories with no participants.tsv row: %s\n",
            count($orphanDirs),
            implode(', ', array_map(static fn ($d) => 'sub-' . $d, array_slice($orphanDirs, 0, 8)))
            . (count($orphanDirs) > 8 ? ' ...' : '')
        ));
        fwrite(STDERR, "       their studies are UNLINKED: reported only, nothing created or ingested\n");
    }

    if (!empty($orphanRows)) {
        fwrite(STDERR, sprintf(
            "  WARN %d participants.tsv row(s) with no directory: %s\n",
            count($orphanRows),
            implode(', ', array_map(static fn ($d) => 'sub-' . $d, array_slice($orphanRows, 0, 8)))
            . (count($orphanRows) > 8 ? ' ...' : '')
        ));
        fwrite(STDERR, "       no imaging delivered for them\n");
    }

    $logger = buildLogger($projectPath, $verbose);

    if (!empty($orphanDirs)) {
        $logger->warning(sprintf(
            'Orphan directories (no participants.tsv row): %s',
            implode(', ', $orphanDirs)
        ));
    }

    if (!empty($orphanRows)) {
        $logger->warning(sprintf(
            'Orphan participants.tsv rows (no directory): %s',
            implode(', ', $orphanRows)
        ));
    }

    try {
        $api = new LorisApiClientAdapter($config, $logger);
        $api->setSite($defaults['site'] ?? null);

        // Resolves ProjectID and the exact LORIS Project.Name from
        // project.json, same priority chain as BidsParticipantSync.
        $api->resolveProject($projectConfig, $config);
        $api->setSexMappings($projectConfig['sex_mappings'] ?? []);
    } catch (Throwable $e) {
        fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n\n");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    // --- Pre-flight: every mismatch, before any candidate is created --------
    // participants.tsv values and identifiers, visit labels against LORIS,
    // studies that would go in without a participant - all in one list. Any
    // error: nothing is created for this project (dry run or not).
    $pre = \LORIS\Pipelines\Dicom\DicomPreflight::check(
        $projectPath,
        $projectConfig,
        static fn (): array => $api->getConfiguredVisitLabels(),
        $exclusions
    );

    fwrite(STDERR, sprintf(
        "  PRE-FLIGHT (before any candidate is created): %d error(s), %d warning(s)\n",
        count($pre['errors']),
        count($pre['warnings'])
    ));
    foreach ($pre['errors'] as $line) {
        fwrite(STDERR, "    ERROR   {$line}\n");
        $logger->error("Pre-flight: {$line}");
    }
    foreach ($pre['warnings'] as $line) {
        fwrite(STDERR, "    warning {$line}\n");
    }

    if (!empty($pre['errors'])) {
        fwrite(STDERR, "  STOP nothing created - fix the error(s) above and re-run\n\n");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    $n = \LORIS\Pipelines\Shared\ParticipantSync::sync(
        $roster,
        $api,
        $wanted,
        $excludedSubjects,
        $confirm,
        static function (string $line): void {
            fwrite(STDERR, $line . "\n");
        },
        $logger,
        $verbose
    );
    [$matched, $created, $skipped, $failed] = [$n['matched'], $n['created'], $n['skipped'], $n['failed']];
    if ($failed > 0) {
        $exitCode = EXIT_FAILURE;
    }

    // Warnings for the import email (it is the run's one summary). Rewritten on
    // every real run, so a fixed difference disappears; a dry run writes nothing.
    if ($confirm) {
        $warnFile = $projectPath . '/processed/imaging/dicom_participant_sync.json';
        if (is_dir(dirname($warnFile)) || @mkdir(dirname($warnFile), 0775, true)) {
            @file_put_contents($warnFile, json_encode([
                'generated_at' => date('c'),
                'problems'     => array_map(
                    static fn (string $m): array => ['level' => 'warning', 'message' => $m],
                    $n['differs']
                ),
            ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        }
    }

    fwrite(STDERR, sprintf(
        "  %d matched, %d %s, %d skipped, %d failed%s\n\n",
        $matched,
        $created,
        $confirm ? 'created' : 'would create',
        $skipped,
        $failed,
        $n['differs'] === [] ? '' : sprintf(', %d differ from LORIS (warning)', count($n['differs']))
    ));
}

if (!$confirm) {
    fwrite(STDERR, "\nDry run complete. No candidates were created.\n");
    fwrite(STDERR, "To create them, run:\n\n");
    fwrite(STDERR, '  ' . confirmCommand($argv) . "\n\n");
}

exit($exitCode);
