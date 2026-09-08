<?php

/**
 * run_dicom_reidentifier.php
 *
 * Step 2 of the DICOM pipeline, mirroring run_bids_reidentifier.php.
 *
 * Maps ExternalIDs to PSCIDs and copies studies into
 * deidentified-lorisid/imaging/dicoms/. Unlike the BIDS reidentifier, which
 * only renames directories, this also rewrites the DICOM patient identity
 * headers on the copy: PatientName becomes PSCID_CandID_VisitLabel and the
 * site's original value is preserved in OtherPatientNames.
 *
 * Runs after run_dicom_organize.php and run_bids_participant_sync.php,
 * and before run_dicom_import.php.
 *
 * Source and target are resolved from project.json -> data_access.mount_path.
 * Dry run is the default; pass --confirm to write.
 *
 * Usage:
 *   php scripts/run_dicom_reidentifier.php --collection=archimedes --project=FDG-PET -v
 *   php scripts/run_dicom_reidentifier.php --collection=archimedes --project=FDG-PET --confirm
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\DicomHeaderWriter;
use LORIS\Pipelines\DicomReidentifier;
use LORIS\Pipelines\ParticipantsTsv;
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
Usage: php scripts/run_dicom_reidentifier.php [options]

Maps ExternalIDs to PSCIDs, copies studies to deidentified-lorisid/imaging/
and rewrites DICOM patient identity headers on the copy.

Runs after run_dicom_organize.php and run_bids_participant_sync.php.

Selection:
  --all                Process all enabled collections & projects
  --collection=NAME    Process all enabled projects in a collection
  --project=NAME       Process a specific project (requires --collection)

Execution:
  --confirm            Execute
  --dry-run            Report without changing anything (the default)
  --force              Reprocess studies already in the tracking file
  --create-candidates  Create candidates this stage cannot resolve. Off by
                       default: run_dicom_participant_sync.php owns candidate
                       creation, because it sees the whole participants.tsv
                       and can report orphans. Use only for a one-off run.
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

            if (empty($project['enabled']) && $wantProject !== $projectName) {
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

    $logger = new Logger('dicom_reidentifier');

    if (is_dir($logDir)) {
        $logger->pushHandler(
            new RotatingFileHandler($logDir . '/dicom_reidentifier.log', 30, Level::Info)
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
        'create-candidates',
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
$create     = isset($options['create-candidates']);
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
    fwrite(STDERR, "No enabled projects matched the selection.\n");
    exit(EXIT_USAGE);
}

$dcmodifyPath = $config['imaging']['dcmodify_path'] ?? '/usr/bin/dcmodify';

if (!is_executable($dcmodifyPath)) {
    fwrite(
        STDERR,
        "ERROR dcmodify not executable: {$dcmodifyPath}\n"
        . "Install the dcmtk package or set imaging.dcmodify_path.\n"
    );
    exit(EXIT_USAGE);
}

if (!$confirm) {
    fwrite(STDERR, "DRY RUN - no files will be written. Pass --confirm to execute.\n\n");
}

// -----------------------------------------------------------------------------
//  Run
// -----------------------------------------------------------------------------

$exitCode = EXIT_OK;

foreach ($targets as $target) {
    $projectPath = $target['path'];
    $label       = "{$target['collection']}/{$target['project']}";

    fwrite(STDERR, "=== {$label} ===\n");

    $manifestPath = $projectPath . '/processed/imaging/dicom_studies.json';

    if (!file_exists($manifestPath)) {
        fwrite(
            STDERR,
            "  SKIP no stage 1 manifest - run run_dicom_organize.php first\n\n"
        );
        continue;
    }

    $targetDir = $projectPath . '/deidentified-lorisid/imaging';

    // Shared with the BIDS pipeline; only the directory differs. No fallback
    // across modalities - a project running both keeps a list per delivery.
    $participants = [];
    $tsvPath      = ParticipantsTsv::locate($projectPath, 'dicom');

    if ($tsvPath === null) {
        fwrite(STDERR, "  WARN no participants.tsv found; demographics unavailable\n");
    } else {
        try {
            $projectConfig = readJson($projectPath . '/project.json');
            $roster        = ParticipantsTsv::load(
                $tsvPath,
                $projectConfig['candidate_defaults'] ?? []
            );

            $participants = $roster->all();

            if (!empty($roster->enrichedColumns())) {
                fwrite(STDERR, sprintf(
                    "  enriched from candidate_defaults: %s\n",
                    implode(', ', $roster->enrichedColumns())
                ));
            }

            $missing = $roster->missingForLoris();
            if (!empty($missing)) {
                fwrite(STDERR, sprintf(
                    "  WARN columns still missing after enrichment: %s\n",
                    implode(', ', $missing)
                ));
            }
        } catch (RuntimeException $e) {
            fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n\n");
            $exitCode = EXIT_FAILURE;
            continue;
        }
    }

    $logger = buildLogger($projectPath, $verbose);

    try {
        // TODO: replace with the project's LORIS API client adapter.
        $api = new \Archimedes\Pipeline\LorisApiClientAdapter($config, $logger);

        $reidentifier = new DicomReidentifier(
            $api,
            new DicomHeaderWriter($dcmodifyPath, $logger),
            $targetDir,
            $logger
        );

        $reidentifier->setDryRun(!$confirm);
        $reidentifier->setCreateCandidates($create);

        $reidentifier->mapFromManifest($manifestPath, $participants);
        $stats = $reidentifier->getStats();

        fwrite(STDERR, sprintf(
            "  %d/%d mapped, %d matched, %d created, %d unlinked, %d phantom\n",
            $stats['studies_mapped'],
            $stats['studies_total'],
            $stats['candidates_matched'],
            $stats['candidates_created'],
            $stats['unlinked_studies'],
            $stats['phantom_studies']
        ));

        fwrite(STDERR, sprintf(
            "  %d file(s) had headers rewritten\n",
            $stats['files_rewritten']
        ));

        if ($reidentifier->hasErrors()) {
            $exitCode = EXIT_FAILURE;
        }
    } catch (Throwable $e) {
        $logger->error($e->getMessage());
        fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n");
        $exitCode = EXIT_FAILURE;
    }

    fwrite(STDERR, "\n");
}

if (!$confirm) {
    fwrite(STDERR, "\nDry run complete. Nothing was copied and no headers rewritten.\n");
    fwrite(STDERR, "To write for real, run:\n\n");
    fwrite(STDERR, '  ' . confirmCommand($argv) . "\n\n");
}

exit($exitCode);
