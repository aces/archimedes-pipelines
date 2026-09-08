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
 * Only participants that actually have organised studies are synced, so a
 * delivery is not used to create candidates for subjects who sent no imaging.
 *
 * Dry run is the default; --confirm creates.
 *
 * Usage:
 *   php scripts/run_dicom_participant_sync.php --collection=archimedes --project=FDG-PET -v
 *   php scripts/run_dicom_participant_sync.php --collection=archimedes --project=FDG-PET --confirm
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\LorisApiClientAdapter;
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
Usage: php scripts/run_dicom_participant_sync.php [options]

Creates LORIS candidates for participants in a DICOM delivery, linked by
ExternalID. Existing candidates are matched, never modified.

Selection:
  --all                Process all enabled collections & projects
  --collection=NAME    Process all enabled projects in a collection
  --project=NAME       Process a specific project (requires --collection)

Execution:
  --confirm            Create candidates
  --dry-run            Report without creating anything (the default)
  --all-participants   Sync every row in participants.tsv, not only those
                       with organised studies
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
 * Subject directories present in the raw delivery.
 *
 * Read from disk rather than the manifest, so a sub- folder that produced no
 * readable DICOM is still seen as present. That is the orphan case worth
 * reporting: a directory was delivered and nothing came of it.
 *
 * @return array<string, bool> subject identifier => true
 */
function subjectDirectories(string $projectPath): array
{
    $root = $projectPath . '/deidentified-raw/imaging/dicoms';
    $dirs = [];

    foreach (glob($root . '/sub-*', GLOB_ONLYDIR) ?: [] as $path) {
        $dirs[substr(basename($path), 4)] = true;
    }

    return $dirs;
}

/**
 * Subjects that actually have organised studies, from the stage 2 manifest.
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
        // Only linked studies need a candidate. Unlinked and phantom studies
        // are archived without one by design.
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

$options = getopt(
    'v',
    [
        'all',
        'collection:',
        'project:',
        'confirm',
        'dry-run',
        'all-participants',
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
            "  SKIP no participants.tsv - studies will be archived unlinked\n\n"
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
        $roster = ParticipantsTsv::load($tsvPath, $defaults);
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

    if (!empty($roster->enrichedColumns())) {
        fwrite(STDERR, sprintf(
            "  enriched from candidate_defaults: %s\n",
            implode(', ', $roster->enrichedColumns())
        ));
    }

    $missing = $roster->missingForLoris();

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
                "  SKIP no linked studies in the manifest - run run_dicom_organize.php "
                . "first, or pass --all-participants\n\n"
            );
            continue;
        }

        fwrite(STDERR, sprintf(
            "  %d subject(s) have linked studies\n",
            count($wanted)
        ));
    }

    // --- Orphan reporting -------------------------------------------------
    // This step owns participants.tsv, so it is the only place that sees both
    // sides. Neither direction is fatal - a delivery can legitimately be
    // partial - but both are worth naming before candidates are created.
    $onDisk = subjectDirectories($projectPath);
    $inTsv  = [];

    foreach (array_keys($roster->all()) as $participantId) {
        $inTsv[
            str_starts_with($participantId, 'sub-')
                ? substr($participantId, 4)
                : $participantId
        ] = true;
    }

    $orphanDirs = array_keys(array_diff_key($onDisk, $inTsv));
    $orphanRows = array_keys(array_diff_key($inTsv, $onDisk));

    if (!empty($orphanDirs)) {
        fwrite(STDERR, sprintf(
            "  WARN %d directory/directories with no participants.tsv row: %s\n",
            count($orphanDirs),
            implode(', ', array_map(static fn ($d) => 'sub-' . $d, array_slice($orphanDirs, 0, 8)))
            . (count($orphanDirs) > 8 ? ' ...' : '')
        ));
        fwrite(STDERR, "       their studies will be archived UNLINKED\n");
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

    $matched = 0;
    $created = 0;
    $skipped = 0;
    $failed  = 0;

    try {
        $api = new LorisApiClientAdapter($config, $logger);
        $api->setSite($defaults['site'] ?? null);

        // Resolves ProjectID and the exact LORIS Project.Name from
        // project.json, same priority chain as BidsParticipantSync.
        $api->resolveProject($projectConfig, $config);
    } catch (Throwable $e) {
        fwrite(STDERR, '  ERROR ' . $e->getMessage() . "\n\n");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    foreach ($roster->all() as $participantId => $row) {
        $subject = str_starts_with($participantId, 'sub-')
            ? substr($participantId, 4)
            : $participantId;

        if ($wanted !== null && !isset($wanted[$subject])) {
            $skipped++;
            continue;
        }

        $externalId = $row['external_id'] ?? $subject;

        try {
            $existing = $api->findCandidateByExternalId($externalId);
        } catch (Throwable $e) {
            fwrite(STDERR, "  ERROR lookup {$externalId}: " . $e->getMessage() . "\n");
            $logger->error("Lookup failed for {$externalId}: " . $e->getMessage());
            $failed++;
            $exitCode = EXIT_FAILURE;
            continue;
        }

        if ($existing !== null) {
            $matched++;

            if ($verbose) {
                fwrite(STDERR, sprintf(
                    "    %-16s matched %s (%s)\n",
                    $externalId,
                    $existing['pscid'],
                    $existing['cand_id']
                ));
            }

            continue;
        }

        if (!$confirm) {
            fwrite(STDERR, "    {$externalId}  would create\n");
            $created++;
            continue;
        }

        try {
            $candidate = $api->createCandidate(
                $subject,
                $externalId,
                $row['dob'] ?? null,
                $row['sex'] ?? null
            );

            $created++;

            fwrite(STDERR, sprintf(
                "    %-16s created %s (%s)\n",
                $externalId,
                $candidate['pscid'],
                $candidate['cand_id']
            ));
        } catch (Throwable $e) {
            fwrite(STDERR, "  ERROR create {$externalId}: " . $e->getMessage() . "\n");
            $logger->error("Creation failed for {$externalId}: " . $e->getMessage());
            $failed++;
            $exitCode = EXIT_FAILURE;
        }
    }

    fwrite(STDERR, sprintf(
        "  %d matched, %d %s, %d skipped, %d failed\n\n",
        $matched,
        $created,
        $confirm ? 'created' : 'would create',
        $skipped,
        $failed
    ));
}

if (!$confirm) {
    fwrite(STDERR, "\nDry run complete. No candidates were created.\n");
    fwrite(STDERR, "To create them, run:\n\n");
    fwrite(STDERR, '  ' . confirmCommand($argv) . "\n\n");
}

exit($exitCode);
