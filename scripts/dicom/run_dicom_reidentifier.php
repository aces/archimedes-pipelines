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
 * Source and target are resolved from loris_client_config.json -> collections[].base_path/<project>.
 * Dry run is the default; pass --confirm to write.
 *
 * Usage:
 *   php scripts/dicom/run_dicom_reidentifier.php --collection=archimedes --project=FDG-PET -v
 *   php scripts/dicom/run_dicom_reidentifier.php --collection=archimedes --project=FDG-PET --confirm
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\Cli;
use LORIS\Pipelines\Shared\PipelineSetup;
use LORIS\Pipelines\Shared\ProjectLock;
use LORIS\Pipelines\Dicom\DicomHeaderWriter;
use LORIS\Pipelines\Dicom\DicomReidentifier;
use LORIS\Pipelines\Shared\ParticipantsTsv;

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
Usage: php scripts/dicom/run_dicom_reidentifier.php [options]

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
  --exclude-file=FILE  Studies to leave out (written by the bundle when the
                       operator continues with the good studies only)
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
 * Resolve which collection/project pairs to process.
 *
 * @return array<int, array{collection: string, project: string, path: string}>
 */
function resolveTargets(array $config, array $options): array
{
    if (($options['project'] ?? null) !== null && ($options['collection'] ?? null) === null) {
        fwrite(STDERR, "--project requires --collection\n\n");
        usage();
    }

    // One selection rule for every imaging pipeline (Shared\ImagingProject).
    return \LORIS\Pipelines\Shared\ImagingProject::targets($config, $options);
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
    'force',
    'create-candidates',
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
    $config = PipelineSetup::readJson($configPath);
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
$lock     = null;

foreach ($targets as $target) {
    $projectPath = $target['path'];
    $label       = "{$target['collection']}/{$target['project']}";

    fwrite(STDERR, "=== {$label} ===\n");

    // One DICOM run per project at a time (shared with the bundle, which hands
    // its lock over); dry runs do not lock.
    if (!ProjectLock::enter($lock, $projectPath, 'dicom', basename(__FILE__), $confirm)) {
        $exitCode = EXIT_FAILURE;
        fwrite(STDERR, "\n");
        continue;
    }

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
            $projectConfig = PipelineSetup::readJson($projectPath . '/project.json');
            $roster        = ParticipantsTsv::load(
                $tsvPath,
                $projectConfig['candidate_defaults'] ?? [],
                $projectConfig
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

    $logger = PipelineSetup::logger($projectPath, 'dicom', 'reidentifier', $verbose);

    try {
        // Same setup as run_dicom_participant_sync.php: site and LORIS
        // project name are needed for visit labels and new sessions.
        $projectConfig ??= PipelineSetup::readJson($projectPath . '/project.json');

        $api = new \LORIS\Endpoints\LorisApiClientAdapter($config, $logger);
        $api->setSite($projectConfig['candidate_defaults']['site'] ?? null);
        $api->resolveProject($projectConfig, $config);
        $api->setSexMappings($projectConfig['sex_mappings'] ?? []);

        $reidentifier = new DicomReidentifier(
            $api,
            new DicomHeaderWriter($dcmodifyPath, $logger),
            $targetDir,
            $logger
        );

        $reidentifier->setDryRun(!$confirm);
        $reidentifier->setCreateCandidates($create);
        $reidentifier->setForce($force);
        $reidentifier->setExcluded(
            \LORIS\Pipelines\Dicom\DicomPreflight::readExclusions($options['exclude-file'] ?? null)['study_uids']
        );

        $reidentifier->mapFromManifest($manifestPath, $participants);
        $stats = $reidentifier->getStats();

        fwrite(STDERR, sprintf(
            "  %d/%d mapped, %d matched, %d created, %d unlinked (not processed)%s\n",
            $stats['studies_mapped'],
            $stats['studies_total'],
            $stats['candidates_matched'],
            $stats['candidates_created'],
            $stats['unlinked_studies'],
            ($stats['visit_refused'] ?? 0) > 0
                ? sprintf(', %d held (visit not in project.json "visits" or not in LORIS)', $stats['visit_refused'])
                : ''
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
    fwrite(STDERR, '  ' . PipelineSetup::confirmCommand($argv) . "\n\n");
}

exit($exitCode);