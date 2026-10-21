<?php

/**
 * run_dicom_bundle.php
 *
 * Bundle runner for the DICOM chain.
 *
 * This script ingests nothing itself - run_dicom_import.php, called last, is
 * the only step that writes to the LORIS imaging tables. Everything before it
 * is preparation and can be deleted and redone freely. Sequences the individual step
 * scripts and stops at the first failure.
 *
 *   1. run_dicom_organize.php        eviData audit (gate) + group by StudyInstanceUID
 *   2. run_dicom_participant_sync.php  candidates          - only with participants.tsv
 *   3. run_dicom_reidentifier.php    ExternalID -> PSCID   - only with participants.tsv
 *   4. run_dicom_import.php          archive into tarchive
 *
 * Each step is invoked as a subprocess, so the individual scripts remain the
 * single implementation and keep their own logging and exit codes. This script
 * only sequences them and decides the branch.
 *
 * Branching is driven by the manifest rather than by the presence of a file:
 * after organizing, any study with link_status other than 'linked' would be
 * archived without a participant link. When there are any, the run stops and
 * asks before continuing.
 *
 * Dry run is the default. --confirm executes.
 *
 * Usage:
 *   php scripts/dicom/run_dicom_bundle.php --collection=archimedes --project=FDG-PET -v
 *   php scripts/dicom/run_dicom_bundle.php --collection=archimedes --project=FDG-PET --confirm
 *   php scripts/dicom/run_dicom_bundle.php --all --confirm --yes     # unattended
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../../vendor/autoload.php';

use LORIS\Pipelines\Shared\Cli;

const EXIT_OK       = 0;
const EXIT_FAILURE  = 1;
const EXIT_USAGE    = 2;
const EXIT_ABORTED  = 3;

const DEFAULT_CONFIG = __DIR__ . '/../../config/loris_client_config.json';

/**
 * Print usage and exit.
 */
function usage(int $code = EXIT_USAGE): void
{
    fwrite(
        $code === EXIT_OK ? STDOUT : STDERR,
        <<<TXT
Usage: php scripts/dicom/run_dicom_bundle.php [options]

Runs the full DICOM ingestion chain:
  1. run_dicom_organize.php          eviData audit (gate) + grouping
  2. run_dicom_participant_sync.php  candidates   (skipped when no study matches a participant)
  3. run_dicom_reidentifier.php      sessions + PSCID_CandID_Visit reidentify
  4. run_dicom_import.php            archive, from deidentified-lorisid/ only

Runs only enabled projects whose project.json lists the Imaging modality.
If organize, sync or reidentify fails, NOTHING is ingested for that project.
Without --confirm every step is a dry run.

Selection:
  --all                Process all enabled collections & projects
  --collection=NAME    Process all enabled projects in a collection
  --project=NAME       Process a specific project (requires --collection)

Execution:
  --confirm            Execute
  --dry-run            Report without changing anything (the default)
  --force              Redo regardless of hashes (organize, reidentify, import)
  --no-unlinked        Stop the project if any study has no participant
                       (they are never ingested either way)
  --continue-good      When data issues are found, continue with the good studies
                       without asking (unattended runs). Without it the bundle
                       lists every issue and asks; with no answer it aborts
  --evidata            Run the EviData header check even if project.json
                       has evidata.imaging.enabled false
  --no-evidata         Skip the EviData check for this run

Step selection:
  --steps=LIST         Run only these steps, comma separated
                       (organize, sync, reidentify, import).
                       --steps=organize,sync,reidentify prepares everything
                       and ingests nothing
  --skip=LIST          Run everything except these steps (--skip=import is
                       the same as the --steps example above)

Studies reidentified to a participant are imported with --session, attached to
the visit the reidentifier created. Unlinked studies are never ingested.
An option this script does not know is an error (exit 2), never ignored.
  --config=FILE        Config file (default: config/loris_client_config.json)
  -v, --verbose        Pass -v to each step
  --help               Show this message

Exit codes: 0 success, 1 a step failed, 2 usage error, 3 aborted by operator

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

        // Named explicitly only when --collection was given; matches() is also
        // true when no name was given, which made --all include disabled ones.
        $collectionNamed = $wantCollection !== null && $collectionWanted;

        if (empty($collection['enabled']) && !$collectionNamed) {
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

            $projectNamed = $wantProject !== null && $projectWanted;

            if (!\LORIS\Pipelines\Dicom\DicomStudyOrganizer::acceptProject(
                "{$collectionName}/{$projectName}",
                $basePath . '/' . $projectName,
                !empty($collection['enabled']) && !empty($project['enabled']),
                $projectNamed || ($collectionNamed && empty($collection['enabled']))
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
 * Append a line to the master log.
 *
 * The bundle keeps its own log of the orchestration - which steps ran, their
 * exit codes, how the unlinked prompt was resolved - and also copies each
 * step's output into it. Every step still writes its own log; this is the
 * single file that shows a whole run.
 */
function masterLog(?string $path, string $line): void
{
    if ($path === null) {
        return;
    }

    @file_put_contents(
        $path,
        sprintf("[%s] %s\n", date('Y-m-d H:i:s'), rtrim($line, "\r\n")),
        FILE_APPEND
    );
}

/**
 * Run one step script as a subprocess, streaming its output and copying it
 * into the master log.
 *
 * proc_open rather than passthru so the output can go two places at once.
 * stderr is merged into stdout because the steps write progress there.
 *
 * @param string      $script    Basename in scripts/dicom/.
 * @param string[]    $args      Additional arguments.
 * @param string|null $logPath   Master log, or null to only stream.
 *
 * @return int Exit code.
 */
function runStep(string $script, array $args, ?string $logPath = null): int
{
    $path = __DIR__ . '/' . $script;

    if (!file_exists($path)) {
        $message = "ERROR step script not found: {$path}";
        fwrite(STDERR, "  {$message}\n");
        masterLog($logPath, $message);
        return EXIT_FAILURE;
    }

    $cmd = 'php ' . escapeshellarg($path);

    foreach ($args as $arg) {
        $cmd .= ' ' . escapeshellarg($arg);
    }

    fwrite(STDERR, "--- {$script}\n");
    masterLog($logPath, "=== BEGIN {$script}");
    masterLog($logPath, "    {$cmd}");

    $descriptors = [
        0 => ['file', '/dev/null', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];

    // The unlinked prompt is asked by this script, not the steps, so no step
    // needs stdin. Giving them /dev/null means a step that unexpectedly reads
    // stdin fails fast instead of hanging a cron run.
    $process = proc_open($cmd, $descriptors, $pipes);

    if (!is_resource($process)) {
        $message = "ERROR could not start {$script}";
        fwrite(STDERR, "  {$message}\n");
        masterLog($logPath, $message);
        return EXIT_FAILURE;
    }

    stream_set_blocking($pipes[1], false);
    stream_set_blocking($pipes[2], false);

    $buffers  = [1 => '', 2 => ''];

    // Some steps abort before doing any work - unresolvable host, failed
    // auth, unreachable mount - and still exit 0, because their failure
    // counters only cover studies they actually examined. Watching the output
    // for those signatures means the chain stops instead of reporting a
    // success-shaped run that ingested nothing.
    $abortSignals = [
        'Could not resolve host'    => 'the LORIS host name does not resolve',
        'Connection refused'        => 'the LORIS host refused the connection',
        'Connection timed out'      => 'the LORIS host did not respond',
        'SSL certificate problem'   => 'the LORIS TLS certificate was rejected',
        'Authentication failed'     => 'LORIS authentication failed',
        'aborting'                  => 'the step aborted',
    ];

    $abortReason = null;

    while (!feof($pipes[1]) || !feof($pipes[2])) {
        $read   = [$pipes[1], $pipes[2]];
        $write  = null;
        $except = null;

        if (stream_select($read, $write, $except, 1) === false) {
            break;
        }

        foreach ($read as $pipe) {
            $index = ($pipe === $pipes[1]) ? 1 : 2;
            $chunk = fread($pipe, 8192);

            if ($chunk === false || $chunk === '') {
                continue;
            }

            $buffers[$index] .= $chunk;

            // Emit whole lines only, so the master log is not fragmented.
            while (($newline = strpos($buffers[$index], "\n")) !== false) {
                $line             = substr($buffers[$index], 0, $newline);
                $buffers[$index]  = substr($buffers[$index], $newline + 1);

                fwrite(STDERR, $line . "\n");
                masterLog($logPath, '  ' . $line);

                if ($abortReason === null) {
                    foreach ($abortSignals as $needle => $meaning) {
                        if (stripos($line, $needle) !== false) {
                            $abortReason = $meaning;
                            break;
                        }
                    }
                }
            }
        }
    }

    foreach ($buffers as $index => $remainder) {
        if ($remainder !== '') {
            fwrite(STDERR, $remainder . "\n");
            masterLog($logPath, '  ' . $remainder);
        }
    }

    fclose($pipes[1]);
    fclose($pipes[2]);

    $exitCode = proc_close($process);

    masterLog($logPath, "=== END {$script} (exit {$exitCode})");

    // A step that reported an abort but exited 0 is worse than one that
    // failed loudly: the chain continues and the run summary claims success.
    // Treat the message as authoritative over the exit code.
    if ($exitCode === EXIT_OK && $abortReason !== null) {
        fwrite(STDERR, sprintf(
            "\n  %s reported an abort (%s) but exited 0 - treating as failed.\n",
            $script,
            $abortReason
        ));
        masterLog(
            $logPath,
            "OVERRIDE {$script} exited 0 but aborted: {$abortReason}"
        );

        return EXIT_FAILURE;
    }

    return $exitCode;
}

/**
 * Ask the operator a yes/no question.
 *
 * Refuses to assume yes when stdin is not a terminal - an unattended run must
 * pass --yes explicitly rather than silently ingesting unlinked studies.
 */
/**
 * Stop a project before ingestion because a preparation step failed.
 *
 * Participant sync and reidentification are all-or-nothing gates: if any
 * candidate could not be created/matched or any study could not be reidentified
 * (or its session created), NOTHING is ingested for the project - not even the
 * studies that did succeed. Ingesting a partial set would leave some of a
 * participant's visits in LORIS and others not, and the run would look
 * finished. Everything before the import is reversible, so re-running after the
 * fix picks up exactly where this stopped.
 *
 * On --confirm runs the project's DICOM error recipients get a FAILED email,
 * because the import step - which sends the usual summary - never runs.
 */
function stopBeforeIngestion(
    string $label,
    string $step,
    string $projectPath,
    array $config,
    ?string $masterLogPath,
    bool $confirm
): void {
    $message = "{$step} failed - INGESTION ABORTED for {$label}. Nothing was "
        . 'sent to LORIS for this project, including the studies that did succeed. '
        . 'Studies already in LORIS are unchanged. Fix the error above and re-run '
        . 'the bundle; only what is still missing will be processed.';

    fwrite(STDERR, "
  STOP {$message}
");
    masterLog($masterLogPath, "STOP {$message}");

    if (!$confirm) {
        return;
    }

    $projectJson = $projectPath . '/project.json';
    $project     = is_file($projectJson)
        ? (json_decode((string) file_get_contents($projectJson), true) ?: [])
        : [];
    $notif = $project['notification_emails']['dicom']
        ?? $config['notification_emails']['dicom']
        ?? $config['notification_defaults']
        ?? [];

    if (!($notif['enabled'] ?? false)) {
        return;
    }

    $to = $notif['on_error'] ?? $config['notification_defaults']['default_on_error'] ?? [];
    $projectName = basename($projectPath);
    $body = "Project: {$projectName} (dicom)\n"
        . "Status:  FAILED - stopped before ingestion\n"
        . "Step:    {$step}\n\n"
        . "Nothing was ingested for this project in this run.\n"
        . "Studies already in LORIS are unchanged. Fix the error and re-run.\n"
        . ($masterLogPath ? "\nBundle log: {$masterLogPath}\n" : '');

    $mailer = new \LORIS\Utils\Notification();
    foreach ((array) $to as $address) {
        $mailer->send($address, "FAILED: {$projectName} DICOM Import (stopped at {$step})", $body);
    }
}

/**
 * DATA ISSUES report: every issue, grouped by what it affects, and the good
 * studies that could go ahead.
 */
function dataIssuesReport(array $plan, int $issueCount): string
{
    $out = sprintf("  DATA ISSUES - found before any candidate is created (%d):\n", $issueCount);

    if (!empty($plan['abort'])) {
        $out .= "    whole project (cannot continue):\n";
        foreach ($plan['abort'] as $m) {
            $out .= "      - {$m}\n";
        }
    }
    foreach ($plan['bad'] as $what => $reasons) {
        $out .= "    {$what}\n";
        foreach ($reasons as $m) {
            $out .= "      - {$m}\n";
        }
    }

    $out .= sprintf(
        "  GOOD: %d participant study/studies with no issue%s\n",
        count($plan['good']),
        empty($plan['good']) ? '' : ': ' . implode(', ', array_slice($plan['good'], 0, 12))
            . (count($plan['good']) > 12 ? ' ...' : '')
    );

    return $out;
}

/**
 * The UNLINKED report: every study with no participant, where it was
 * delivered, why, and the exact commands to get it in.
 */
function unlinkedReport(string $manifestPath, array $target, string $configPath): string
{
    $manifest = is_file($manifestPath)
        ? (json_decode((string) file_get_contents($manifestPath), true) ?: [])
        : [];
    $out = '';
    $n   = 0;
    foreach ($manifest['studies'] ?? [] as $s) {
        if (($s['link_status'] ?? '') !== 'unlinked') {
            continue;
        }
        $n++;
        $where = ($s['subject'] ?? null) !== null
            ? sprintf('sub-%s/ses-%s', $s['subject'], $s['session'] ?? '?')
            : ($s['directory_name'] ?? '?');
        $out .= sprintf("    %s  [%s]\n      %s\n",
            $where,
            implode('-', $s['modalities'] ?? []),
            \LORIS\Pipelines\Dicom\DicomPreflight::unlinkedAdvice($s['link_reason'] ?? null, $s['subject'] ?? null)
        );
    }

    return sprintf("  UNLINKED %d study/studies - no matching participant or session: not synced, not reidentified, NOT ingested:\n", $n)
        . $out;
}

function confirmPrompt(string $question, bool $assumeYes): bool
{
    if ($assumeYes) {
        fwrite(STDERR, "{$question} [yes - answered by command-line option]\n");
        return true;
    }

    if (!stream_isatty(STDIN)) {
        fwrite(STDERR, "{$question}\n");
        fwrite(STDERR, "  stdin is not a terminal and --yes was not given.\n");
        return false;
    }

    fwrite(STDERR, "{$question} [y/N]: ");
    $answer = trim((string) fgets(STDIN));

    return strtolower($answer) === 'y' || strtolower($answer) === 'yes';
}

/**
 * Count studies by link status in a stage 0 manifest.
 *
 * @return array{total: int, linked: int, unlinked: int}
 */
function manifestCounts(string $manifestPath): array
{
    $counts = ['total' => 0, 'linked' => 0, 'unlinked' => 0];

    if (!file_exists($manifestPath)) {
        return $counts;
    }

    try {
        $manifest = readJson($manifestPath);
    } catch (RuntimeException) {
        return $counts;
    }

    foreach ($manifest['studies'] ?? [] as $study) {
        $counts['total']++;
        $status = $study['link_status'] ?? 'linked';

        if (isset($counts[$status])) {
            $counts[$status]++;
        }
    }

    return $counts;
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
    'force',
    'no-unlinked',
    'continue-good',
    'evidata',
    'no-evidata',
    'steps:',
    'skip:',
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
$noUnlinked = isset($options['no-unlinked']);
$continueGood    = isset($options['continue-good']);
$evidataFlag = isset($options['no-evidata']) ? ['--no-evidata']
    : (isset($options['evidata']) ? ['--evidata'] : []);
$verbose    = isset($options['v']) || isset($options['verbose']);
$configPath = $options['config'] ?? DEFAULT_CONFIG;

$allSteps = ['organize', 'sync', 'reidentify', 'import'];

// Step selection: --steps is a whitelist, --skip subtracts. A step that is off is reported, not silently absent, so a run
// that did less than expected says so.
$enabledSteps = $allSteps;

if (isset($options['steps'])) {
    $requested = array_filter(array_map('trim', explode(',', $options['steps'])));
    $unknown   = array_diff($requested, $allSteps);

    if (!empty($unknown)) {
        fwrite(STDERR, 'Unknown step(s): ' . implode(', ', $unknown) . "\n");
        fwrite(STDERR, 'Valid steps: ' . implode(', ', $allSteps) . "\n\n");
        usage();
    }

    $enabledSteps = $requested;
}

$skip = [];

if (isset($options['skip'])) {
    $skip = array_filter(array_map('trim', explode(',', $options['skip'])));
    $unknown = array_diff($skip, $allSteps);

    if (!empty($unknown)) {
        fwrite(STDERR, 'Unknown step(s): ' . implode(', ', $unknown) . "\n\n");
        usage();
    }
}

$enabledSteps = array_values(array_diff($enabledSteps, $skip));

if (empty($enabledSteps)) {
    fwrite(STDERR, "Every step is disabled - nothing to do.\n");
    exit(EXIT_USAGE);
}

/**
 * Is a step enabled for this run?
 */
$stepEnabled = static function (string $step) use ($enabledSteps): bool {
    return in_array($step, $enabledSteps, true);
};

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

// --- Reachability preflight ------------------------------------------------
// Every step that talks to LORIS will fail the same way if the host is wrong,
// each after its own DNS or connect timeout. Checking once here turns a
// multi-minute run that ends in a confusing "0 studies found" into a two
// second failure that names the problem.
$lorisUrl = $config['loris']['base_url'] ?? $config['api']['base_url'] ?? null;

if ($lorisUrl !== null && $lorisUrl !== '') {
    $host = parse_url($lorisUrl, PHP_URL_HOST);

    if ($host === null || $host === false) {
        fwrite(STDERR, "ERROR loris.base_url is not a valid URL: {$lorisUrl}\n");
        exit(EXIT_USAGE);
    }

    // A literal IP needs no resolution; gethostbyname returns the input
    // unchanged when it cannot resolve, which is how a failure is detected.
    if (filter_var($host, FILTER_VALIDATE_IP) === false
        && gethostbyname($host) === $host
    ) {
        fwrite(STDERR, "ERROR Cannot resolve the LORIS host '{$host}'.\n\n");
        fwrite(STDERR, "  loris.base_url is {$lorisUrl}\n");
        fwrite(STDERR, "  Nothing that talks to LORIS can work until this "
            . "resolves:\n");
        fwrite(STDERR, "  candidate lookup, candidate creation, session "
            . "creation and the import.\n\n");
        fwrite(STDERR, "  Check:\n");
        fwrite(STDERR, "    hostname -f\n");
        fwrite(STDERR, "    getent hosts {$host}\n");
        fwrite(STDERR, "    grep -n base_url " . $configPath . "\n\n");
        fwrite(STDERR, "  If LORIS is on this machine, https://localhost is "
            . "usually right.\n");
        exit(EXIT_USAGE);
    }
}

if (!$confirm) {
    fwrite(STDERR, "DRY RUN - no files written, nothing ingested. Pass --confirm to execute.\n\n");
}

// -----------------------------------------------------------------------------
//  Run
// -----------------------------------------------------------------------------

$exitCode = EXIT_OK;

foreach ($targets as $target) {
    $projectPath = $target['path'];
    $label       = "{$target['collection']}/{$target['project']}";
    $projectStart = time();

    // One bundle run per project at a time (released when the next project
    // starts or the script exits).
    $bundleLock = @fopen($projectPath . '/.dicom_bundle.lock', 'c');
    if ($bundleLock === false || !flock($bundleLock, LOCK_EX | LOCK_NB)) {
        fwrite(STDERR, "\n  SKIP {$label}: another bundle run is in progress for this project\n");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    // A delivery still being uploaded is left alone - nothing is read, so a
    // half-written participants.tsv cannot create wrong candidates.
    $waitReason = \LORIS\Pipelines\Dicom\DicomStudyOrganizer::uploadInProgress(
        $projectPath . '/deidentified-raw/imaging/dicoms',
        (int) ($config['imaging']['delivery_quiet_minutes'] ?? 5)
    );
    if ($waitReason !== null && !$force) {
        fwrite(STDERR, "\n  WAIT {$label}: {$waitReason}\n");
        continue;
    }

    fwrite(STDERR, "\n========================================\n");
    fwrite(STDERR, "  {$label}\n");
    fwrite(STDERR, "========================================\n");

    // Master log: the orchestration plus a copy of every step's output.
    // Each step still writes its own log alongside this one.
    $logDir = $projectPath . '/logs/dicom';

    if (!is_dir($logDir)) {
        @mkdir($logDir, 0775, true);
    }

    $masterLogPath = is_dir($logDir)
        ? $logDir . '/dicom_bundle_' . date('Y-m-d') . '.log'
        : null;

    if ($masterLogPath === null) {
        fwrite(STDERR, "  WARN could not create {$logDir} - no master log\n");
    }

    masterLog($masterLogPath, str_repeat('=', 60));
    masterLog($masterLogPath, "RUN {$label}");
    masterLog($masterLogPath, sprintf(
        'mode=%s steps=%s%s',
        $confirm ? 'confirm' : 'dry-run',
        implode(',', $enabledSteps),
        $force ? ' force' : ''
    ));

    // Arguments every step accepts.
    $common = [
        '--collection=' . $target['collection'],
        '--project=' . $target['project'],
        '--config=' . $configPath,
    ];

    if ($confirm) {
        $common[] = '--confirm';
    }

    if ($verbose) {
        $common[] = '-v';
    }

    // --force is not universal. getopt ignores options a script does not
    // declare, so passing it everywhere would silently do nothing rather
    // than erroring - only add it where it is handled.
    $withForce = $force ? array_merge($common, ['--force']) : $common;

    // --- Step 1: eviData audit + organize --------------------------------------
    // The audit gate lives inside the organize runner and aborts there, so a
    // non-zero exit here already means "do not ingest this project".
    if (!$stepEnabled('organize')) {
        fwrite(STDERR, "  SKIP organize (disabled)\n");
        masterLog($masterLogPath, 'SKIP organize (disabled)');
        $code = EXIT_OK;
    } else {
        // EviData is optional per modality (project.json evidata.imaging.enabled);
        // --evidata / --no-evidata override it for this run only.
        $code = runStep(
            'run_dicom_organize.php',
            array_merge($withForce, $evidataFlag),
            $masterLogPath
        );
    }

    // Organize exits 1 both when it could not run at all (unreadable
    // delivery, EviData gate, no participant_id column...) and when it ran
    // and found data issues in some studies. Only a fresh manifest means the
    // latter; those issues are reported with the rest below.
    if ($code !== EXIT_OK) {
        $orgManifest  = $projectPath . '/processed/imaging/dicom_studies.json';
        $orgGenerated = is_file($orgManifest)
            ? (strtotime((string) ((json_decode((string) file_get_contents($orgManifest), true) ?: [])['generated_at'] ?? '')) ?: 0)
            : 0;
        if ($orgGenerated < $projectStart - 1) {
            stopBeforeIngestion($label, 'organize', $projectPath, $config, $masterLogPath, $confirm);
            $exitCode = EXIT_FAILURE;
            continue;
        }
        $exitCode = EXIT_FAILURE;   // issues were found; reported below
    }

    // --- Data issues: report all, then continue with the good ones or abort ---
    // Every issue from organize and the pre-flight, before any candidate is
    // created. Whole-project issues abort. Otherwise the operator chooses:
    // continue with the good studies (the others are not created,
    // reidentified or ingested this run), or abort until all data is good.
    $exclusionFile = null;
    $excludedLive  = $projectPath . '/processed/imaging/.dicom_excluded.json';

    if ($stepEnabled('sync') || $stepEnabled('reidentify') || $stepEnabled('import')) {
        $projectConfig = is_file($projectPath . '/project.json')
            ? (json_decode((string) file_get_contents($projectPath . '/project.json'), true) ?: [])
            : [];
        // project.json "visits" (the LORIS API cannot list configured visits)
        $visitLabels = static fn (): array => array_values(array_filter(
            array_map('strval', (array) ($projectConfig['visits'] ?? []))
        ));

        $pre  = \LORIS\Pipelines\Dicom\DicomPreflight::check($projectPath, $projectConfig, $visitLabels);
        $plan = \LORIS\Pipelines\Dicom\DicomPreflight::plan($projectPath, $pre['issues']);

        if (empty($pre['issues'])) {
            if ($confirm && is_file($excludedLive)) {
                @unlink($excludedLive);
            }
        } else {
            $report = dataIssuesReport($plan, count($pre['issues']));
            fwrite(STDERR, "\n" . $report);
            masterLog($masterLogPath, $report);

            if (!empty($plan['abort'])) {
                stopBeforeIngestion($label, 'data checks (whole-project issue)', $projectPath, $config, $masterLogPath, $confirm);
                $exitCode = EXIT_FAILURE;
                continue;
            }
            if (empty($plan['good'])) {
                stopBeforeIngestion($label, 'data checks (no study without issues)', $projectPath, $config, $masterLogPath, $confirm);
                $exitCode = EXIT_FAILURE;
                continue;
            }

            $question = sprintf(
                "\n  Continue with the %d good study/studies? The %d with issues are NOT created,\n"
                . "  reidentified or ingested in this run; fix them and re-run to add them.",
                count($plan['good']),
                count($plan['exclude_uids'])
            );

            if (!$confirm) {
                fwrite(STDERR, $question . " [dry run - not asked; the preview continues without them]\n");
            } elseif (!confirmPrompt($question, $continueGood)) {
                fwrite(STDERR, "\n  ABORTED - nothing created or ingested for {$label}. Fix the issues listed above\n"
                    . "  and re-run (or answer yes / pass --continue-good to go ahead with the good studies).\n");
                masterLog($masterLogPath, 'ABORTED by operator - data issues');
                stopBeforeIngestion($label, 'data checks (operator chose not to continue)', $projectPath, $config, $masterLogPath, $confirm);
                $exitCode = EXIT_ABORTED;
                continue;
            }

            $exclusionFile = $confirm ? $excludedLive : (tempnam(sys_get_temp_dir(), 'dicom_excl_') ?: $excludedLive);
            @mkdir(dirname($exclusionFile), 0775, true);
            file_put_contents($exclusionFile, json_encode([
                'generated_at' => date('c'),
                'study_uids'   => $plan['exclude_uids'],
                'subjects'     => $plan['exclude_subjects'],
                'issues'       => $plan['bad'],
            ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
            masterLog($masterLogPath, sprintf(
                'continuing with %d good study/studies, %d excluded (%s)',
                count($plan['good']),
                count($plan['exclude_uids']),
                $continueGood ? '--continue-good' : 'operator confirmed'
            ));
        }
    }
    $exArgs = $exclusionFile !== null ? ['--exclude-file=' . $exclusionFile] : [];

    // --- Branch on what the manifest says --------------------------------------
    $manifestPath = $projectPath . '/processed/imaging/dicom_studies.json';
    $counts       = manifestCounts($manifestPath);

    if ($counts['total'] === 0) {
        fwrite(STDERR, "\n  no studies organized - nothing to ingest\n");
        continue;
    }

    $countLine = sprintf(
        '%d study/studies: %d matched to a participant, %d unlinked (no matching participant or session)',
        $counts['total'],
        $counts['linked'],
        $counts['unlinked']
    );

    fwrite(STDERR, "\n  {$countLine}\n");
    masterLog($masterLogPath, $countLine);

    // Unlinked studies (no participant) are never synced, reidentified or
    // ingested - only reported.

    if ($counts['unlinked'] > 0) {
        $report = unlinkedReport($manifestPath, $target, $configPath);
        fwrite(STDERR, "\n" . $report . "\n");
        masterLog($masterLogPath, $report);
    }

    // --- Steps 2 and 3: only meaningful when something is linked ----------------
    if ($counts['linked'] > 0) {
        if (!$stepEnabled('sync')) {
            fwrite(STDERR, "  SKIP sync (disabled)\n");
            masterLog($masterLogPath, 'SKIP sync (disabled)');
            $code = EXIT_OK;
        } else {
            $code = runStep('run_dicom_participant_sync.php', array_merge($common, $exArgs), $masterLogPath);
        }

        if ($code !== EXIT_OK) {
            stopBeforeIngestion($label, 'participant sync', $projectPath, $config, $masterLogPath, $confirm);
            $exitCode = EXIT_FAILURE;
            continue;
        }

        if (!$stepEnabled('reidentify')) {
            fwrite(STDERR, "  SKIP reidentify (disabled)\n");
            masterLog($masterLogPath, 'SKIP reidentify (disabled)');
            $code = EXIT_OK;
        } else {
            $code = runStep('run_dicom_reidentifier.php', array_merge($withForce, $exArgs), $masterLogPath);
        }

        if ($code !== EXIT_OK) {
            stopBeforeIngestion($label, 'reidentification', $projectPath, $config, $masterLogPath, $confirm);
            $exitCode = EXIT_FAILURE;
            continue;
        }
    } else {
        // Nothing resolved to a participant - no participants.tsv, or an
        // unorganized delivery, or no matching rows. Both middle steps have
        // nothing to act on, so they are skipped by name with the reason,
        // rather than one combined line that hides which ran.
        $why = 'no study matches a participant (no participants.tsv, unorganized delivery, '
            . 'or no matching rows)';

        fwrite(STDERR, "  SKIP participant sync - {$why}\n");
        masterLog($masterLogPath, "SKIP participant sync - {$why}");

        // Reidentify still runs: it reports the unlinked studies and makes no
        // LORIS call for them. Nothing unlinked is copied or ingested.
        if (!$stepEnabled('reidentify')) {
            fwrite(STDERR, "  SKIP reidentify (disabled)\n");
            masterLog($masterLogPath, 'SKIP reidentify (disabled)');
            $code = EXIT_OK;
        } else {
            $code = runStep('run_dicom_reidentifier.php', array_merge($withForce, $exArgs), $masterLogPath);
        }

        if ($code !== EXIT_OK) {
            stopBeforeIngestion($label, 'reidentification', $projectPath, $config, $masterLogPath, $confirm);
            $exitCode = EXIT_FAILURE;
            continue;
        }

        fwrite(
            STDERR,
            "       unlinked studies are reported only - nothing is ingested for them\n"
        );
    }

    // --- Reidentification gate, immediately before ingestion ----------------
    // Everything up to here is reversible: delete processed/ and
    // deidentified-lorisid/ and start again. The import is not. So the last
    // thing before it is an explicit statement of what was NOT reidentified.
    if ($noUnlinked && $counts['unlinked'] > 0) {
        fwrite(STDERR, sprintf(
            "\n  STOP %d study/studies have no participant and --no-unlinked was given\n",
            $counts['unlinked']
        ));
        masterLog($masterLogPath, 'STOP no participant, --no-unlinked');
        $exitCode = EXIT_FAILURE;
        continue;
    }

    // --- Step 4: the actual ingestion ----------------------------------------
    // Only reidentified studies are imported, from deidentified-lorisid/.
    // Importing the raw delivery or processed/ would archive the original
    // files with the site identifier still in PatientName.
    if (!$stepEnabled('import')) {
        fwrite(STDERR, "  SKIP import (disabled) - nothing ingested\n");
        masterLog($masterLogPath, 'SKIP import (disabled) - nothing ingested');
        fwrite(STDERR, "\n  {$label} prepared, not ingested\n");
        masterLog($masterLogPath, "RESULT {$label} prepared, not ingested");
        continue;
    }

    // Import from ONE place. The reidentify step copies every matched study
    // into deidentified-lorisid/, reidentified. Importing processed/ as well would archive each
    // study a second time - and the importer's ALREADY_EXISTS auto-retry
    // (--update --overwrite) would then replace the reidentified archive with
    // the site-identifier copy. So processed/ is used only when reidentification
    // has not produced output (this run or an earlier one).
    $sources        = [];
    $lorisidSubdir  = 'deidentified-lorisid/imaging/dicoms';
    $lorisidManifest = $projectPath . '/deidentified-lorisid/imaging/dicom_studies_lorisid.json';

    // Ingest ONLY reidentified output. processed/ holds site identifiers and
    // is never a source: importing it would overwrite reidentified archives.
    if (is_file($lorisidManifest)) {
        // A dry run of an update: organize and reidentify only preview the new
        // or changed studies, so deidentified-lorisid/ still holds what the
        // last real run wrote, and the import below sees only those. Count what
        // is really there and say what --confirm will add.
        $ready = $counts['linked'];
        if (!$confirm) {
            $m     = json_decode((string) @file_get_contents($lorisidManifest), true);
            $ready = count(array_filter(
                (array) ($m['studies'] ?? []),
                static fn ($s): bool => is_array($s)
                    && ($s['link_status'] ?? '') === 'linked'
                    && is_dir((string) ($s['directory'] ?? ''))
            ));
            if ($counts['linked'] > $ready) {
                $more = $counts['linked'] - $ready;
                fwrite(STDERR, sprintf(
                    "\n  import (dry run): %d study/studies already reidentified are checked below; %d more matched\n"
                    . "  study/studies new in this delivery are organized, reidentified and ingested with --confirm\n",
                    $ready,
                    $more
                ));
                masterLog($masterLogPath, "dry run: {$more} new matched study/studies not yet reidentified");
            }
        }
        $sources[$lorisidSubdir] = sprintf('%d reidentified', $ready);
    } elseif ($counts['linked'] > 0 && !$confirm) {
        // A dry run of reidentify writes nothing, so a delivery seen for the
        // first time has no deidentified-lorisid/ yet. That is expected, not a
        // failure: say what --confirm would ingest.
        fwrite(STDERR, sprintf(
            "\n  import (dry run): would ingest %d study/studies matched to a participant, each with its session,\n"
            . "  once reidentify has written deidentified-lorisid/ (it does with --confirm)\n",
            $counts['linked']
        ));
        masterLog($masterLogPath, "dry run: would ingest {$counts['linked']} matched study/studies");
        continue;
    } elseif ($counts['linked'] > 0) {
        stopBeforeIngestion(
            $label,
            'reidentification (no deidentified-lorisid/ output - run it before importing)',
            $projectPath,
            $config,
            $masterLogPath,
            $confirm
        );
        $exitCode = EXIT_FAILURE;
        continue;
    }

    if (empty($sources)) {
        fwrite(STDERR, "\n  nothing to import\n");
        masterLog($masterLogPath, 'nothing to import');
        continue;
    }

    $importFailed = false;

    foreach ($sources as $subdir => $what) {
        fwrite(STDERR, "\n  importing {$what} from {$subdir}\n");
        masterLog($masterLogPath, "import source {$subdir} ({$what})");

        $code = runStep(
            'run_dicom_import.php',
            array_merge(
                $withForce,
                ['--source-subdir=' . $subdir],
                $exArgs
            ),
            $masterLogPath
        );

        if ($code !== EXIT_OK) {
            fwrite(STDERR, "\n  STOP import failed for {$label} ({$subdir})\n");
            masterLog($masterLogPath, "STOP import failed: {$subdir}");
            $exitCode     = EXIT_FAILURE;
            $importFailed = true;
            break;
        }
    }

    if ($importFailed) {
        continue;
    }

    fwrite(STDERR, "\n  {$label} complete\n");
    masterLog($masterLogPath, "RESULT {$label} complete");
}

fwrite(STDERR, "\n");

if (!$confirm) {
    fwrite(STDERR, "\nDry run complete. Nothing was written and nothing ingested.\n");
    fwrite(STDERR, "To execute the chain, run:\n\n");
    fwrite(STDERR, '  ' . confirmCommand($argv) . "\n\n");
}

exit($exitCode);