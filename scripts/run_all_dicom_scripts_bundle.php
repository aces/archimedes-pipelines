<?php

/**
 * run_all_dicom_scripts_bundle.php
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
 * after organising, any study with link_status other than 'linked' would be
 * archived without a participant link. When there are any, the run stops and
 * asks before continuing.
 *
 * Dry run is the default. --confirm executes.
 *
 * Usage:
 *   php scripts/run_all_dicom_scripts_bundle.php --collection=archimedes --project=FDG-PET -v
 *   php scripts/run_all_dicom_scripts_bundle.php --collection=archimedes --project=FDG-PET --confirm
 *   php scripts/run_all_dicom_scripts_bundle.php --all --confirm --yes     # unattended
 *
 * PHP Version 8.1
 *
 * @category Scripts
 * @package  Archimedes
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

const EXIT_OK       = 0;
const EXIT_FAILURE  = 1;
const EXIT_USAGE    = 2;
const EXIT_ABORTED  = 3;

const DEFAULT_CONFIG = __DIR__ . '/../config/loris_client_config.json';

/**
 * Print usage and exit.
 */
function usage(int $code = EXIT_USAGE): void
{
    fwrite(
        $code === EXIT_OK ? STDOUT : STDERR,
        <<<TXT
Usage: php scripts/run_all_dicom_scripts_bundle.php [options]

Runs the full DICOM ingestion chain:
  1. run_dicom_organize.php          eviData audit (gate) + grouping
  2. run_dicom_participant_sync.php  candidates       (skipped without participants.tsv)
  3. run_dicom_reidentifier.php      LORIS IDs        (skipped without participants.tsv)
  4. run_dicom_import.php            archive

Stops at the first failing step. When any study would be archived without a
participant link, stops and asks before running the import.

Selection:
  --all                Process all enabled collections & projects
  --collection=NAME    Process all enabled projects in a collection
  --project=NAME       Process a specific project (requires --collection)

Execution:
  --confirm            Execute
  --dry-run            Report without changing anything (the default)
  --force              Pass --force to each step
  --yes                Answer yes to the unlinked-studies prompt. Required
                       when stdin is not a terminal.
  --no-unlinked        Do not ingest unlinked studies at all; stop instead
  --stop-after=STEP    organize | sync | reidentify  (skip later steps)

Step selection:
  --steps=LIST         Run only these steps, comma separated
                       (organize, sync, reidentify, import)
  --skip=LIST          Run everything except these steps
  --no-organize        Shorthand for --skip=organize
  --no-sync            Shorthand for --skip=sync
  --no-reidentify      Shorthand for --skip=reidentify
  --no-import          Shorthand for --skip=import  (prepare only, no ingestion)
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
 * @param string      $script    Basename in scripts/.
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
function confirmPrompt(string $question, bool $assumeYes): bool
{
    if ($assumeYes) {
        fwrite(STDERR, "{$question} [--yes given]\n");
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
 * @return array{total: int, linked: int, unlinked: int, phantom: int}
 */
function manifestCounts(string $manifestPath): array
{
    $counts = ['total' => 0, 'linked' => 0, 'unlinked' => 0, 'phantom' => 0];

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

$options = getopt(
    'v',
    [
        'all',
        'collection:',
        'project:',
        'confirm',
        'dry-run',
        'force',
        'yes',
        'no-unlinked',
        'stop-after:',
        'steps:',
        'skip:',
        'no-organize',
        'no-sync',
        'no-reidentify',
        'no-import',
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
$assumeYes  = isset($options['yes']);
$noUnlinked = isset($options['no-unlinked']);
$verbose    = isset($options['v']) || isset($options['verbose']);
$configPath = $options['config'] ?? DEFAULT_CONFIG;
$stopAfter  = $options['stop-after'] ?? null;

$allSteps = ['organize', 'sync', 'reidentify', 'import'];

// Step selection: --steps is a whitelist, --skip and the --no-* shorthands
// subtract. A step that is off is reported, not silently absent, so a run
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

foreach (['organize', 'sync', 'reidentify', 'import'] as $step) {
    if (isset($options['no-' . $step])) {
        $skip[] = $step;
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

$validStops = ['organize', 'sync', 'reidentify'];

if ($stopAfter !== null && !in_array($stopAfter, $validStops, true)) {
    fwrite(STDERR, "--stop-after must be one of: " . implode(', ', $validStops) . "\n\n");
    usage();
}

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

    // --- Step 1: eviData audit + organise --------------------------------------
    // The audit gate lives inside the organise runner and aborts there, so a
    // non-zero exit here already means "do not ingest this project".
    if (!$stepEnabled('organize')) {
        fwrite(STDERR, "  SKIP organize (disabled)\n");
        masterLog($masterLogPath, 'SKIP organize (disabled)');
        $code = EXIT_OK;
    } else {
        $code = runStep('run_dicom_organize.php', $withForce, $masterLogPath);
    }

    if ($code !== EXIT_OK) {
        fwrite(STDERR, "\n  STOP organise failed or was gated - skipping {$label}\n");
        masterLog($masterLogPath, "STOP organise failed or was gated");
        $exitCode = EXIT_FAILURE;
        continue;
    }

    if ($stopAfter === 'organize') {
        fwrite(STDERR, "\n  stopped after organise as requested\n");
        continue;
    }

    // --- Branch on what the manifest says --------------------------------------
    $manifestPath = $projectPath . '/processed/imaging/dicom_studies.json';
    $counts       = manifestCounts($manifestPath);

    if ($counts['total'] === 0) {
        fwrite(STDERR, "\n  no studies organised - nothing to ingest\n");
        continue;
    }

    $countLine = sprintf(
        '%d study/studies: %d linked, %d unlinked, %d phantom',
        $counts['total'],
        $counts['linked'],
        $counts['unlinked'],
        $counts['phantom']
    );

    fwrite(STDERR, "\n  {$countLine}\n");
    masterLog($masterLogPath, $countLine);

    $unlinkedTotal = $counts['unlinked'] + $counts['phantom'];

    // --- Steps 2 and 3: only meaningful when something is linked ----------------
    if ($counts['linked'] > 0) {
        if (!$stepEnabled('sync')) {
            fwrite(STDERR, "  SKIP sync (disabled)\n");
            masterLog($masterLogPath, 'SKIP sync (disabled)');
            $code = EXIT_OK;
        } else {
            $code = runStep('run_dicom_participant_sync.php', $common, $masterLogPath);
        }

        if ($code !== EXIT_OK) {
            fwrite(STDERR, "\n  STOP participant sync failed - skipping {$label}\n");
            masterLog($masterLogPath, "STOP participant sync failed");
            $exitCode = EXIT_FAILURE;
            continue;
        }

        if ($stopAfter === 'sync') {
            fwrite(STDERR, "\n  stopped after sync as requested\n");
            continue;
        }

        if (!$stepEnabled('reidentify')) {
            fwrite(STDERR, "  SKIP reidentify (disabled)\n");
            masterLog($masterLogPath, 'SKIP reidentify (disabled)');
            $code = EXIT_OK;
        } else {
            $code = runStep('run_dicom_reidentifier.php', $common, $masterLogPath);
        }

        if ($code !== EXIT_OK) {
            fwrite(STDERR, "\n  STOP reidentifier failed - skipping {$label}\n");
            masterLog($masterLogPath, "STOP reidentifier failed");
            $exitCode = EXIT_FAILURE;
            continue;
        }
    } else {
        // Nothing resolved to a participant - no participants.tsv, or an
        // unorganised delivery, or no matching rows. Both middle steps have
        // nothing to act on, so they are skipped by name with the reason,
        // rather than one combined line that hides which ran.
        $why = $counts['total'] === $counts['phantom']
            ? 'all studies are phantoms'
            : 'no linked studies (no participants.tsv, unorganised delivery, '
            . 'or no matching rows)';

        foreach (['participant sync', 'reidentifier'] as $step) {
            fwrite(STDERR, "  SKIP {$step} - {$why}\n");
            masterLog($masterLogPath, "SKIP {$step} - {$why}");
        }

        fwrite(
            STDERR,
            "       studies will be archived with no participant link\n"
        );
    }

    if ($stopAfter === 'reidentify') {
        fwrite(STDERR, "\n  stopped after reidentify as requested\n");
        continue;
    }

    // --- Reidentification gate, immediately before ingestion ----------------
    // Everything up to here is reversible: delete processed/ and
    // deidentified-lorisid/ and start again. The import is not. So the last
    // thing before it is an explicit statement of what was NOT reidentified.
    if ($unlinkedTotal > 0) {
        if ($noUnlinked) {
            fwrite(STDERR, sprintf(
                "\n  STOP %d study/studies were not reidentified and "
                . "--no-unlinked was given\n",
                $unlinkedTotal
            ));
            masterLog($masterLogPath, 'STOP not reidentified, --no-unlinked');
            $exitCode = EXIT_FAILURE;
            continue;
        }

        $question = sprintf(
            "\n  NOT REIDENTIFIED: %d of %d study/studies\n"
            . "    %d unlinked - no LORIS ID could be resolved\n"
            . "    %d phantom  - no LORIS ID applies\n"
            . "\n"
            . "  Their DICOM headers are unchanged from the delivery: PatientName\n"
            . "  still holds the site identifier, and they will archive with a\n"
            . "  NULL SessionID, under no candidate.\n"
            . "\n"
            . "  Ingest them anyway, with no linked LORIS ID?",
            $unlinkedTotal,
            $counts['total'],
            $counts['unlinked'],
            $counts['phantom']
        );

        if (!$confirm) {
            fwrite(STDERR, $question . " [dry run - not asked]\n");
            masterLog($masterLogPath, 'reidentification prompt not asked (dry run)');
        } elseif (!confirmPrompt($question, $assumeYes)) {
            fwrite(
                STDERR,
                "\n  ABORTED - nothing ingested for {$label}. Add participants.tsv\n"
                . "  rows for the unresolved subjects, re-run the chain, and the\n"
                . "  studies will be reidentified instead.\n"
            );
            masterLog($masterLogPath, 'ABORTED by operator - not reidentified');
            $exitCode = EXIT_ABORTED;
            continue;
        } else {
            masterLog($masterLogPath, sprintf(
                'ingesting %d non-reidentified study/studies (%s)',
                $unlinkedTotal,
                $assumeYes ? '--yes' : 'operator confirmed'
            ));
        }
    }

    // --- Step 4: the actual ingestion ----------------------------------------
    // Relabelled studies live in deidentified-lorisid/, unlinked ones stay in
    // processed/. Importing from the raw delivery would archive the original
    // files with the site identifier still in PatientName, silently discarding
    // the relabelling - so the source is chosen per group, and the importer is
    // invoked once for each that has studies.
    if (!$stepEnabled('import')) {
        fwrite(STDERR, "  SKIP import (disabled) - nothing ingested\n");
        masterLog($masterLogPath, 'SKIP import (disabled) - nothing ingested');
        fwrite(STDERR, "\n  {$label} prepared, not ingested\n");
        masterLog($masterLogPath, "RESULT {$label} prepared, not ingested");
        continue;
    }

    $sources = [];

    if ($counts['linked'] > 0 && $stepEnabled('reidentify')) {
        $sources['deidentified-lorisid/imaging/dicoms'] = sprintf(
            '%d relabelled',
            $counts['linked']
        );
    } elseif ($counts['linked'] > 0) {
        // Relabelling was skipped, so linked studies were never copied to
        // deidentified-lorisid/ - they are still in processed/.
        $sources['processed/imaging/dicoms'] = sprintf(
            '%d linked (relabel skipped)',
            $counts['linked']
        );
    }

    if ($unlinkedTotal > 0) {
        $key = 'processed/imaging/dicoms';
        $sources[$key] = isset($sources[$key])
            ? $sources[$key] . sprintf(' + %d unlinked/phantom', $unlinkedTotal)
            : sprintf('%d unlinked/phantom', $unlinkedTotal);
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
            array_merge($withForce, ['--source-subdir=' . $subdir]),
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