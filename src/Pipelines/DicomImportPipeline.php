<?php
declare(strict_types=1);

/**
 * ARCHIMEDES DICOM Import Pipeline
 *
 * Scans <projectDir>/deidentified-raw/imaging/dicoms/ for study directories
 * and imports each via POST /cbigr_api/script/importdicomstudy endpoint.
 *
 * Execution model:
 *   - Imports fired as async=true → SPM forks script → returns job_id immediately
 *   - Pipeline polls GET /cbigr_api/script/job/{id} every 30s
 *   - On ERROR the full Python/Perl stdout+stderr is returned (not a cURL timeout)
 *
 * Flag selection:
 *   - Normal run          → --insert (script errors if already in DB → auto-retry with --update --overwrite)
 *   - --force / --retry   → --update --overwrite (known to be in DB from prior run)
 *   - --force-study=NAME  → --update --overwrite (explicit safe reinsertion)
 *
 * @package LORIS\Pipelines
 */

namespace LORIS\Pipelines;

use GuzzleHttp\Client as HttpClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use LORIS\Utils\{Notification, CleanLogFormatter};

class DicomImportPipeline
{
    private array $config;
    private Logger $logger;
    private HttpClient $http;
    private Notification $notification;
    private bool $dryRun;
    private bool $verbose;
    private string $token = '';

    private string $runTimestamp;

    /** @var resource|null */
    private $errorFh = null;
    private ?string $errorLogPath = null;

    /** @var resource|null */
    private $runLogFh = null;
    private ?string $runLogPath = null;

    private ?string $logDir = null;

    private const TRACK_FILE = '.dicom_import_processed.json';

    /** Seconds between SPM job status poll requests */
    private const POLL_INTERVAL_SECONDS = 30;

    /** Maximum seconds to wait for a single study before declaring timeout */
    private const POLL_TIMEOUT_SECONDS = 14400; // 4 hours

    /**
     * Python script error message indicating the study is already in the DB.
     * Matched against computeExitText() output from ScriptServerProcess.
     */
    private const ALREADY_INSERTED_PATTERN = '/already inserted in LORIS/i';

    private array $importResults = [];

    private array $stats = [
        'studies_found'         => 0,
        'studies_processed'     => 0,
        'studies_skipped'       => 0,
        'studies_already_exist' => 0,
        'studies_failed'        => 0,
    ];

    // ──────────────────────────────────────────────────────────────────
    // Constructor
    // ──────────────────────────────────────────────────────────────────

    public function __construct(array $config, bool $dryRun = false, bool $verbose = false)
    {
        $this->config       = $config;
        $this->dryRun       = $dryRun;
        $this->verbose      = $verbose;
        $this->runTimestamp = date('Y-m-d_H-i-s');

        $logLevel = $verbose ? Logger::DEBUG : Logger::INFO;
        $this->logger = new Logger('dicom_import');
        $formatter    = new CleanLogFormatter();

        $console = new StreamHandler('php://stdout', $logLevel);
        $console->setFormatter($formatter);
        $this->logger->pushHandler($console);

        $this->http = new HttpClient([
            'base_uri'    => rtrim($config['api']['base_url'] ?? '', '/') . '/',
            'timeout'     => 30,        // handshake only — scripts run async via SPM
            'verify'      => $config['verify_ssl'] ?? true,
            'http_errors' => false,
        ]);

        $this->notification = new Notification();
    }

    // ──────────────────────────────────────────────────────────────────
    // Main entry
    // ──────────────────────────────────────────────────────────────────

    /**
     * @param string   $projectDir    Full path to project directory
     * @param bool     $force         Reprocess ALL studies (use --update --overwrite)
     * @param bool     $retryFailed   Auto-retry previously failed studies (default true)
     * @param array    $flags         Base script flags (insert/update/overwrite/session/verbose)
     * @param string   $profile       Python config profile name
     * @param string[] $forceStudies  Specific study names to force-reinsert with
     *                                --update --overwrite regardless of tracking status
     */
    public function run(
        string $projectDir,
        bool   $force        = false,
        bool   $retryFailed  = true,
        array  $flags        = ['insert', 'verbose'],
        string $profile      = 'database_config.py',
        array  $forceStudies = []
    ): array {
        $this->importResults = [];
        $this->stats = [
            'studies_found'         => 0,
            'studies_processed'     => 0,
            'studies_skipped'       => 0,
            'studies_already_exist' => 0,
            'studies_failed'        => 0,
        ];

        $this->logDir = rtrim($projectDir, '/') . '/logs/dicom';
        $this->openRunLog();

        $this->log("=== DICOM IMPORT PIPELINE ===");
        $this->log("Project: {$projectDir}");
        $this->log("Run: {$this->runTimestamp}");
        if ($this->dryRun) {
            $this->log("MODE: DRY RUN");
        }
        $this->log("Flags: " . implode(', ', $flags));
        $this->log("Profile: {$profile}");
        if ($force) {
            $this->log("Mode: FORCE — all studies reprocessed with --update --overwrite");
        } elseif ($retryFailed) {
            $this->log("Mode: RETRY FAILED — auto-retrying previously failed studies");
        } else {
            $this->log("Mode: SKIP FAILED — skipping previously failed studies");
        }
        if (!empty($forceStudies)) {
            $this->log("Force-study targets: " . implode(', ', $forceStudies));
        }
        $this->log("========================================");

        try {
            if (!$this->dryRun) {
                if (!$this->authenticate()) {
                    $this->writeError("AUTH", "Authentication failed — aborting.");
                    $this->sendNotification($projectDir);
                    $this->closeAllLogs();
                    return $this->stats;
                }
            }

            $this->processProject(
                $projectDir, $force, $retryFailed, $flags, $profile, $forceStudies
            );
            $this->writeProjectSummary(basename($projectDir));
            $this->sendNotification($projectDir);
            $this->closeAllLogs();

            return $this->stats;

        } catch (\Exception $e) {
            $this->writeError("FATAL", $e->getMessage());
            $this->logger->debug($e->getTraceAsString());
            $this->sendNotification($projectDir);
            $this->closeAllLogs();
            return $this->stats;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Process project
    // ──────────────────────────────────────────────────────────────────

    private function processProject(
        string $projectDir,
        bool   $force,
        bool   $retryFailed,
        array  $flags,
        string $profile,
        array  $forceStudies
    ): void {
        $this->log("");
        $this->log("──── STEP 1: SCAN DICOM DIRECTORIES ────");

        $studies = $this->findStudyDirectories($projectDir);
        $this->stats['studies_found'] = count($studies);

        if (empty($studies)) {
            $this->log("  No DICOM study directories found.");
            return;
        }

        $this->log("  Found {$this->stats['studies_found']} stud"
            . ($this->stats['studies_found'] === 1 ? 'y' : 'ies'));
        $this->log("");

        $dicomRoot = rtrim($projectDir, '/') . '/deidentified-raw/imaging/dicoms';
        $processed = $this->loadProcessed($dicomRoot);

        $this->log("──── STEP 2: IMPORT STUDIES ────");
        $this->log("");

        foreach ($studies as $studyPath) {
            $name       = basename($studyPath);
            $isForced   = in_array($name, $forceStudies, true);
            $prev       = $processed[$name] ?? null;
            $prevStatus = $prev['status'] ?? null;

            // ── Skip / retry logic ─────────────────────────────────────────────
            if (!$force && !$isForced && $prev !== null) {
                if ($prevStatus === 'failed' && $retryFailed) {
                    $this->log("  [{$name}] RETRYING — previously failed"
                        . " ({$prev['timestamp']}): "
                        . $this->_truncateForLog($prev['detail'] ?? 'unknown'));
                    // fall through to import
                } else {
                    $this->log("  [{$name}] SKIPPED — {$prevStatus} ({$prev['timestamp']})");
                    $this->importResults[$name] = [
                        'status' => 'skipped',
                        'reason' => "already {$prevStatus}",
                    ];
                    if ($prevStatus === 'already_exists') {
                        $this->stats['studies_already_exist']++;
                    } else {
                        $this->stats['studies_skipped']++;
                    }
                    continue;
                }
            }

            $this->log("  [{$name}]");

            if ($this->dryRun) {
                $effectiveFlags = $this->_resolveFlags($flags, $force, $isForced, $prevStatus);
                $this->log("    DRY RUN — would POST importdicomstudy (async via SPM)");
                $this->log("    source={$studyPath}");
                $this->log("    flags: " . implode(', ', $effectiveFlags));
                $this->importResults[$name] = ['status' => 'dry_run'];
                $this->stats['studies_processed']++;
                continue;
            }

            // ── Resolve flags ───────────────────────────────────────────────────
            // --force / --force-study on a study with a prior tracking entry means
            // we know it reached the DB (or partially did). Use --update --overwrite
            // to safely overwrite without creating a duplicate tarchive row.
            // Normal first-run studies use --insert; if the script replies "already
            // inserted", _importOneStudy() automatically retries with --update --overwrite.
            $effectiveFlags = $this->_resolveFlags($flags, $force, $isForced, $prevStatus);

            $this->importOneStudy(
                $name, $studyPath, $dicomRoot, $processed, $effectiveFlags, $profile
            );
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Flag resolution
    // ──────────────────────────────────────────────────────────────────

    /**
     * Resolve the effective flags for a single study.
     *
     * Rule:
     *   - force=true OR isForced=true AND prevStatus !== null
     *     → --update --overwrite  (known to be in DB, safe overwrite)
     *   - everything else
     *     → caller-supplied flags (normally --insert)
     *     If the script returns ALREADY_EXISTS, _importOneStudy() will
     *     automatically retry with --update --overwrite (two-pass fallback).
     *
     * @param array   $baseFlags   Flags from CLI / run() caller
     * @param bool    $force       Global --force flag
     * @param bool    $isForced    Study is in --force-study list
     * @param ?string $prevStatus  Previous tracking status or null
     *
     * @return array Resolved flags
     */
    private function _resolveFlags(
        array   $baseFlags,
        bool    $force,
        bool    $isForced,
        ?string $prevStatus
    ): array {
        $useOverwrite = $force || ($isForced && $prevStatus !== null);

        if ($useOverwrite) {
            // Strip insert, ensure update + overwrite
            $flags = array_filter($baseFlags, fn($f) => $f !== 'insert');
            if (!in_array('update', $flags, true)) {
                $flags[] = 'update';
            }
            if (!in_array('overwrite', $flags, true)) {
                $flags[] = 'overwrite';
            }
            return array_values($flags);
        }

        return $baseFlags;
    }

    /**
     * Build flags for automatic retry after ALREADY_EXISTS error.
     * Switches insert → update + overwrite.
     *
     * @param array $flags Current flags
     *
     * @return array
     */
    private function _buildRetryFlags(array $flags): array
    {
        $flags = array_filter($flags, fn($f) => $f !== 'insert');
        if (!in_array('update', $flags, true)) {
            $flags[] = 'update';
        }
        if (!in_array('overwrite', $flags, true)) {
            $flags[] = 'overwrite';
        }
        return array_values($flags);
    }

    // ──────────────────────────────────────────────────────────────────
    // Import one study — fire async, poll, auto-retry on ALREADY_EXISTS
    // ──────────────────────────────────────────────────────────────────

    private function importOneStudy(
        string  $name,
        string  $studyPath,
        string  $dicomRoot,
        array  &$processed,
        array   $flags,
        string  $profile,
        bool    $isRetry = false  // true when auto-retrying after ALREADY_EXISTS
    ): void {
        try {
            $t0     = microtime(true);
            $launch = $this->_launchAsync($studyPath, $flags, $profile);

            if (($launch['http_status'] ?? 0) !== 202) {
                $elapsed = round(microtime(true) - $t0, 2);
                $errMsg  = $this->extractErrorMessage($launch);
                $this->log("    FAILED to launch ({$elapsed}s): {$errMsg}");
                $this->writeError($name, "Launch failed: {$errMsg}");
                $this->writeErrorDetail("HTTP status: " . ($launch['http_status'] ?? 0));
                $this->markProcessed($dicomRoot, $processed, $name, 'failed', $errMsg);
                $this->importResults[$name] = ['status' => 'failed', 'reason' => $errMsg, 'elapsed' => $elapsed];
                $this->stats['studies_failed']++;
                return;
            }

            $jobId = $launch['job_id'] ?? null;
            $pid   = $launch['pid']    ?? null;
            if (!$jobId) {
                $elapsed = round(microtime(true) - $t0, 2);
                $errMsg  = "No job_id returned from async launch";
                $this->log("    FAILED ({$elapsed}s): {$errMsg}");
                $this->writeError($name, $errMsg);
                $this->markProcessed($dicomRoot, $processed, $name, 'failed', $errMsg);
                $this->importResults[$name] = ['status' => 'failed', 'reason' => $errMsg, 'elapsed' => $elapsed];
                $this->stats['studies_failed']++;
                return;
            }

            $retryLabel = $isRetry ? ' [retry with --update --overwrite]' : '';
            $this->log("    Launched — job_id={$jobId} pid={$pid}{$retryLabel}, polling every "
                . self::POLL_INTERVAL_SECONDS . "s ...");

            // ── Poll ───────────────────────────────────────────────────────────
            $pollStart        = microtime(true);
            $pollFailures     = 0;
            $wasRunning       = false;

            while (true) {
                sleep(self::POLL_INTERVAL_SECONDS);

                $elapsed = round(microtime(true) - $t0, 2);

                if ((microtime(true) - $pollStart) > self::POLL_TIMEOUT_SECONDS) {
                    $errMsg = "Job {$jobId} timed out after "
                        . self::POLL_TIMEOUT_SECONDS . "s — "
                        . "check server_processes id={$jobId}";
                    $this->log("    TIMEOUT ({$elapsed}s)");
                    $this->writeError($name, $errMsg);
                    $this->markProcessed($dicomRoot, $processed, $name, 'failed', $errMsg);
                    $this->importResults[$name] = [
                        'status'  => 'failed',
                        'reason'  => $errMsg,
                        'elapsed' => $elapsed,
                        'job_id'  => $jobId,
                    ];
                    $this->stats['studies_failed']++;
                    return;
                }

                $status = $this->_pollJob($jobId);

                if ($status === null) {
                    $pollFailures++;
                    $this->log("    WARNING: poll failed for job {$jobId} at {$elapsed}s"
                        . " — attempt {$pollFailures}/3");
                    if ($pollFailures >= 3) {
                        $errMsg = "Job {$jobId} poll failed 3 consecutive times"
                            . " — check GET cbigr_api/script/job/{$jobId}";
                        $this->log("    FAILED ({$elapsed}s): {$errMsg}");
                        $this->writeError($name, $errMsg);
                        $this->markProcessed($dicomRoot, $processed, $name, 'failed', $errMsg);
                        $this->importResults[$name] = [
                            'status'  => 'failed',
                            'reason'  => $errMsg,
                            'elapsed' => $elapsed,
                            'job_id'  => $jobId,
                        ];
                        $this->stats['studies_failed']++;
                        return;
                    }
                    continue;
                }

                $pollFailures = 0;
                $state        = $status['state'] ?? 'UNKNOWN';
                $this->log("    [{$elapsed}s] job {$jobId} → {$state}");

                if ($state === 'RUNNING') {
                    $wasRunning = true;
                    continue;
                }

                // UNKNOWN means SPM couldn't read the exit code file — either it was
                // deleted before the monitor synced (exit 0, deleteProcessFiles ran)
                // or the process died unexpectedly.
                // If we saw RUNNING previously, the process completed and the file was
                // cleaned up after a successful exit — treat as SUCCESS.
                if ($state === 'UNKNOWN') {
                    if ($wasRunning) {
                        $this->log("    SUCCESS (exit code file cleaned up after successful run)"
                            . " ({$elapsed}s)");
                        $this->markProcessed($dicomRoot, $processed, $name, 'success');
                        $this->importResults[$name] = [
                            'status'  => 'success',
                            'elapsed' => $elapsed,
                            'job_id'  => $jobId,
                        ];
                        $this->stats['studies_processed']++;
                        return;
                    }
                    // Never saw RUNNING — process likely died immediately
                    $errMsg = "Job {$jobId} ended in UNKNOWN state without running"
                        . " — check server_processes id={$jobId}";
                    $this->log("    FAILED ({$elapsed}s): {$errMsg}");
                    $this->writeError($name, $errMsg);
                    $this->markProcessed($dicomRoot, $processed, $name, 'failed', $errMsg);
                    $this->importResults[$name] = [
                        'status'  => 'failed',
                        'reason'  => $errMsg,
                        'elapsed' => $elapsed,
                        'job_id'  => $jobId,
                    ];
                    $this->stats['studies_failed']++;
                    return;
                }

                // ── Job finished ───────────────────────────────────────────────
                $elapsed = round(microtime(true) - $t0, 2);

                if ($state === 'SUCCESS') {
                    $this->log("    SUCCESS ({$elapsed}s)");
                    $this->markProcessed($dicomRoot, $processed, $name, 'success');
                    $this->importResults[$name] = [
                        'status'  => 'success',
                        'elapsed' => $elapsed,
                        'job_id'  => $jobId,
                    ];
                    $this->stats['studies_processed']++;
                    return;
                }

                // ── ERROR — read actual script output from SPM ─────────────────
                $errorDetail = $status['error_detail'] ?? $status['progress'] ?? 'Unknown error';
                $exitCode    = $status['exit_code'] ?? '?';

                // ALREADY_EXISTS: study is in DB but we tried --insert.
                // Auto-retry once with --update --overwrite.
                if (!$isRetry && preg_match(self::ALREADY_INSERTED_PATTERN, $errorDetail)) {
                    $this->log("    Already inserted — retrying with --update --overwrite ({$elapsed}s)");
                    $retryFlags = $this->_buildRetryFlags($flags);
                    $this->importOneStudy(
                        $name, $studyPath, $dicomRoot, $processed, $retryFlags, $profile, true
                    );
                    return;
                }

                // On retry ALREADY_EXISTS means a genuine conflict — mark as already_exists
                if ($isRetry && preg_match(self::ALREADY_INSERTED_PATTERN, $errorDetail)) {
                    $this->log("    Already inserted (confirmed) — marking done ({$elapsed}s)");
                    $this->markProcessed($dicomRoot, $processed, $name, 'already_exists');
                    $this->importResults[$name] = [
                        'status'  => 'already_exists',
                        'elapsed' => $elapsed,
                        'job_id'  => $jobId,
                    ];
                    $this->stats['studies_already_exist']++;
                    return;
                }

                // Real failure — the full Python/Perl error is in errorDetail
                $this->log("    FAILED ({$elapsed}s) exit_code={$exitCode}: "
                    . $this->_truncateForLog($errorDetail));
                $this->writeError($name, "Script failed (exit {$exitCode}): "
                    . $this->_truncateForLog($errorDetail, 300));
                $this->writeErrorDetail("job_id: {$jobId}");
                $this->writeErrorDetail("Full error:\n{$errorDetail}");
                $this->markProcessed($dicomRoot, $processed, $name, 'failed', $errorDetail);
                $this->importResults[$name] = [
                    'status'    => 'failed',
                    'reason'    => $errorDetail,
                    'elapsed'   => $elapsed,
                    'job_id'    => $jobId,
                    'exit_code' => $exitCode,
                ];
                $this->stats['studies_failed']++;
                return;
            }

        } catch (\Exception $e) {
            $this->log("    EXCEPTION: " . $e->getMessage());
            $this->writeError($name, "Import exception: " . $e->getMessage());
            $this->importResults[$name] = ['status' => 'failed', 'reason' => $e->getMessage()];
            $this->stats['studies_failed']++;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Auth
    // ──────────────────────────────────────────────────────────────────

    private function authenticate(): bool
    {
        $apiVersion = $this->config['api']['api_version']
            ?? $this->config['api_version']
            ?? 'v0.0.4-dev';
        $username = $this->config['api']['username'] ?? '';
        $password = $this->config['api']['password'] ?? '';

        $this->log("Authenticating ({$apiVersion})...");

        try {
            $resp = $this->http->post("api/{$apiVersion}/login", [
                'json' => compact('username', 'password'),
            ]);

            if ($resp->getStatusCode() !== 200) {
                $this->log("  Auth failed: HTTP " . $resp->getStatusCode());
                return false;
            }

            $body        = json_decode($resp->getBody()->getContents(), true);
            $this->token = $body['token'] ?? '';

            if (empty($this->token)) {
                $this->log("  Auth failed: no token returned");
                return false;
            }

            $this->log("  Authenticated");
            return true;
        } catch (\Exception $e) {
            $this->log("  Auth exception: " . $e->getMessage());
            return false;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // HTTP helpers
    // ──────────────────────────────────────────────────────────────────

    /**
     * POST importdicomstudy with async=true.
     * Returns immediately with { job_id, poll_url }.
     */
    private function _launchAsync(
        string $sourceDir,
        array  $flags   = ['insert'],
        string $profile = 'database_config.py'
    ): array {
        $endpoint = 'cbigr_api/script/importdicomstudy';

        $payload = [
            'args'  => ['source' => $sourceDir, 'profile' => $profile],
            'flags' => $flags,
            'async' => true,
        ];

        $this->logger->debug("  POST {$endpoint} async=true flags=" . implode(',', $flags));

        try {
            $resp = $this->http->post($endpoint, [
                'json'    => $payload,
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
                'timeout' => 30,
            ]);
            $body = json_decode($resp->getBody()->getContents(), true) ?? [];
            $body['http_status'] = $resp->getStatusCode();
            return $body;
        } catch (\Exception $e) {
            return [
                'status'      => 'error',
                'http_status' => 0,
                'errors'      => [['type' => 'exception', 'message' => $e->getMessage()]],
            ];
        }
    }

    /**
     * GET /cbigr_api/script/job/{id}.
     * Returns null on transient failure — caller retries next poll cycle.
     */
    private function _pollJob(int $jobId): ?array
    {
        try {
            $resp = $this->http->get("cbigr_api/script/job/{$jobId}", [
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
                'timeout' => 15,
            ]);
            $body = json_decode($resp->getBody()->getContents(), true);
            return is_array($body) ? $body : null;
        } catch (\Exception $e) {
            $this->logger->debug("  poll exception job {$jobId}: " . $e->getMessage());
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Tracking file
    // ──────────────────────────────────────────────────────────────────

    private function loadProcessed(string $dicomRoot): array
    {
        $path = rtrim($dicomRoot, '/') . '/' . self::TRACK_FILE;
        if (!file_exists($path)) {
            return [];
        }
        return json_decode(file_get_contents($path), true) ?? [];
    }

    private function saveProcessed(string $dicomRoot, array $processed): void
    {
        $path = rtrim($dicomRoot, '/') . '/' . self::TRACK_FILE;
        $json = json_encode($processed, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        if (@file_put_contents($path, $json) === false) {
            $this->writeError('TRACKING', "Cannot write: {$path}");
        }
    }

    private function markProcessed(
        string  $dicomRoot,
        array  &$processed,
        string  $studyName,
        string  $status,
        string  $detail = ''
    ): void {
        $processed[$studyName] = [
            'status'    => $status,
            'detail'    => $detail,
            'timestamp' => date('c'),
        ];
        $this->saveProcessed($dicomRoot, $processed);
    }

    // ──────────────────────────────────────────────────────────────────
    // Find study dirs
    // ──────────────────────────────────────────────────────────────────

    private function findStudyDirectories(string $projectDir): array
    {
        $dicomRoot = rtrim($projectDir, '/') . '/deidentified-raw/imaging/dicoms';

        if (!is_dir($dicomRoot)) {
            $this->log("  Directory not found: {$dicomRoot}");
            return [];
        }

        $studies = [];
        foreach (scandir($dicomRoot) as $entry) {
            if ($entry[0] === '.') {
                continue;
            }
            $full = "{$dicomRoot}/{$entry}";
            if (is_dir($full)) {
                $studies[] = $full;
            }
        }
        sort($studies);
        return $studies;
    }

    // ──────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────

    private function _truncateForLog(string $text, int $limit = 120): string
    {
        $text = preg_replace('/\s+/', ' ', trim($text));
        if (strlen($text) <= $limit) {
            return $text;
        }
        return substr($text, 0, $limit - 3) . '...';
    }

    private function extractErrorMessage(array $result): string
    {
        $errors = $result['errors'] ?? [];
        if (!empty($errors)) {
            $msgs = array_map(fn($e) => $e['message'] ?? $e['type'] ?? 'unknown', $errors);
            return implode('; ', array_slice($msgs, 0, 3));
        }
        if (!empty($result['output'])) {
            $lines = array_filter(explode("\n", trim($result['output'])));
            return end($lines) ?: 'Unknown error';
        }
        return $result['error_type']
            ?? 'Unknown error (HTTP ' . ($result['http_status'] ?? '?') . ')';
    }

    // ══════════════════════════════════════════════════════════════════
    //  RUN LOG
    // ══════════════════════════════════════════════════════════════════

    private function openRunLog(): void
    {
        if ($this->runLogFh !== null || $this->logDir === null) {
            return;
        }
        if (!is_dir($this->logDir)) {
            @mkdir($this->logDir, 0755, true);
        }
        $this->runLogPath = "{$this->logDir}/dicom_run_{$this->runTimestamp}.log";
        $this->runLogFh   = @fopen($this->runLogPath, 'a');
        if ($this->runLogFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->runLogFh,
                "{$sep}\n ARCHIMEDES DICOM Import Pipeline — Run Log\n"
                . " Started: " . date('Y-m-d H:i:s T') . "\n"
                . ($this->dryRun ? " Mode: DRY RUN\n" : "")
                . "{$sep}\n\n"
            );
        }
    }

    private function log(string $msg): void
    {
        $this->logger->info($msg);
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[" . date('H:i:s') . "] {$msg}\n");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  ERROR LOG
    // ══════════════════════════════════════════════════════════════════

    private function writeError(string $context, string $msg): void
    {
        $this->logger->error("[{$context}] {$msg}");

        if ($this->errorFh === null && $this->logDir !== null) {
            if (!is_dir($this->logDir)) {
                @mkdir($this->logDir, 0755, true);
            }
            $this->errorLogPath = "{$this->logDir}/dicom_errors_{$this->runTimestamp}.log";
            $this->errorFh      = @fopen($this->errorLogPath, 'a');
            if ($this->errorFh) {
                $sep = str_repeat('=', 72);
                fwrite($this->errorFh,
                    "{$sep}\n ARCHIMEDES DICOM Import Pipeline — Error Log\n"
                    . " Run: {$this->runTimestamp}\n{$sep}\n\n"
                );
            }
        }

        $ts = date('H:i:s');
        if ($this->errorFh) {
            fwrite($this->errorFh, "[{$ts}] [{$context}] {$msg}\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[{$ts}] ERROR [{$context}] {$msg}\n");
        }
    }

    private function writeErrorDetail(string $text): void
    {
        if ($this->errorFh) {
            fwrite($this->errorFh, "  {$text}\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "  ERROR-DETAIL: {$text}\n");
        }
    }

    private function closeAllLogs(): void
    {
        $sep = str_repeat('=', 72);
        $ts  = date('Y-m-d H:i:s T');
        if ($this->errorFh) {
            fwrite($this->errorFh, "\n{$sep}\n Closed: {$ts}\n{$sep}\n");
            fclose($this->errorFh);
            $this->errorFh = null;
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "\n{$sep}\n Completed: {$ts}\n{$sep}\n");
            fclose($this->runLogFh);
            $this->runLogFh = null;
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  PROJECT SUMMARY
    // ══════════════════════════════════════════════════════════════════

    private function writeProjectSummary(string $projectName): void
    {
        $this->log("");
        $this->log("──── PROJECT SUMMARY: {$projectName} ────");
        $this->log("  DICOM STUDY IMPORT:");

        if (empty($this->importResults)) {
            $this->log("    (no studies found)");
        } else {
            $success = $failed = $skipped = $alreadyExist = $dryRun = [];
            foreach ($this->importResults as $study => $r) {
                switch ($r['status']) {
                    case 'success':        $success[]      = $study; break;
                    case 'failed':         $failed[]       = $study; break;
                    case 'skipped':        $skipped[]      = $study; break;
                    case 'already_exists': $alreadyExist[] = $study; break;
                    case 'dry_run':        $dryRun[]       = $study; break;
                }
            }

            if (!empty($success)) {
                $this->log("    Imported (" . count($success) . "):");
                foreach ($success as $s) {
                    $e = $this->importResults[$s]['elapsed'] ?? '';
                    $j = $this->importResults[$s]['job_id'] ?? null;
                    $this->log("      + {$s}" . ($e ? " ({$e}s)" : "") . ($j ? " [job {$j}]" : ""));
                }
            }
            if (!empty($alreadyExist)) {
                $this->log("    Already imported (" . count($alreadyExist) . "):");
                foreach ($alreadyExist as $s) {
                    $this->log("      = {$s}");
                }
            }
            if (!empty($skipped)) {
                $this->log("    Skipped (" . count($skipped) . "):");
                foreach ($skipped as $s) {
                    $this->log("      - {$s} — " . ($this->importResults[$s]['reason'] ?? '?'));
                }
            }
            if (!empty($failed)) {
                $this->log("    Failed (" . count($failed) . "):");
                foreach ($failed as $s) {
                    $j = $this->importResults[$s]['job_id'] ?? null;
                    $this->log("      ! {$s} — "
                        . $this->_truncateForLog($this->importResults[$s]['reason'] ?? '?')
                        . ($j ? " [job {$j}]" : ""));
                }
            }
            if (!empty($dryRun)) {
                $this->log("    Dry run (" . count($dryRun) . "):");
                foreach ($dryRun as $s) {
                    $this->log("      ~ {$s}");
                }
            }
        }

        $s = $this->stats;
        $this->log("");
        $this->log("========================================");
        $this->log("PIPELINE RUN SUMMARY");
        $this->log("========================================");
        $this->log("  Run: {$this->runTimestamp}");
        $this->log("  Studies found:          {$s['studies_found']}");
        $this->log("  Successfully imported:  {$s['studies_processed']}");
        $this->log("  Already existed:        {$s['studies_already_exist']}");
        $this->log("  Skipped:                {$s['studies_skipped']}");
        $this->log("  Failed:                 {$s['studies_failed']}");
        if ($this->runLogPath)   $this->log("  Run log:   {$this->runLogPath}");
        if ($this->errorLogPath) $this->log("  Error log: {$this->errorLogPath}");
        $this->log("  Result: " . ($s['studies_failed'] > 0 ? 'COMPLETED WITH ERRORS' : 'COMPLETED SUCCESSFULLY'));
        $this->log("========================================");
    }

    // ══════════════════════════════════════════════════════════════════
    //  EMAIL NOTIFICATION
    // ══════════════════════════════════════════════════════════════════

    private function sendNotification(string $projectDir): void
    {
        if ($this->dryRun) {
            $this->log("  Notification skipped (dry run)");
            return;
        }

        $projectName = basename($projectDir);
        $s           = $this->stats;
        $hasFailures = $s['studies_failed'] > 0;

        $projectJson = rtrim($projectDir, '/') . '/project.json';
        $projectData = file_exists($projectJson)
            ? (json_decode(file_get_contents($projectJson), true) ?? [])
            : [];

        $notifConfig = $projectData['notification_emails']['dicom']
            ?? $this->config['notification_emails']['dicom']
            ?? $this->config['notification_defaults']
            ?? [];

        if (!($notifConfig['enabled'] ?? false)) {
            $this->log("  Notifications disabled for dicom");
            return;
        }

        $successEmails = $notifConfig['on_success']
            ?? $this->config['notification_defaults']['default_on_success']
            ?? [];
        $errorEmails   = $notifConfig['on_error']
            ?? $this->config['notification_defaults']['default_on_error']
            ?? [];

        $emailsToSend = $hasFailures ? $errorEmails : $successEmails;

        if (empty($emailsToSend)) {
            $this->log("  No notification emails configured for dicom");
            return;
        }

        $status  = $hasFailures ? 'FAILED' : 'SUCCESS';
        $subject = "{$status}: {$projectName} DICOM Import";

        $body  = "Project: {$projectName}\n";
        $body .= "Modality: dicom\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n";
        $body .= "Run: {$this->runTimestamp}\n\n";
        $body .= "DICOM Study Import:\n";

        $byStatus = ['success' => [], 'failed' => [], 'skipped' => [], 'already_exists' => [], 'dry_run' => []];
        foreach ($this->importResults as $study => $r) {
            $byStatus[$r['status']][] = $study;
        }

        if (empty($this->importResults)) {
            $body .= "  (no studies found)\n";
        } else {
            if (!empty($byStatus['success'])) {
                $body .= "  ✔ Imported: " . count($byStatus['success']) . "\n";
                foreach ($byStatus['success'] as $study) {
                    $e = $this->importResults[$study]['elapsed'] ?? '';
                    $j = $this->importResults[$study]['job_id'] ?? null;
                    $body .= "     {$study}" . ($e ? " ({$e}s)" : "") . ($j ? " [job {$j}]" : "") . "\n";
                }
            }
            if (!empty($byStatus['already_exists'])) {
                $body .= "  ● Already existed: " . count($byStatus['already_exists'])
                    . " (" . implode(', ', $byStatus['already_exists']) . ")\n";
            }
            if (!empty($byStatus['failed'])) {
                $body .= "  ✗ Failed: " . count($byStatus['failed']) . "\n";
                foreach ($byStatus['failed'] as $study) {
                    $reason   = $this->importResults[$study]['reason'] ?? 'unknown';
                    $j        = $this->importResults[$study]['job_id'] ?? null;
                    $exitCode = $this->importResults[$study]['exit_code'] ?? null;
                    $body    .= "     {$study}"
                        . ($j        ? " [job {$j}]"    : "")
                        . ($exitCode !== null ? " exit={$exitCode}" : "") . "\n"
                        . "       Error: {$reason}\n";
                }
            }
            if (!empty($byStatus['skipped'])) {
                $body .= "  ⚠ Skipped: " . count($byStatus['skipped']) . "\n";
            }
        }

        $body .= "\n" . str_repeat('-', 50) . "\n";
        $body .= "Studies found:          {$s['studies_found']}\n";
        $body .= "Successfully imported:  {$s['studies_processed']}\n";
        $body .= "Already existed:        {$s['studies_already_exist']}\n";
        $body .= "Skipped:                {$s['studies_skipped']}\n";
        $body .= "Failed:                 {$s['studies_failed']}\n";

        if ($this->runLogPath)   $body .= "\nRun log: {$this->runLogPath}\n";
        if ($this->errorLogPath) $body .= "Error log: {$this->errorLogPath}\n";

        $this->log("  Sending notification to: " . implode(', ', $emailsToSend));
        foreach ($emailsToSend as $to) {
            $this->notification->send($to, $subject, $body);
        }
    }
}