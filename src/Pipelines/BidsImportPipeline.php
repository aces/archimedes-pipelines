<?php

declare(strict_types=1);

namespace LORIS\Pipelines;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use LORIS\Utils\{Notification, CleanLogFormatter};
use GuzzleHttp\Client as GuzzleClient;

/**
 * BIDS Import Pipeline
 *
 * Runs POST /cbigr_api/script/bidsimport with async=true on the
 * deidentified-lorisid/bids/ directory, then polls
 * GET /cbigr_api/script/job/{id} until the job completes.
 *
 * Assumes BidsParticipantSync and BidsReidentifier have already run:
 *   - Candidates exist in LORIS with ExternalID mappings
 *   - deidentified-lorisid/bids/ is populated with PSCID-labelled data
 *
 * Enabled when project.json lists "Imaging" in modalities.
 * Notifications use project.json → notification_emails.bids_import key.
 *
 * UNKNOWN state handling:
 *   SPM returns UNKNOWN when the exit code file is deleted before the poll
 *   can read it. This is normal on a clean exit (exit 0). We resolve it by
 *   tracking $wasRunning — if the job was seen as RUNNING, treat as SUCCESS.
 *
 * @package LORIS\Pipelines
 */
class BidsImportPipeline
{
    private Logger       $logger;
    private array        $config;
    private array        $projectConfig = [];
    private string       $projectPath   = '';
    private ?string      $token         = null;
    private GuzzleClient $httpClient;
    private Notification $notification;

    private string $runTimestamp;

    /** @var resource|null */
    private $runLogFh   = null;
    private ?string $runLogPath  = null;

    /** @var resource|null */
    private $errorFh    = null;
    private ?string $errorLogPath = null;

    private ?string $logDir = null;

    /** Tracking file name — stored in {projectPath}/processed/ */
    private const TRACK_FILE = '.bids_import_processed.json';

    /** Seconds between job status poll requests */
    private const POLL_INTERVAL_SECONDS = 30;

    /** Maximum seconds to wait for bidsimport before timing out */
    private const POLL_TIMEOUT_SECONDS = 86400; // 24 hours

    /** Consecutive poll failures before giving up */
    private const MAX_POLL_FAILURES = 3;

    private array $stats = [
        'scans_found'     => 0,
        'scans_processed' => 0,
        'scans_skipped'   => 0,
        'scans_failed'    => 0,
        'errors'          => [],
        'job_id'          => null,
        'pid'             => null,
    ];

    public function __construct(array $config = [], bool $verbose = false)
    {
        // Support both 'loris' and 'api' config keys
        if (!isset($config['loris']) && isset($config['api'])) {
            $config['loris'] = $config['api'];
        }

        $this->config        = $config;
        $this->runTimestamp  = date('Y-m-d_H-i-s');
        $this->notification  = new Notification();
        $this->httpClient    = new GuzzleClient([
            'verify'      => $config['verify_ssl'] ?? false,
            'timeout'     => 30,
            'http_errors' => false,
        ]);

        $this->_initLogger($verbose);
    }

    // =========================================================================
    //  LOGGING
    // =========================================================================

    private function _initLogger(bool $verbose): void
    {
        $logLevel  = $verbose ? Logger::DEBUG : Logger::INFO;
        $this->logger = new Logger('bids-import');

        $formatter      = new CleanLogFormatter();
        $consoleHandler = new StreamHandler('php://stdout', $logLevel);
        $consoleHandler->setFormatter($formatter);
        $this->logger->pushHandler($consoleHandler);
    }

    private function _initProjectLogger(string $projectPath): void
    {
        $this->logDir = "{$projectPath}/logs";
        if (!is_dir($this->logDir)) {
            mkdir($this->logDir, 0755, true);
        }

        $formatter = new CleanLogFormatter();

        $this->runLogPath = "{$this->logDir}/bids_import_run_{$this->runTimestamp}.log";
        $this->runLogFh   = @fopen($this->runLogPath, 'a');
        if ($this->runLogFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->runLogFh,
                "{$sep}\n BIDS Import Pipeline — Run Log\n"
                . " Started: " . date('Y-m-d H:i:s T') . "\n{$sep}\n\n"
            );
        }

        $fileHandler = new StreamHandler($this->runLogPath, Logger::DEBUG);
        $fileHandler->setFormatter($formatter);
        $this->logger->pushHandler($fileHandler);
    }

    private function _log(string $msg): void
    {
        $this->logger->info($msg);
    }

    private function _writeError(string $context, string $msg): void
    {
        $this->logger->error("[{$context}] {$msg}");

        if ($this->errorFh === null && $this->logDir !== null) {
            $this->errorLogPath = "{$this->logDir}/bids_import_errors_{$this->runTimestamp}.log";
            $this->errorFh      = @fopen($this->errorLogPath, 'a');
            if ($this->errorFh) {
                $sep = str_repeat('=', 72);
                fwrite($this->errorFh,
                    "{$sep}\n BIDS Import Pipeline — Error Log\n"
                    . " Run: {$this->runTimestamp}\n{$sep}\n\n"
                );
                $this->_log("  Error log: {$this->errorLogPath}");
            }
        }

        $ts = date('H:i:s');
        if ($this->errorFh) {
            fwrite($this->errorFh, "[{$ts}] [{$context}] {$msg}\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[{$ts}] ERROR [{$context}] {$msg}\n");
        }
        $this->stats['errors'][] = "[{$context}] {$msg}";
    }

    private function _closeAllLogs(): void
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

    // =========================================================================
    //  AUTHENTICATION
    // =========================================================================

    public function authenticate(string $username, string $password): bool
    {
        $baseUrl = rtrim($this->config['loris']['base_url'] ?? $this->config['loris_url'] ?? '', '/');
        $version = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';

        try {
            $response    = $this->httpClient->request('POST', "{$baseUrl}/api/{$version}/login", [
                'json' => compact('username', 'password'),
            ]);
            $data        = json_decode((string)$response->getBody(), true);
            $this->token = $data['token'] ?? null;
            return !empty($this->token);
        } catch (\Exception $e) {
            $this->logger->error("Authentication failed: " . $e->getMessage());
            return false;
        }
    }

    // =========================================================================
    //  CONFIG
    // =========================================================================

    /**
     * Check if BIDS import is enabled for this project.
     *
     * Enabled when project.json lists "Imaging" (case-insensitive) in modalities.
     *
     * @return bool
     */
    public function isEnabled(): bool
    {
        $modalities = $this->projectConfig['modalities'] ?? [];
        foreach ($modalities as $m) {
            if (strcasecmp(trim($m), 'imaging') === 0) {
                return true;
            }
        }
        return false;
    }

    private function _loadProjectConfig(string $projectPath): ?array
    {
        $configFile = "{$projectPath}/project.json";
        if (!file_exists($configFile)) {
            $this->logger->error("project.json not found: {$configFile}");
            return null;
        }

        $config = json_decode(file_get_contents($configFile), true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->logger->error("Invalid project.json: " . json_last_error_msg());
            return null;
        }

        return $config;
    }

    // =========================================================================
    //  BIDS DIRECTORY
    // =========================================================================

    private function _findBidsDirectory(string $projectPath): ?string
    {
        if (!empty($this->projectConfig['bids_path'])
            && is_dir($this->projectConfig['bids_path'])
        ) {
            return $this->projectConfig['bids_path'];
        }

        $locations = [
            "{$projectPath}/deidentified-lorisid/bids",
            "{$projectPath}/deidentified-lorisid/imaging/bids",
            "{$projectPath}/bids",
        ];

        foreach ($locations as $path) {
            if (is_dir($path)) {
                return $path;
            }
        }

        return null;
    }

    // =========================================================================
    //  TRACKING
    // =========================================================================

    private function _loadTracking(string $projectPath): array
    {
        $path = rtrim($projectPath, '/') . '/processed/' . self::TRACK_FILE;
        if (!file_exists($path)) {
            return [];
        }
        return json_decode(file_get_contents($path), true) ?? [];
    }

    private function _saveTracking(string $projectPath, array $tracking): void
    {
        $dir = rtrim($projectPath, '/') . '/processed';
        if (!is_dir($dir)) {
            @mkdir($dir, 0755, true);
        }
        $path = "{$dir}/" . self::TRACK_FILE;
        file_put_contents($path, json_encode($tracking, JSON_PRETTY_PRINT));
    }

    private function _markTracked(
        string $projectPath,
        array &$tracking,
        string $status,
        string $detail = ''
    ): void {
        $tracking['last_run'] = [
            'status'     => $status,
            'timestamp'  => date('c'),
            'detail'     => $detail,
            'scans_found' => $this->stats['scans_found'],
        ];
        $this->_saveTracking($projectPath, $tracking);
    }

    private function _countNewScans(string $bidsPath, array $tracking): int
    {
        // Compare current sub-* count against what was tracked on last success.
        // bidsimport runs on the whole directory — new = subjects added since last run.
        $currentCount = count(glob("{$bidsPath}/sub-*", GLOB_ONLYDIR) ?: []);
        $trackedCount = (int) ($tracking['last_run']['scans_found'] ?? 0);
        return max(0, $currentCount - $trackedCount);
    }

    // =========================================================================
    //  PARTICIPANTS TSV — ENRICH MISSING COLUMNS
    // =========================================================================

    /**
     * Enrich target participants.tsv with missing required columns.
     *
     * bids_import requires cohort, site, and sex columns.
     * If participants.tsv is missing any, inject defaults from
     * project.json → candidate_defaults.
     * For site, use site_alias (short form) not the full site name.
     *
     * @param string $bidsPath BIDS directory containing participants.tsv
     */
    private function _enrichParticipantsTsv(string $bidsPath): void
    {
        $tsvPath = rtrim($bidsPath, '/') . '/participants.tsv';

        if (!file_exists($tsvPath)) {
            $this->_log("  No participants.tsv found in {$bidsPath} — skipping enrich");
            return;
        }

        // Read existing TSV
        $handle = fopen($tsvPath, 'r');
        if (!$handle) return;

        $headerLine = fgets($handle);
        if (!$headerLine) { fclose($handle); return; }
        $headers = array_map('trim', explode("	", trim($headerLine)));

        $rows = [];
        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if ($line === '') continue;
            $fields = array_map('trim', explode("	", $line));
            $row    = [];
            foreach ($headers as $i => $h) {
                $row[$h] = $fields[$i] ?? '';
            }
            $rows[] = $row;
        }
        fclose($handle);

        // Resolve defaults from project.json candidate_defaults
        $defaults = $this->projectConfig['candidate_defaults'] ?? [];

        // Columns to inject if missing — all taken directly from candidate_defaults
        $requiredCols = [
            'cohort' => $defaults['cohort'] ?? '',
            'site'   => $defaults['site']   ?? '',
            'sex'    => $defaults['sex']    ?? '',
        ];

        $addedCols = [];
        foreach ($requiredCols as $col => $val) {
            if (!in_array($col, $headers, true) && !empty($val)) {
                $headers[]       = $col;
                $addedCols[$col] = $val;
                $this->_log("  participants.tsv missing '{$col}'"
                    . " — taking value from project.json candidate_defaults: '{$val}'");
            }
        }

        if (empty($addedCols)) {
            $this->_log("  participants.tsv has all required columns — no changes needed");
            return;
        }

        // Rewrite TSV
        $out = fopen($tsvPath, 'w');
        if (!$out) {
            $this->_log("  WARNING: Cannot write enriched participants.tsv");
            return;
        }

        fwrite($out, implode("	", $headers) . "
");
        foreach ($rows as $row) {
            foreach ($addedCols as $col => $val) {
                $row[$col] = $val;
            }
            $values = array_map(fn($h) => $row[$h] ?? '', $headers);
            fwrite($out, implode("	", $values) . "
");
        }
        fclose($out);

        $this->_log("  ✓ participants.tsv enriched ("
            . count($rows) . " rows, added: "
            . implode(', ', array_keys($addedCols)) . ")");
    }

    // =========================================================================
    //  ASYNC LAUNCH + POLL
    // =========================================================================

    private function _launchBidsImport(string $bidsPath, array $options): array
    {
        $baseUrl = rtrim(
            $this->config['loris']['base_url'] ?? $this->config['loris_url'] ?? '',
            '/'
        );
        $url = "{$baseUrl}/cbigr_api/script/bidsimport";

        $flags = ['createcandidate', 'createsession'];
        if ($options['verbose'] ?? false) {
            $flags[] = 'verbose';
        }
        if ($options['no_bids_validation'] ?? false) {
            $flags[] = 'nobidsvalidation';
        }

        $payload = [
            'args'  => [
                'directory' => $bidsPath,
                'profile'   => $this->config['loris_profile']
                    ?? $this->config['loris']['profile']
                        ?? 'database_config.py',
            ],
            'flags' => $flags,
            'async' => true,
        ];

        $this->logger->debug("POST {$url}");
        $this->logger->debug(json_encode($payload, JSON_PRETTY_PRINT));

        try {
            $response            = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json'    => $payload,
                'timeout' => 30,
            ]);
            $statusCode          = $response->getStatusCode();
            $rawBody             = (string)$response->getBody();
            $this->logger->debug("  Response HTTP {$statusCode}: {$rawBody}");
            $body                = json_decode($rawBody, true) ?? [];
            $body['http_status'] = $statusCode;
            return $body;
        } catch (\Exception $e) {
            return ['http_status' => 0, 'error' => $e->getMessage()];
        }
    }

    private function _pollJob(int $jobId): ?array
    {
        $baseUrl = rtrim(
            $this->config['loris']['base_url'] ?? $this->config['loris_url'] ?? '',
            '/'
        );

        try {
            $response = $this->httpClient->request(
                'GET',
                "{$baseUrl}/cbigr_api/script/job/{$jobId}",
                [
                    'headers' => ['Authorization' => "Bearer {$this->token}"],
                    'timeout' => 15,
                ]
            );
            $statusCode = $response->getStatusCode();
            if ($statusCode !== 200) {
                $this->logger->debug("  poll HTTP {$statusCode} for job {$jobId}");
                return null;
            }
            $body = json_decode((string)$response->getBody(), true);
            return is_array($body) ? $body : null;
        } catch (\Exception $e) {
            $this->logger->debug("  poll exception job {$jobId}: " . $e->getMessage());
            return null;
        }
    }

    // =========================================================================
    //  MAIN
    // =========================================================================

    public function run(string $projectPath, array $options = []): array
    {
        $this->projectPath = $projectPath;
        $this->_initProjectLogger($projectPath);

        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  BIDS Import Pipeline");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  Project path : {$projectPath}");
        $this->_log("  Run          : {$this->runTimestamp}");
        $this->_log("───────────────────────────────────────────────────────────");

        // Load project config first — isEnabled() depends on it
        $this->projectConfig = $this->_loadProjectConfig($projectPath);
        if (!$this->projectConfig) {
            $this->_writeError("CONFIG", "Failed to load project.json");
            $success = empty($this->stats['errors'])
                && $this->stats['scans_failed'] === 0;
            $this->sendNotification($success);
            $this->_closeAllLogs();
            return $this->stats;
        }

        // Gate on project.json modalities["Imaging"]
        if (!$this->isEnabled()) {
            $this->_log("  Skipping — 'Imaging' not listed in project.json modalities");
            $success = empty($this->stats['errors'])
                && $this->stats['scans_failed'] === 0;
            $this->sendNotification($success);
            $this->_closeAllLogs();
            return $this->stats;
        }

        $projectName = $this->projectConfig['project_common_name'] ?? basename($projectPath);
        $this->_log("  Project name : {$projectName}");

        // Find BIDS directory (deidentified-lorisid/bids/)
        $bidsPath = $this->_findBidsDirectory($projectPath);
        if (!$bidsPath) {
            $this->_log("  No BIDS directory found in {$projectPath}");
            $success = empty($this->stats['errors'])
                && $this->stats['scans_failed'] === 0;
            $this->sendNotification($success);
            $this->_closeAllLogs();
            return $this->stats;
        }

        $this->_log("  BIDS directory: {$bidsPath}");

        $this->stats['scans_found'] = $this->_countScans($bidsPath);
        $this->_log("  Scans found: {$this->stats['scans_found']}");

        // Load tracking — skip if already successfully imported
        // To reimport, delete the tracking file:
        //   {projectPath}/processed/.bids_import_processed.json
        $tracking   = $this->_loadTracking($projectPath);
        $lastRun    = $tracking['last_run'] ?? null;
        $lastStatus = $lastRun['status']    ?? '';
        $lastTs     = $lastRun['timestamp'] ?? '';

        $this->_log("  Tracking: "
            . ($lastRun ? "last run {$lastTs} — {$lastStatus}" : "no previous run"));

        if ($lastStatus === 'success') {
            $newScans = $this->_countNewScans($bidsPath, $tracking);
            if ($newScans === 0) {
                $this->_log("  ✓ Already imported successfully (last run: {$lastTs})"
                    . " — no new scans found. Skipping.");
                $this->_log("  To reimport, delete:"
                    . " {$projectPath}/processed/.bids_import_processed.json");
                $this->stats['scans_skipped'] = $this->stats['scans_found'];
                $success = true;
                $this->sendNotification($success);
                $this->_closeAllLogs();
                return $this->stats;
            }
            $this->_log("  {$newScans} new scan(s) found since last run — importing");
        }

        if ($options['dry_run'] ?? false) {
            $this->_log("");
            $this->_log("  [DRY-RUN] Would POST /cbigr_api/script/bidsimport");
            $this->_log("    directory : {$bidsPath}");
            $this->_log("    flags     : createcandidate, createsession");
            $this->_closeAllLogs();
            return $this->stats;
        }

        // Enrich participants.tsv with missing required columns
        // (cohort, site alias, sex) from project.json candidate_defaults
        $this->_enrichParticipantsTsv($bidsPath);

        // Launch async bidsimport
        $this->_log("  Launching bidsimport ...");
        $launch = $this->_launchBidsImport($bidsPath, $options);

        if (($launch['http_status'] ?? 0) !== 202) {
            $errMsg = $launch['error'] ?? "HTTP " . ($launch['http_status'] ?? '?');
            $this->_writeError("LAUNCH", "Failed to launch bidsimport: {$errMsg}");
            $this->stats['scans_failed'] = $this->stats['scans_found'];
            $success = empty($this->stats['errors'])
                && $this->stats['scans_failed'] === 0;
            $this->sendNotification($success);
            $this->_closeAllLogs();
            return $this->stats;
        }

        $jobId = $launch['job_id'] ?? null;
        $pid   = $launch['pid']    ?? null;

        if (!$jobId) {
            $this->_writeError("LAUNCH", "No job_id returned from async launch");
            $success = empty($this->stats['errors'])
                && $this->stats['scans_failed'] === 0;
            $this->sendNotification($success);
            $this->_closeAllLogs();
            return $this->stats;
        }

        $this->stats['job_id'] = $jobId;
        $this->stats['pid']    = $pid;

        $this->_log("  Launched — job_id={$jobId} pid={$pid},"
            . " polling every " . self::POLL_INTERVAL_SECONDS . "s ...");

        // Poll loop
        $t0           = microtime(true);
        $pollStart    = $t0;
        $pollFailures = 0;
        $wasRunning   = false;
        $firstPoll    = true;

        while (true) {
            if ($firstPoll) {
                $firstPoll = false;
                sleep(5);
            } else {
                sleep(self::POLL_INTERVAL_SECONDS);
            }

            $elapsed = round(microtime(true) - $t0, 2);

            if ((microtime(true) - $pollStart) > self::POLL_TIMEOUT_SECONDS) {
                $errMsg = "Job {$jobId} timed out after " . self::POLL_TIMEOUT_SECONDS . "s";
                $this->_writeError("TIMEOUT", $errMsg);
                $this->stats['scans_failed'] = $this->stats['scans_found'];
                $this->_closeAllLogs();
                return $this->stats;
            }

            $status = $this->_pollJob($jobId);

            if ($status === null) {
                $pollFailures++;
                $this->_log("  WARNING: poll failed for job {$jobId} at {$elapsed}s"
                    . " — attempt {$pollFailures}/" . self::MAX_POLL_FAILURES);
                if ($pollFailures >= self::MAX_POLL_FAILURES) {
                    $this->_writeError("POLL",
                        "Job {$jobId} poll failed " . self::MAX_POLL_FAILURES . " consecutive times"
                    );
                    $this->stats['scans_failed'] = $this->stats['scans_found'];
                    $this->_closeAllLogs();
                    return $this->stats;
                }
                continue;
            }

            $pollFailures = 0;
            $state        = $status['state'] ?? 'UNKNOWN';
            $this->_log("  [{$elapsed}s] job {$jobId} → {$state}");

            if ($state === 'RUNNING') {
                $wasRunning = true;
                continue;
            }

            // UNKNOWN: SPM deleted exit code file before poll could read it.
            // If we saw RUNNING before, process completed cleanly.
            if ($state === 'UNKNOWN') {
                if ($wasRunning) {
                    $this->_log(
                        "  ✓ SUCCESS (exit code file cleaned up after successful run) ({$elapsed}s)"
                    );
                    $this->_markTracked($projectPath, $tracking, 'success');
                    $this->stats['scans_processed'] = $this->stats['scans_found'];
                    $this->_closeAllLogs();
                    return $this->stats;
                }
                $this->_writeError("UNKNOWN",
                    "Job {$jobId} ended UNKNOWN without ever running"
                    . " — check server_processes id={$jobId}"
                );
                $this->stats['scans_failed'] = $this->stats['scans_found'];
                $this->_closeAllLogs();
                return $this->stats;
            }

            if ($state === 'SUCCESS') {
                $this->_log("  ✓ SUCCESS ({$elapsed}s)");
                $this->_markTracked($projectPath, $tracking, 'success');
                $this->stats['scans_processed'] = $this->stats['scans_found'];
                $this->_closeAllLogs();
                return $this->stats;
            }

            // ERROR
            $errorDetail = $status['error_detail'] ?? $status['progress'] ?? 'Unknown error';
            $exitCode    = $status['exit_code'] ?? '?';

            // Use full stdout/stderr if JobStatus returned them (preferred).
            // Falls back to truncated error_detail from exit_text.
            $fullOutput = '';
            if (!empty($status['stderr'])) {
                $fullOutput = $status['stderr'];
            } elseif (!empty($status['stdout'])) {
                $fullOutput = $status['stdout'];
            }

            $this->_markTracked($projectPath, $tracking, 'failed',
                "exit {$exitCode} job_id={$jobId}");
            $this->_writeError("BIDSIMPORT",
                "Script failed (exit {$exitCode}). job_id={$jobId} pid={$pid}."
                . (!empty($fullOutput)
                    ? "\n\nFull output:\n{$fullOutput}"
                    : "\n\nOutput (truncated):\n    "
                    . str_replace("\n", "\n    ", $errorDetail))
            );
            $this->stats['scans_failed'] = $this->stats['scans_found'];
            $success = empty($this->stats['errors'])
                && $this->stats['scans_failed'] === 0;
            $this->sendNotification($success);
            $this->_closeAllLogs();
            return $this->stats;
        }
    }

    // =========================================================================
    //  HELPERS
    // =========================================================================

    private function _countScans(string $bidsPath): int
    {
        $count    = 0;
        $subjects = glob("{$bidsPath}/sub-*", GLOB_ONLYDIR) ?: [];

        foreach ($subjects as $subjectDir) {
            $sessions = glob("{$subjectDir}/ses-*", GLOB_ONLYDIR) ?: [];
            $count   += empty($sessions) ? 1 : count($sessions);
        }

        return $count;
    }

    // =========================================================================
    //  EMAIL NOTIFICATION
    // =========================================================================

    /**
     * Send email notification.
     *
     * Gated on:
     *   1. project.json modalities contains "Imaging"
     *   2. project.json notification_emails.bids.enabled = true
     */
    public function sendNotification(bool $success): void
    {
        if ($this->dryRun ?? false) {
            $this->_log("  Notification skipped (dry run)");
            return;
        }

        $projectName = $this->projectConfig['project_common_name']
            ?? basename($this->projectPath);

        // Read notification config — project.json takes priority,
        // fall back to global config, matching DicomImportPipeline pattern.
        $notifConfig = $this->projectConfig['notification_emails']['bids']
            ?? $this->config['notification_emails']['bids']
            ?? $this->config['notification_defaults']
            ?? [];

        if (!($notifConfig['enabled'] ?? false)) {
            $this->_log("  Notifications disabled (notification_emails.bids.enabled = false)");
            return;
        }

        $successEmails = $notifConfig['on_success']
            ?? $this->config['notification_defaults']['default_on_success']
            ?? [];
        $errorEmails   = $notifConfig['on_error']
            ?? $this->config['notification_defaults']['default_on_error']
            ?? [];

        $emailsToSend = $success ? $successEmails : $errorEmails;

        if (empty($emailsToSend)) {
            $this->_log("  No notification recipients configured for bids_import");
            return;
        }

        $status  = $success ? 'SUCCESS' : 'FAILED';
        $subject = "{$status}: {$projectName} BIDS Import Pipeline";
        $body    = $this->_buildEmailBody($success, $projectName);

        $this->_log("  Sending notification to: " . implode(', ', $emailsToSend));
        foreach ($emailsToSend as $to) {
            $this->notification->send($to, $subject, $body);
        }
    }

    private function _buildEmailBody(bool $success, string $projectName): string
    {
        $s    = $this->stats;
        $body = "Project    : {$projectName}\n";
        $body .= "Run        : {$this->runTimestamp}\n";
        $body .= "Timestamp  : " . date('Y-m-d H:i:s') . "\n";
        $body .= "Status     : " . ($success ? 'SUCCESS' : 'FAILED') . "\n\n";

        if ($s['job_id']) {
            $body .= "SPM Job    : job_id={$s['job_id']}"
                . ($s['pid'] ? " pid={$s['pid']}" : "") . "\n\n";
        }

        $body .= "Statistics:\n";
        $body .= str_repeat('-', 40) . "\n";
        $body .= "Scans found     : {$s['scans_found']}\n";
        $body .= "Scans processed : {$s['scans_processed']}\n";
        $body .= "Scans skipped   : {$s['scans_skipped']}\n";
        $body .= "Scans failed    : {$s['scans_failed']}\n";

        if (!empty($s['errors'])) {
            $body .= "\nErrors:\n" . str_repeat('-', 40) . "\n";
            foreach ($s['errors'] as $err) {
                $body .= "- {$err}\n";
            }
        }

        if ($this->runLogPath)   $body .= "\nRun log  : {$this->runLogPath}\n";
        if ($this->errorLogPath) $body .= "Error log: {$this->errorLogPath}\n";

        return $body;
    }

    public function getStats(): array
    {
        return $this->stats;
    }

    public function getProjectConfig(): array
    {
        return $this->projectConfig;
    }
}