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

    /**
     * Full bids_import.py stdout, captured incrementally during polling
     * so it survives SPM's /tmp cleanup. Bytes are appended on each poll
     * via _captureScriptOutput(); _scriptOutputOffset tracks how much we
     * already have so we don't duplicate.
     *
     * @var resource|null
     */
    private $scriptOutputFh    = null;
    private ?string $scriptOutputPath = null;
    private int $scriptOutputOffset   = 0;

    private ?string $logDir = null;

    /** Temp staging dir for enriched participants.tsv; cleaned in _closeAllLogs(). */
    private ?string $stagingDir = null;

    /**
     * Per-subject scan inventory built from the BIDS tree at run time.
     * Shape: ['subjects' => [PSCID => ['sessions' => [ses => [mod => count]]]],
     *         'totals'   => ['subjects' => N, 'sessions' => N, 'scans' => N,
     *                        'by_modality' => [mod => count]]].
     */
    private array $scanInventory = [];

    /**
     * Once-per-run guard for email notifications. Set true after the first
     * sendNotification() actually dispatches; further calls (from the
     * pipeline itself, or from a wrapper script that also calls it) are
     * silently ignored to prevent the duplicate-email problem.
     */
    private bool $notificationSent = false;

    /** Tracking file name — stored in {projectPath}/processed/bids/ */
    private const TRACK_FILE = '.bids_import_processed.json';

    /**
     * Persisted copy of the enriched participants.tsv handed to bidsimport,
     * stored in {projectPath}/processed/bids/. Kept for audit so an operator
     * can verify which cohort/site/sex values were applied to which subjects
     * in the most recent run. Overwritten on each enrichment.
     */
    private const PROCESSED_TSV = 'participants_processed.tsv';

    /**
     * Adaptive poll interval (seconds). The wrapper polls fast at the start
     * to catch jobs that complete in under a minute (so the SPM stdout/stderr
     * files are still present when JobStatus reads them), then backs off as
     * elapsed time grows so long jobs don't generate poll spam.
     *
     * Logic in _adaptivePollInterval(): for elapsed E in seconds —
     *   E <  30  → 2s   (fast jobs caught while RUNNING)
     *   E <  300 → 10s  (medium jobs)
     *   E >= 300 → 30s  (long jobs)
     */
    private const POLL_INTERVAL_FAST    = 2;
    private const POLL_INTERVAL_MEDIUM  = 10;
    private const POLL_INTERVAL_SLOW    = 30;

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
        // Populated when scans_skipped > 0 — explains why they were skipped
        // so the summary doesn't just say "skipped 3" with no context.
        'skip_reason'     => '',
        // Timestamp of the original import run that processed these scans
        // (when this run is a no-op because everything was already imported).
        'last_imported_at' => '',
        // Per-file counts parsed from the python script's stdout. The SPM job
        // can return SUCCESS (exit 0) even when individual files failed; these
        // fields surface that detail so a partial failure isn't reported as
        // a full success.
        'files_total'    => null,  // total NIfTI files the script saw
        'files_imported' => null,  // newly-inserted in LORIS this run
        'files_ignored'  => null,  // already registered, skipped
        'files_errored'  => null,  // hash collision, bad data, etc.
        // Per-failure attribution. Each entry: {pscid, session, file, reason}.
        // Built from the python script's stdout when files_errored > 0 so the
        // operator sees exactly which candidate-session-file combinations
        // failed without grepping logs.
        'failures'       => [],
        // SUMMARY status — pipeline ran successfully but file-level issues
        // (hash collisions, empty files) prevented some individual scans
        // from importing. Distinct from FAILED (pipeline broke) and
        // SUCCESS (everything imported cleanly).
        'summary_status'     => false,
        'file_issue_count'   => 0,
        'script_output_tail' => '',
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
        $this->logDir = "{$projectPath}/logs/bids";
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

        // Pre-resolve the script-output log path so _captureScriptOutput
        // can lazily open it on first poll that actually has stdout.
        // Skipping this for runs with no SPM job (no-op skip, validation
        // failure before launch, etc) keeps the log dir tidy.
        $this->scriptOutputPath = "{$this->logDir}/bids_import_script_output_"
            . "{$this->runTimestamp}.log";

        $fileHandler = new StreamHandler($this->runLogPath, Logger::DEBUG);
        $fileHandler->setFormatter($formatter);
        $this->logger->pushHandler($fileHandler);
    }

    /**
     * Append new bytes from the script's stdout to the per-run script
     * output log. Called on every poll while a job is running. Tracks the
     * cumulative offset so we only write the diff each time, not the
     * whole stdout-so-far over and over.
     *
     * The file is opened lazily on the first poll that returns non-empty
     * stdout — this avoids creating empty script-output logs for runs
     * that don't actually launch a script (no-op skips, early failures).
     *
     * Why this exists: SPM writes stdout to /tmp/spm_xxxxxx and deletes
     * the file as soon as the job exits with code 0. By the time the
     * pipeline finishes polling and tries to read the full output for
     * per-subject error attribution, the file is gone. Capturing during
     * polling — when JobStatus inlines the file contents in its response
     * — gives us a durable copy on the pipeline's own filesystem, scoped
     * to the project's logs/bids/ directory alongside the run and error
     * logs.
     */
    private function _captureScriptOutput(string $cumulativeStdout): void
    {
        $totalLen = strlen($cumulativeStdout);
        if ($totalLen <= $this->scriptOutputOffset) {
            return; // nothing new
        }

        if ($this->scriptOutputFh === null) {
            if ($this->scriptOutputPath === null) {
                return; // logger not initialized — defensive
            }
            $this->scriptOutputFh = @fopen($this->scriptOutputPath, 'a');
            if (!$this->scriptOutputFh) {
                $this->_log("  Note: could not open script output log at "
                    . "{$this->scriptOutputPath} — full per-file attribution"
                    . " may be limited to what the parser captures inline.");
                return;
            }
            $sep = str_repeat('=', 72);
            fwrite($this->scriptOutputFh,
                "{$sep}\n BIDS Import Pipeline — Full Script Output\n"
                . " Run     : {$this->runTimestamp}\n"
                . " Started : " . date('Y-m-d H:i:s T') . "\n"
                . " Source  : bids_import.py stdout (captured incrementally"
                . " during SPM polling — see SPM job_id in run log for the"
                . " original SPM job).\n{$sep}\n\n"
            );
            $this->_log("  Script output log: {$this->scriptOutputPath}");
        }

        $newBytes = substr($cumulativeStdout, $this->scriptOutputOffset);
        fwrite($this->scriptOutputFh, $newBytes);
        $this->scriptOutputOffset = $totalLen;
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
        // Always tear down the temp staging dir before closing logs so its
        // cleanup line shows up in the run log and an interrupted run doesn't
        // leave orphan symlink trees in /tmp.
        $this->_cleanupStaging();

        $sep = str_repeat('=', 72);
        $ts  = date('Y-m-d H:i:s T');
        if ($this->errorFh) {
            fwrite($this->errorFh, "\n{$sep}\n Closed: {$ts}\n{$sep}\n");
            fclose($this->errorFh);
            $this->errorFh = null;
        }
        if ($this->scriptOutputFh) {
            // Footer mirrors the run/error log style. Note total bytes
            // captured so the operator can spot truncation at a glance
            // (full bids_import runs are typically tens of KB; 0 bytes
            // here means JobStatus never inlined stdout — likely the
            // server-side change isn't deployed yet).
            fwrite($this->scriptOutputFh,
                "\n{$sep}\n Closed: {$ts}\n"
                . " Bytes captured: {$this->scriptOutputOffset}\n{$sep}\n"
            );
            fclose($this->scriptOutputFh);
            $this->scriptOutputFh = null;
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
        $path = rtrim($projectPath, '/') . '/processed/bids/' . self::TRACK_FILE;
        if (!file_exists($path)) {
            return [];
        }
        return json_decode(file_get_contents($path), true) ?? [];
    }

    private function _saveTracking(string $projectPath, array $tracking): void
    {
        $dir = rtrim($projectPath, '/') . '/processed/bids';
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
    /**
     * Produce a BIDS directory ready for bidsimport.
     *
     * If participants.tsv is already complete (has cohort/site/sex), returns
     * $bidsPath unchanged. Otherwise builds a temp BIDS tree under
     * sys_get_temp_dir() that symlinks every source item EXCEPT
     * participants.tsv, and writes the enriched tsv as a real file in the
     * temp tree. bidsimport sees a complete BIDS dataset; the source
     * deidentified-lorisid/bids/ is never modified.
     *
     * The temp dir is recorded on $this->stagingDir so _closeAllLogs()
     * cleans it up on every exit path.
     *
     * @return string|null Path to use for bidsimport, or null on hard error
     */
    private function _enrichParticipantsTsv(string $bidsPath): ?string
    {
        $tsvPath = rtrim($bidsPath, '/') . '/participants.tsv';

        if (!file_exists($tsvPath)) {
            $this->_log("  No participants.tsv found in {$bidsPath} — skipping enrich");
            return $bidsPath;
        }

        // Read existing TSV (read-only — never write back to source)
        $handle = @fopen($tsvPath, 'r');
        if (!$handle) {
            $this->_writeError("ENRICH",
                "Cannot read participants.tsv at {$tsvPath}"
                . " — check file permissions"
            );
            return null;
        }

        $headerLine = fgets($handle);
        if (!$headerLine) { fclose($handle); return $bidsPath; }
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
            $this->_log("  participants.tsv has all required columns — no enrichment needed");
            return $bidsPath;
        }

        // ── Stage a temp BIDS dir ─────────────────────────────────────────
        // Source is treated as read-only. Symlink every top-level item
        // except participants.tsv into a temp dir; write the enriched tsv
        // there as a real file. Bidsimport reads through the symlinks for
        // sub-* data and gets the enriched tsv from the temp dir.
        $stagePath = sys_get_temp_dir() . '/bids_import_stage_' . $this->runTimestamp;

        if (is_dir($stagePath)) {
            exec('rm -rf ' . escapeshellarg($stagePath));
        }
        if (!@mkdir($stagePath, 0755, true)) {
            $this->_writeError("ENRICH",
                "Cannot create staging directory: {$stagePath}"
                . " — check that " . sys_get_temp_dir() . " is writable"
            );
            return null;
        }
        $this->stagingDir = $stagePath;

        // Symlink every top-level item except participants.tsv
        $sourceDir = rtrim($bidsPath, '/');
        foreach (glob("{$sourceDir}/*") as $item) {
            $name = basename($item);
            if ($name === 'participants.tsv') {
                continue; // we'll write our own enriched copy
            }
            if (!@symlink($item, "{$stagePath}/{$name}")) {
                $this->_writeError("ENRICH",
                    "Failed to symlink {$name} into staging dir {$stagePath}"
                );
                return null;
            }
        }

        // Write enriched participants.tsv as a real file in the staging dir
        $stagedTsv = "{$stagePath}/participants.tsv";
        $out       = @fopen($stagedTsv, 'w');
        if (!$out) {
            $this->_writeError("ENRICH",
                "Cannot write enriched participants.tsv to {$stagedTsv}"
            );
            return null;
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

        // Persist a durable copy under processed/bids/ so the operator can
        // audit which cohort/site/sex values were applied without digging
        // through /tmp (which is cleaned at the end of the run).
        $processedDir = rtrim($this->projectPath, '/') . '/processed/bids';
        if (!is_dir($processedDir)) {
            @mkdir($processedDir, 0755, true);
        }
        $persistedTsv = "{$processedDir}/" . self::PROCESSED_TSV;
        if (!@copy($stagedTsv, $persistedTsv)) {
            // Non-fatal: bidsimport still gets the staged copy. Log so the
            // operator knows the audit copy is missing.
            $this->_log("    WARNING: Could not persist enriched copy to {$persistedTsv}"
                . " — bidsimport will still receive the staged tsv");
        }

        $this->_log("  ✓ participants.tsv enriched in temp staging dir ("
            . count($rows) . " rows, added: "
            . implode(', ', array_keys($addedCols)) . ")");
        $this->_log("    Staging dir         : {$stagePath}");
        $this->_log("    Persisted audit copy: {$persistedTsv}");
        $this->_log("    Source participants.tsv at {$tsvPath} is untouched");

        return $stagePath;
    }

    /**
     * Tear down the staging directory if one was created. Idempotent.
     * Called from _closeAllLogs() so every exit path triggers cleanup.
     */
    private function _cleanupStaging(): void
    {
        if ($this->stagingDir === null) {
            return;
        }
        if (is_dir($this->stagingDir)) {
            exec('rm -rf ' . escapeshellarg($this->stagingDir));
            $this->_log("  Staging directory removed: {$this->stagingDir}");
        }
        $this->stagingDir = null;
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

    /**
     * Pick the next sleep interval based on how long the job has been
     * running. Fast at the start, back off over time.
     *
     * @param float $elapsedSeconds Seconds since the job was launched
     */
    private function _adaptivePollInterval(float $elapsedSeconds): int
    {
        if ($elapsedSeconds < 30) {
            return self::POLL_INTERVAL_FAST;
        }
        if ($elapsedSeconds < 300) {
            return self::POLL_INTERVAL_MEDIUM;
        }
        return self::POLL_INTERVAL_SLOW;
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
        $this->scanInventory        = $this->_buildScanInventory($bidsPath);
        $this->_log("  Scans found: {$this->stats['scans_found']}");

        // Brief inventory line — a one-shot summary so the operator sees
        // what's about to be imported without scrolling. The full per-subject
        // breakdown is logged on SUCCESS and included in the email body.
        $totals = $this->scanInventory['totals'];
        $modParts = [];
        foreach ($totals['by_modality'] as $mod => $n) {
            $modParts[] = "{$mod}: {$n}";
        }
        $this->_log("  Inventory  : {$totals['subjects']} subject(s), "
            . "{$totals['sessions']} session(s), "
            . "{$totals['scans']} scan file(s)"
            . (empty($modParts) ? '' : " — " . implode(', ', $modParts)));

        // Load tracking — skip if already successfully imported
        // To reimport, delete the tracking file:
        //   {projectPath}/processed/bids/.bids_import_processed.json
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
                    . " {$projectPath}/processed/bids/.bids_import_processed.json");
                $this->stats['scans_skipped']    = $this->stats['scans_found'];
                $this->stats['skip_reason']      = 'already processed';
                $this->stats['last_imported_at'] = $lastTs;
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
        // (cohort, site alias, sex) from project.json candidate_defaults.
        // The source file is never modified — when enrichment is needed,
        // a temp staging dir is built with symlinks to source items plus
        // the enriched tsv, and bidsimport runs against that.
        $effectiveBidsPath = $this->_enrichParticipantsTsv($bidsPath);
        if ($effectiveBidsPath === null) {
            $this->stats['scans_failed'] = $this->stats['scans_found'];
            $this->sendNotification(false);
            $this->_closeAllLogs();
            return $this->stats;
        }

        // Launch async bidsimport
        $this->_log("  Launching bidsimport ...");
        $launch = $this->_launchBidsImport($effectiveBidsPath, $options);

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
            . " polling adaptively (2s → 10s → 30s) ...");

        // Poll loop — adaptive interval: fast at start so we catch jobs
        // that complete in seconds (SPM deletes its stdout/stderr temp
        // files on exit 0, so we need at least one poll where the files
        // still exist), backing off as elapsed time grows.
        $t0           = microtime(true);
        $pollStart    = $t0;
        $pollFailures = 0;
        $wasRunning   = false;
        $firstPoll    = true;

        while (true) {
            $elapsedNow = microtime(true) - $t0;
            if ($firstPoll) {
                $firstPoll = false;
                sleep(2); // first peek quickly — job may already be done
            } else {
                sleep($this->_adaptivePollInterval($elapsedNow));
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

            // Capture cumulative stdout from this poll into our own log
            // file BEFORE acting on terminal state. JobStatus inlines the
            // file contents on every poll (RUNNING + SUCCESS + ERROR) so
            // that by the time SPM cleans up /tmp on exit-0 completion,
            // we already have a durable copy on the pipeline's filesystem.
            // This is what makes per-file attribution survive SPM's
            // truncation: the parser reads our captured log instead of
            // SPM's progress tail.
            if (!empty($status['stdout'])) {
                $this->_captureScriptOutput((string)$status['stdout']);
            }

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
                    $this->_logInventoryDetails();
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
                // Debug: log the SPM response shape so we can see what
                // fields are populated. This is one-shot per run. Helps
                // identify which key carries the disk-backed log path
                // for the full bids_import.py stdout.
                $this->logger->debug("SPM job-status response keys: "
                    . implode(', ', array_keys($status)));
                foreach ($status as $key => $value) {
                    if (in_array($key, ['stdout', 'stderr', 'output', 'log', 'progress'], true)) {
                        $len = is_string($value) ? strlen($value) : 0;
                        $this->logger->debug("  '{$key}' = {$len} chars"
                            . ($len > 0 ? " (preview: " . substr((string)$value, 0, 80) . "...)" : ''));
                    } elseif (is_string($value) && strlen($value) < 500) {
                        $this->logger->debug("  '{$key}' = " . $value);
                    } elseif (is_scalar($value)) {
                        $this->logger->debug("  '{$key}' = " . (string)$value);
                    }
                }

                // SPM exit 0 only tells us the python script ran to completion —
                // not that it actually imported anything. Parse its stdout for
                // the summary line so we can distinguish: real success (files
                // imported), no-op (all already-registered), and partial
                // failure (script ran but some files errored on hash collision,
                // bad data, etc.).
                //
                // SPM's JobStatus endpoint returns inline stdout only on ERROR
                // state. On SUCCESS it returns just a `stdout_file` path with
                // an empty stdout field. _extractScriptOutput reads the file
                // directly so we get the full output regardless of SPM state,
                // which is what makes per-file ERROR attribution work.
                $progress = $this->_extractScriptOutput($status);
                $this->_parseScriptCounts($progress);

                $errored  = $this->stats['files_errored']  ?? 0;
                $imported = $this->stats['files_imported'] ?? 0;

                if ($errored > 0) {
                    // SUMMARY status — pipeline ran successfully end-to-end,
                    // but some individual files couldn't be imported (hash
                    // collision, empty file, etc). These are NOT pipeline
                    // failures: bidsimport ran cleanly, LORIS state is
                    // consistent, and tracking should still mark this a
                    // success so the next no-op run can correctly skip.
                    $this->_log("  ⚠ SUMMARY ({$elapsed}s) — {$imported} imported, "
                        . ($this->stats['files_ignored'] ?? 0) . " already-registered, "
                        . "{$errored} file issue(s)");

                    // Last 10 lines of script output go straight into the
                    // run log so the operator sees what bidsimport was
                    // saying — same content SPM stores in exit_text and
                    // shows in its admin UI, no duplication needed.
                    //
                    // The dedicated script-output log file (separately
                    // populated by _captureScriptOutput during polling)
                    // holds the FULL bidsimport.py stdout when JobStatus
                    // inlined it. If polling didn't capture full output
                    // (very fast jobs, JobStatus change not yet
                    // deployed), no script-output log file is created —
                    // the truncated tail in the run log is enough on its
                    // own and we don't want a second file with the same
                    // 10 lines pretending to be "full output".
                    $this->_logScriptOutputTail($progress, 10);

                    $this->_markTracked($projectPath, $tracking, 'success',
                        "summary: {$imported} imported, {$errored} file issue(s) "
                        . "job_id={$jobId}");

                    $this->stats['scans_processed']  = $this->stats['scans_found'];
                    $this->stats['summary_status']   = true;
                    $this->stats['file_issue_count'] = $errored;
                    $this->stats['script_output_tail'] = $progress;

                    $this->_logInventoryDetails();
                    $this->sendNotification(true);
                    $this->_closeAllLogs();
                    return $this->stats;
                }

                $this->_log("  ✓ SUCCESS ({$elapsed}s)"
                    . ($imported > 0
                        ? " — {$imported} file(s) imported"
                        : " — all files already registered (no-op)"));
                $this->_markTracked($projectPath, $tracking, 'success');
                $this->stats['scans_processed'] = $this->stats['scans_found'];
                $this->_logInventoryDetails();
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

    /**
     * Walk the BIDS tree and produce a per-subject / per-session /
     * per-modality count of NIfTI scan files, used by both the run-log
     * detailed summary and the email notification body.
     *
     * Counts only `.nii` and `.nii.gz` files — JSON sidecars, .bvec, .bval,
     * and .tsv files don't count as scans (they're metadata for scans).
     *
     * Modality directories follow standard BIDS naming: anat, func, dwi,
     * fmap, perf, pet, meg, eeg, ieeg, micr, beh, nirs. Anything found
     * under any other subdir name is still counted under that name.
     *
     * @return array{
     *   subjects: array<string, array{sessions: array<string, array<string,int>>, modalities?: array<string,int>}>,
     *   totals:   array{subjects:int, sessions:int, scans:int, by_modality: array<string,int>}
     * }
     */
    private function _buildScanInventory(string $bidsPath): array
    {
        $inventory = [
            'subjects' => [],
            'totals'   => [
                'subjects'    => 0,
                'sessions'    => 0,
                'scans'       => 0,
                'by_modality' => [],
            ],
        ];

        $subjectDirs = glob("{$bidsPath}/sub-*", GLOB_ONLYDIR) ?: [];
        sort($subjectDirs);

        foreach ($subjectDirs as $subjectDir) {
            $pscid    = preg_replace('/^sub-/', '', basename($subjectDir));
            $sessions = glob("{$subjectDir}/ses-*", GLOB_ONLYDIR) ?: [];
            sort($sessions);

            $entry = ['sessions' => []];

            if (empty($sessions)) {
                // No sessions — modality dirs sit directly under the subject
                $modCounts           = $this->_countModalitiesIn($subjectDir);
                $entry['modalities'] = $modCounts;
                $this->_addModCountsToTotals($inventory, $modCounts);
            } else {
                foreach ($sessions as $sessionDir) {
                    $sesLabel = preg_replace('/^ses-/', '', basename($sessionDir));
                    $modCounts = $this->_countModalitiesIn($sessionDir);
                    $entry['sessions'][$sesLabel] = $modCounts;
                    $this->_addModCountsToTotals($inventory, $modCounts);
                    $inventory['totals']['sessions']++;
                }
            }

            $inventory['subjects'][$pscid] = $entry;
            $inventory['totals']['subjects']++;
        }

        return $inventory;
    }

    /**
     * Count `.nii` and `.nii.gz` files per modality subdir under $parent.
     * Returns ['anat' => N, 'func' => N, ...] sorted alphabetically; a
     * modality with zero scans is omitted.
     */
    private function _countModalitiesIn(string $parent): array
    {
        $counts  = [];
        $modDirs = glob("{$parent}/*", GLOB_ONLYDIR) ?: [];

        foreach ($modDirs as $modDir) {
            $modName  = basename($modDir);
            $niiFiles = array_merge(
                glob("{$modDir}/*.nii.gz") ?: [],
                glob("{$modDir}/*.nii")    ?: []
            );
            if (!empty($niiFiles)) {
                $counts[$modName] = count($niiFiles);
            }
        }

        ksort($counts);
        return $counts;
    }

    /**
     * Roll one modality-count map up into the inventory totals.
     * Mutates $inventory['totals'] in place.
     */
    private function _addModCountsToTotals(array &$inventory, array $modCounts): void
    {
        foreach ($modCounts as $mod => $n) {
            $inventory['totals']['scans'] += $n;
            $inventory['totals']['by_modality'][$mod] =
                ($inventory['totals']['by_modality'][$mod] ?? 0) + $n;
        }
    }

    /**
     * Pull the fullest available script output from SPM's job-status response.
     *
     * SPM's JobStatus endpoint behavior (server_processes_manager):
     *   - On ERROR state: returns `stdout` and `stderr` inline (full content)
     *     plus `stdout_file` and `stderr_file` paths.
     *   - On SUCCESS state: returns only the file paths, NOT the inline content
     *     (an explicit optimization in the endpoint to avoid large payloads).
     *   - `progress` is always present but contains only the truncated tail.
     *
     * That asymmetry is why per-file ERROR lines disappear on partial-success
     * runs — they're in the stdout_file on disk, but never make it into the
     * response body. This method reads the file directly when SPM gives us
     * the path but no inline content.
     *
     * Strategy:
     *   1. Use inline `stdout`/`stderr` if present (SPM filled them on ERROR).
     *   2. Otherwise read `stdout_file` from disk directly (SUCCESS path).
     *   3. Fall back to `progress` tail only if neither works.
     *
     * Returns whatever string is found, or '' if nothing was available.
     */
    private function _extractScriptOutput(array $status): string
    {
        // 1. Locally-captured cumulative log — populated incrementally by
        //    _captureScriptOutput() on every poll. This is the most
        //    complete source: it has every byte the script wrote to
        //    stdout across the full job lifetime, even after SPM's
        //    /tmp cleanup. Reading from disk (not from the in-flight
        //    response) ensures we get the final-poll bytes too.
        if ($this->scriptOutputPath !== null
            && is_readable($this->scriptOutputPath)
            && filesize($this->scriptOutputPath) > 0
        ) {
            // Flush any open handle so file_get_contents returns the
            // bytes we just wrote in this same poll cycle.
            if ($this->scriptOutputFh !== null) {
                @fflush($this->scriptOutputFh);
            }
            $contents = @file_get_contents($this->scriptOutputPath);
            if ($contents !== false && strlen($contents) > 0) {
                return $contents;
            }
        }

        // 2. Inline stdout from this single poll's JobStatus response.
        //    Used when the local log isn't usable (filesystem error,
        //    very early failure before any poll captured output).
        foreach (['stdout', 'stderr', 'output', 'log'] as $key) {
            if (!empty($status[$key])) {
                return (string)$status[$key];
            }
        }

        // 3. Disk-backed log paths from SPM. May be unreadable post-
        //    cleanup but worth one try.
        foreach (['stdout_file', 'stderr_file'] as $key) {
            if (empty($status[$key]) || !is_string($status[$key])) {
                continue;
            }
            $path = $status[$key];
            if (!is_readable($path)) {
                continue;
            }
            $contents = @file_get_contents($path);
            if ($contents === false || strlen($contents) === 0) {
                continue;
            }
            return $contents;
        }

        // 4. Truncated tail — last resort. Has the summary count line
        //    but typically misses per-file ERROR lines.
        return (string)($status['progress'] ?? '');
    }

    /**
     * Parse the python script's summary line out of SPM's stdout/progress.
     *
     * The bids_import.py script prints exactly this line near the end on
     * every run (success or partial failure), regardless of overall exit
     * code:
     *
     *   "Processed N MRI files, including X imported files, Y ignored files,
     *    and Z errors."
     *
     * Populates $this->stats['files_total'], 'files_imported', 'files_ignored',
     * 'files_errored'. Leaves them null if the line isn't found — caller
     * should fall through to the SPM-exit-code-based decision.
     */
    private function _parseScriptCounts(string $output): void
    {
        // Pattern A — the script ran the import loop to completion:
        //   "Processed N MRI files, including X imported files, Y ignored
        //    files, and Z errors."
        if (preg_match(
            '/Processed\s+(\d+)\s+MRI\s+files,\s+including\s+(\d+)\s+imported\s+files,\s+(\d+)\s+ignored\s+files,\s+and\s+(\d+)\s+errors/i',
            $output,
            $m
        )) {
            $this->stats['files_total']    = (int) $m[1];
            $this->stats['files_imported'] = (int) $m[2];
            $this->stats['files_ignored']  = (int) $m[3];
            $this->stats['files_errored']  = (int) $m[4];

            $this->_log("  Script reported: {$m[1]} total, {$m[2]} imported,"
                . " {$m[3]} ignored, {$m[4]} errors");
        }
        // Pattern B — the script aborted before the import loop because
        // the BIDS validation stage flagged problems:
        //   "ERROR: Found N errors while checking BIDS subjects and sessions.
        //    No candidate or session has been created."
        // This is a genuine script-level abort. We didn't process any files,
        // so files_total/imported/ignored stay null, but files_errored is
        // populated so the SUCCESS handler can route to the failure path.
        elseif (preg_match(
            '/ERROR:\s+Found\s+(\d+)\s+errors\s+while\s+checking\s+BIDS\s+subjects/i',
            $output,
            $m
        )) {
            $this->stats['files_errored'] = (int) $m[1];
            $this->_log("  Script aborted before import: {$m[1]} BIDS validation error(s)"
                . " — no candidates or sessions created.");
        }

        // Build per-failure attribution from whatever ERROR lines the script
        // emitted. Runs unconditionally so the failures array is always
        // accurate to what the script said, regardless of which summary
        // pattern (or none) appeared.
        $this->_parseScriptFailures($output);
    }

    /**
     * Walk the python script's stdout linearly to attribute each per-file
     * ERROR to the subject/session it belongs to.
     *
     * The script prints a context header before each subject's files:
     *
     *   Importing files for subject 'FDGP0000045' and session '01'.
     *   Importing MRI file 'sub-FDGP0000045_ses-01_T1w.nii.gz'... (15 / 32)
     *   ERROR: Error while importing MRI file '...'. Error message:
     *   File with hash '...' already present in the database.
     *   Skipping.
     *
     * Strategy: scan top-to-bottom, update $currentPscid/$currentSession
     * whenever a header is seen, and on each ERROR line capture the
     * filename it mentions plus the next non-blank line as the reason.
     */
    private function _parseScriptFailures(string $output): void
    {
        $lines = preg_split('/\R/', $output);
        $currentPscid    = '';
        $currentSession  = '';
        $failures        = [];
        $scriptErrs      = []; // dedupe verbatim script ERROR messages

        for ($i = 0, $n = count($lines); $i < $n; $i++) {
            $line = $lines[$i];

            // Subject/session context header
            if (preg_match(
                "/Importing files for subject '([^']+)' and session '([^']+)'/",
                $line,
                $m
            )) {
                $currentPscid   = $m[1];
                $currentSession = $m[2];
                continue;
            }

            // Per-file error: "ERROR: Error while importing MRI file 'FOO'."
            if (preg_match(
                "/ERROR: Error while importing MRI file '([^']+)'/",
                $line,
                $m
            )) {
                $file   = $m[1];
                $reason = '';
                for ($j = $i + 1; $j < min($i + 5, $n); $j++) {
                    $next = trim($lines[$j]);
                    if ($next === '' || $next === 'Skipping.') continue;
                    if (stripos($next, 'Error message') !== false) continue;
                    $reason = $next;
                    break;
                }
                $failures[] = [
                    'pscid'   => $currentPscid   ?: '(unknown)',
                    'session' => $currentSession ?: '(unknown)',
                    'file'    => $file,
                    'reason'  => $reason ?: '(no reason captured)',
                ];
                continue;
            }

            // Other ERROR lines from the script. The script tells us what's
            // wrong — surface its message verbatim. Examples:
            //   "ERROR: No 'cohort' column found in the BIDS participants.tsv..."
            //   "ERROR: No 'site' column found in the BIDS participants.tsv..."
            //   "ERROR: Cannot connect to LORIS database"
            //   "ERROR: Found N errors while checking BIDS subjects..."
            //
            // Dedupe by message (the script repeats some errors per subject).
            // The rollup line "Found N errors while checking..." is captured
            // here too — useful as confirmation the script aborted at the
            // validation stage.
            if (preg_match("/^ERROR:\s*(.+)$/", trim($line), $m)) {
                $errMsg = trim($m[1]);
                if (stripos($errMsg, 'while importing MRI file') !== false) {
                    continue; // already captured above
                }
                $scriptErrs[$errMsg] = ($scriptErrs[$errMsg] ?? 0) + 1;
            }
        }

        // Emit one failure entry per distinct script ERROR message. These
        // are flagged as the script reported them — not glossed over with
        // a "(setup error)" label. The operator sees: "the script said X."
        foreach ($scriptErrs as $msg => $count) {
            $detail = $count > 1 ? " (reported {$count} times)" : "";
            $failures[] = [
                'pscid'   => '(script error)',
                'session' => '',
                'file'    => '',
                'reason'  => "{$msg}{$detail}",
            ];
        }

        $this->stats['failures'] = $failures;
    }

    /**
     * Log per-candidate file issues to the run log. Called from the
     * SUMMARY branch so the operator sees which subjects need attention,
     * grouped by candidate-session, with reason for each file.
     */
    /**
     * Ensure the script-output log file ends up with everything we know
     * about the script's output, regardless of where it came from.
     *
     * If JobStatus inlined stdout during polling, _captureScriptOutput
     * already populated the log file as the job progressed. In that case
     * this function is a no-op. If the captured log is empty (because
     * JobStatus only ever returned the truncated `progress` tail —
     * happens for very fast jobs or when the server-side change isn't
     * deployed), this function writes the truncated tail to the log so
     * the operator has at least that on disk in the same place they'd
     * expect to find the full output.
     *
     * The header notes which case applied, so the operator can tell at a
     * glance whether they're looking at full bids_import.py stdout or
     * just SPM's tail-only preview.
     */
    private function _persistScriptOutput(string $fallback): void
    {
        if ($this->scriptOutputPath === null) {
            return;
        }

        // If polling already populated the log file, leave it alone — it
        // has the most complete view.
        if (is_file($this->scriptOutputPath)
            && filesize($this->scriptOutputPath) > 0
        ) {
            return;
        }
        if (trim($fallback) === '') {
            return;
        }

        // Open lazily — same pattern as _captureScriptOutput so a 0-byte
        // file isn't created when we have nothing to write.
        if ($this->scriptOutputFh === null) {
            $this->scriptOutputFh = @fopen($this->scriptOutputPath, 'a');
            if (!$this->scriptOutputFh) {
                return;
            }
            $sep = str_repeat('=', 72);
            fwrite($this->scriptOutputFh,
                "{$sep}\n BIDS Import Pipeline — Script Output\n"
                . " Run     : {$this->runTimestamp}\n"
                . " Started : " . date('Y-m-d H:i:s T') . "\n"
                . " Source  : SPM truncated job-status response (tail-only).\n"
                . "           The pipeline did not receive full stdout —\n"
                . "           contents below are limited to what SPM sent.\n{$sep}\n\n"
            );
            $this->_log("  Script output log: {$this->scriptOutputPath}"
                . " (truncated — SPM did not return full stdout)");
        }

        // Strip SPM's "{description} completed successfully.\n\nLast output:"
        // wrapper so the file holds raw script output, not the SPM frame.
        $cleaned = preg_replace('/^.*?Last output:\s*\n/s', '', $fallback, 1);
        fwrite($this->scriptOutputFh, $cleaned);
        $this->scriptOutputOffset = strlen($cleaned);
    }

    /**
     * Emit the last N lines of the script's output to the run log so the
     * operator sees the actual bidsimport context that surrounded any
     * errors. This is the at-a-glance "where did the error happen" view
     * alongside the SUMMARY line — no parsing, no inference, just the
     * script's own output verbatim.
     *
     * Source priority:
     *   1. The polling-captured script output log on disk (most complete
     *      when JobStatus inlines stdout).
     *   2. The string passed in ($progress) — typically the truncated tail
     *      from SPM's exit_text.
     */
    private function _logScriptOutputTail(string $progress, int $n): void
    {
        // Prefer the captured log file when it exists and has content.
        $output = '';
        if ($this->scriptOutputPath !== null
            && is_file($this->scriptOutputPath)
            && filesize($this->scriptOutputPath) > 0
        ) {
            if ($this->scriptOutputFh !== null) {
                @fflush($this->scriptOutputFh);
            }
            $output = (string) @file_get_contents($this->scriptOutputPath);
        }
        if ($output === '') {
            $output = $progress;
        }
        if (trim($output) === '') {
            return;
        }

        // Strip the "Last output:" wrapper SPM adds in exit_text so we
        // don't double-prefix it in our run log.
        $output = preg_replace('/^.*?Last output:\s*\n/s', '', $output, 1);

        $lines = preg_split('/\R/', rtrim($output));
        $tail  = array_slice($lines, -$n);

        $this->_log("");
        $this->_log("  ═══ SCRIPT OUTPUT (last {$n} line(s)) ═══");
        foreach ($tail as $line) {
            $this->_log("    {$line}");
        }
        if ($this->scriptOutputPath !== null
            && is_file($this->scriptOutputPath)
            && filesize($this->scriptOutputPath) > 0
        ) {
            $this->_log("");
            $this->_log("  Full script output captured at:");
            $this->_log("    {$this->scriptOutputPath}");
        }
        $this->_log("  ═════════════════════════════════════════════");
    }

    /**
     * Render the last N lines of script output as an email-body block.
     * Mirrors _logScriptOutputTail's logic but returns a string for the
     * caller to splice into the email body.
     *
     * Source priority:
     *   1. The polling-captured script output log on disk (most complete
     *      when JobStatus inlined stdout during polling).
     *   2. $this->stats['script_output_tail'] — captured at SUMMARY time
     *      from SPM's truncated progress field; always non-empty when the
     *      script reported any output at all.
     *
     * Returns '' only when there's literally no output to show.
     */
    private function _renderScriptOutputTailForEmail(int $n): string
    {
        $output = '';
        if ($this->scriptOutputPath !== null
            && is_file($this->scriptOutputPath)
            && filesize($this->scriptOutputPath) > 0
        ) {
            if ($this->scriptOutputFh !== null) {
                @fflush($this->scriptOutputFh);
            }
            $output = (string) @file_get_contents($this->scriptOutputPath);
        }
        if (trim($output) === '') {
            // Fall back to the truncated progress tail captured from SPM.
            $output = (string) ($this->stats['script_output_tail'] ?? '');
        }
        if (trim($output) === '') {
            return '';
        }

        // Strip SPM's "{description} completed successfully.\n\nLast output:"
        // wrapper so the output looks like raw script output, not the SPM
        // frame. Same regex used in _logScriptOutputTail.
        $output = preg_replace('/^.*?Last output:\s*\n/s', '', $output, 1);
        $lines  = preg_split('/\R/', rtrim($output));
        $tail   = array_slice($lines, -$n);

        $body  = "Script output (last {$n} line(s)):\n"
            . str_repeat('-', 40) . "\n";
        foreach ($tail as $line) {
            $body .= "  {$line}\n";
        }
        return $body;
    }


    /**
     * Emit the per-subject inventory to the run log via _log() line by line.
     * Called from each SUCCESS path so the operator can see exactly which
     * subjects, sessions, and modalities were sent to bidsimport.
     */
    private function _logInventoryDetails(): void
    {
        $this->_log("");
        $this->_log("  ═══ SCAN INVENTORY (sent to bidsimport) ═══");
        foreach ($this->_renderInventoryLines('  ') as $line) {
            $this->_log($line);
        }
        $this->_log("  ═════════════════════════════════════════════");
    }

    /**
     * Render the inventory in compact form for the email body — totals
     * plus a one-line-per-subject list of CandID and session labels.
     * The full per-modality breakdown stays in the run log only, since
     * email recipients want a quick "what landed for whom" summary.
     */
    private function _renderInventoryForEmail(): string
    {
        if (empty($this->scanInventory['subjects'])) {
            return '';
        }
        $totals = $this->scanInventory['totals'];

        // Compact totals line
        $modParts = [];
        ksort($totals['by_modality']);
        foreach ($totals['by_modality'] as $mod => $n) {
            $modParts[] = "{$mod}: {$n}";
        }
        $modText = empty($modParts) ? '' : ' — ' . implode(', ', $modParts);

        $body  = "Scan inventory:\n" . str_repeat('-', 40) . "\n";
        $body .= "{$totals['subjects']} subject(s), {$totals['sessions']} session(s), "
            . "{$totals['scans']} scan file(s){$modText}\n\n";

        // Per-subject: just CandID + session labels (no modalities here —
        // those are in the run log via _logInventoryDetails()).
        $body .= "Subjects:\n";
        foreach ($this->scanInventory['subjects'] as $pscid => $entry) {
            if (isset($entry['modalities'])) {
                // No-sessions BIDS layout
                $body .= "  sub-{$pscid} (no sessions)\n";
                continue;
            }
            $sesLabels = array_keys($entry['sessions']);
            if (empty($sesLabels)) {
                $body .= "  sub-{$pscid} (no sessions)\n";
                continue;
            }
            $sesList = array_map(fn($s) => "ses-{$s}", $sesLabels);
            $body .= "  sub-{$pscid} — " . implode(', ', $sesList) . "\n";
        }

        return $body;
    }

    /**
     * Render the inventory as plain text. Used for both run log (line by
     * line via _log) and email body (single string). $indent is prepended
     * to every line.
     */
    private function _renderInventoryLines(string $indent = ''): array
    {
        if (empty($this->scanInventory) || empty($this->scanInventory['subjects'])) {
            return ["{$indent}(no subjects found)"];
        }

        $inv    = $this->scanInventory;
        $totals = $inv['totals'];
        $lines  = [];

        // Top-level totals
        $modSummary = [];
        ksort($totals['by_modality']);
        foreach ($totals['by_modality'] as $mod => $n) {
            $modSummary[] = "{$mod}: {$n}";
        }
        $modText = empty($modSummary) ? '(none)' : implode(', ', $modSummary);

        $lines[] = "{$indent}Subjects   : {$totals['subjects']}";
        $lines[] = "{$indent}Sessions   : {$totals['sessions']}";
        $lines[] = "{$indent}Scan files : {$totals['scans']} ({$modText})";
        $lines[] = "{$indent}";
        $lines[] = "{$indent}Per-subject breakdown:";

        foreach ($inv['subjects'] as $pscid => $entry) {
            $lines[] = "{$indent}  sub-{$pscid}";

            // Subject without sessions
            if (isset($entry['modalities'])) {
                $mods = $entry['modalities'];
                if (empty($mods)) {
                    $lines[] = "{$indent}    (no scans)";
                } else {
                    $parts = [];
                    foreach ($mods as $mod => $n) {
                        $parts[] = "{$mod} ({$n})";
                    }
                    $lines[] = "{$indent}    " . implode(', ', $parts);
                }
                continue;
            }

            // Subject with sessions
            if (empty($entry['sessions'])) {
                $lines[] = "{$indent}    (no sessions)";
                continue;
            }
            foreach ($entry['sessions'] as $sesLabel => $mods) {
                if (empty($mods)) {
                    $lines[] = "{$indent}    ses-{$sesLabel}: (no scans)";
                    continue;
                }
                $parts = [];
                foreach ($mods as $mod => $n) {
                    $parts[] = "{$mod} ({$n})";
                }
                $lines[] = "{$indent}    ses-{$sesLabel}: " . implode(', ', $parts);
            }
        }

        return $lines;
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
        if ($this->notificationSent) {
            $this->_log("  Notification already sent this run — skipping duplicate");
            return;
        }

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

        $isNoOp    = ($this->stats['skip_reason'] !== ''
            && $this->stats['scans_skipped'] === $this->stats['scans_found']);
        $isSummary = !empty($this->stats['summary_status']);

        if ($isNoOp) {
            $status = 'ALREADY PROCESSED';
        } elseif ($isSummary) {
            // SUMMARY: pipeline ran cleanly but had file-level issues.
            // Subject includes the issue count so operator can triage at
            // inbox-glance time without opening the email.
            $issueCount = $this->stats['file_issue_count'] ?? 0;
            $status     = "SUMMARY ({$issueCount} file issue"
                . ($issueCount === 1 ? '' : 's') . ')';
        } else {
            $status = $success ? 'SUCCESS' : 'FAILED';
        }
        $subject = "{$status}: {$projectName} BIDS Import Pipeline";
        $body    = $this->_buildEmailBody($success, $projectName);

        $this->_log("  Sending notification to: " . implode(', ', $emailsToSend));
        foreach ($emailsToSend as $to) {
            $this->notification->send($to, $subject, $body);
        }
        $this->notificationSent = true;
    }

    private function _buildEmailBody(bool $success, string $projectName): string
    {
        $s     = $this->stats;
        $isNoOp    = ($s['skip_reason'] !== '' && $s['scans_skipped'] === $s['scans_found']);
        $isSummary = !empty($s['summary_status']);

        if ($isNoOp) {
            $statusLine = 'ALREADY PROCESSED';
        } elseif ($isSummary) {
            $statusLine = 'SUMMARY — ' . ($s['file_issue_count'] ?? 0)
                . ' file issue(s) need review';
        } else {
            $statusLine = $success ? 'SUCCESS' : 'FAILED';
        }

        $body = "Project    : {$projectName}\n";
        $body .= "Run        : {$this->runTimestamp}\n";
        $body .= "Timestamp  : " . date('Y-m-d H:i:s') . "\n";
        $body .= "Status     : {$statusLine}\n";
        if ($isNoOp && !empty($s['last_imported_at'])) {
            $body .= "Last import: {$s['last_imported_at']}\n";
        }
        $body .= "\n";

        if ($s['job_id']) {
            $body .= "SPM Job    : job_id={$s['job_id']}"
                . ($s['pid'] ? " pid={$s['pid']}" : "") . "\n\n";
        }

        // Annotate the skipped count with its reason so "skipped 3" doesn't
        // look like an error or a mystery.
        $skipSuffix = ($s['scans_skipped'] > 0 && $s['skip_reason'] !== '')
            ? " ({$s['skip_reason']}"
            . (!empty($s['last_imported_at']) ? " on {$s['last_imported_at']}" : '')
            . ")"
            : '';

        $body .= "Statistics:\n";
        $body .= str_repeat('-', 40) . "\n";
        $body .= "Scans found     : {$s['scans_found']}\n";
        $body .= "Scans processed : {$s['scans_processed']}\n";
        $body .= "Scans skipped   : {$s['scans_skipped']}{$skipSuffix}\n";
        $body .= "Scans failed    : {$s['scans_failed']}\n";

        // File-level counts from the python script (bids_import.py). These
        // are NIfTI-file granularity, distinct from the subject-session
        // 'scans_*' counts above. Surfaces partial-failure runs where
        // some files imported and others didn't.
        if ($s['files_total'] !== null) {
            $body .= "\n";
            $body .= "File-level (from bids_import.py):\n";
            $body .= "  Total NIfTI files : {$s['files_total']}\n";
            $body .= "  Imported          : {$s['files_imported']}\n";
            $body .= "  Already in LORIS  : {$s['files_ignored']}\n";
            $body .= "  Errors            : {$s['files_errored']}\n";
        }

        // Per-subject breakdown of what was sent to bidsimport.
        // Built from the BIDS tree on disk — NIfTI scan files counted by
        // subject → session → modality. Same content as the run log's
        // SCAN INVENTORY block.
        if (!empty($this->scanInventory['subjects'])) {
            $body .= "\n" . $this->_renderInventoryForEmail();
        }

        // For SUMMARY runs, append the last 10 lines of script output
        // inline. Operator sees what bidsimport was saying without
        // opening the full script log. Per-subject attribution is
        // intentionally omitted — SPM's truncated stdout doesn't reliably
        // contain the ERROR lines we'd need, and the summary line + tail
        // give the operator everything they need at a glance. For full
        // detail they go to the script-output log file referenced in the
        // footer.
        if (!empty($s['summary_status'])) {
            $tail = $this->_renderScriptOutputTailForEmail(10);
            if ($tail !== '') {
                $body .= "\n" . $tail;
            }
        }

        if (!empty($s['errors'])) {
            $body .= "\nErrors:\n" . str_repeat('-', 40) . "\n";
            foreach ($s['errors'] as $err) {
                $body .= "- {$err}\n";
            }
        }

        if ($this->runLogPath)   $body .= "\nRun log    : {$this->runLogPath}\n";
        if ($this->errorLogPath) $body .= "Error log  : {$this->errorLogPath}\n";
        // Reference the script-output log only when it actually got
        // populated — empty/missing means JobStatus never returned inline
        // stdout (likely the server-side change isn't deployed yet) and
        // the operator would just hit a missing file.
        if ($this->scriptOutputPath
            && is_file($this->scriptOutputPath)
            && filesize($this->scriptOutputPath) > 0
        ) {
            $body .= "Script log : {$this->scriptOutputPath}\n";
        }

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