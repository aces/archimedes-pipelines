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
        // Populated when scans_skipped > 0 — explains why they were skipped
        // so the summary doesn't just say "skipped 3" with no context.
        'skip_reason'     => '',
        // Timestamp of the original import run that processed these scans
        // (when this run is a no-op because everything was already imported).
        'last_imported_at' => '',
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
                $this->_log("  ✓ SUCCESS ({$elapsed}s)");
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

        $isNoOp  = ($this->stats['skip_reason'] !== ''
            && $this->stats['scans_skipped'] === $this->stats['scans_found']);
        $status  = $isNoOp ? 'ALREADY PROCESSED' : ($success ? 'SUCCESS' : 'FAILED');
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
        $isNoOp = ($s['skip_reason'] !== '' && $s['scans_skipped'] === $s['scans_found']);

        if ($isNoOp) {
            $statusLine = 'ALREADY PROCESSED';
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

        // Per-subject breakdown of what was sent to bidsimport.
        // Built from the BIDS tree on disk — NIfTI scan files counted by
        // subject → session → modality. Same content as the run log's
        // SCAN INVENTORY block.
        if (!empty($this->scanInventory['subjects'])) {
            $body .= "\n" . $this->_renderInventoryForEmail();
        }

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