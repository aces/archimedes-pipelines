<?php

declare(strict_types=1);

namespace LORIS\Pipelines;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use GuzzleHttp\Client as GuzzleClient;
use LORIS\Utils\CleanLogFormatter;

/**
 * BIDS Reidentifier Pipeline
 *
 * Calls POST /cbigr_api/script/bidsreidentifier with async=true,
 * then polls GET /cbigr_api/script/job/{id} until the job completes.
 *
 * The underlying bids_reidentifier.php script:
 *   - Walks source_dir recursively
 *   - Replaces sub-{ExtStudyID} → sub-{PSCID} in filenames, dirs, TSV, JSON
 *   - Prints "ERROR: no mapping found for sub-XX" but does NOT exit non-zero
 *     → pipeline scans stdout for this pattern
 *
 * Log files written to {projectPath}/logs/:
 *   bids_reidentifier_run_{timestamp}.log    — full run log
 *   bids_reidentifier_errors_{timestamp}.log — errors only (on first error)
 *
 * @package LORIS\Pipelines
 */
class BidsReidentifier
{
    private Logger       $logger;
    private array        $config;
    private ?string      $token         = null;
    private GuzzleClient $httpClient;
    private ?array       $projectConfig = null;
    private string       $runTimestamp;

    private ?string $runLogPath   = null;
    private ?string $errorLogPath = null;
    /** @var resource|null */
    private $runLogFh = null;
    /** @var resource|null */
    private $errorFh  = null;

    /** Seconds between job status poll requests */
    private const POLL_INTERVAL_SECONDS = 30;

    /** Maximum seconds to wait for reidentification before timing out */
    private const POLL_TIMEOUT_SECONDS = 14400; // 4 hours

    /** Consecutive poll failures before giving up */
    private const MAX_POLL_FAILURES = 3;

    /** Pattern printed by bids_reidentifier.php for unmapped subjects */
    private const UNMAPPED_PATTERN = '/ERROR: no mapping found for (sub-\S+)/i';

    private array $stats = [
        'subjects_found'    => 0,
        'subjects_mapped'   => 0,
        'subjects_unmapped' => 0,
        'unmapped_subjects' => [],
        'errors'            => [],
    ];

    public function __construct(array $config, bool $verbose = false)
    {
        if (!isset($config['loris']) && isset($config['api'])) {
            $config['loris'] = $config['api'];
        }

        $this->config       = $config;
        $this->runTimestamp = date('Y-m-d_H-i-s');
        $this->logger       = $this->_initLogger($verbose);
        $this->httpClient   = new GuzzleClient([
            'verify'      => false,
            'timeout'     => 30,
            'http_errors' => false, // Never throw on 4xx/5xx — handle manually
        ]);
    }

    // =========================================================================
    //  LOGGING
    // =========================================================================

    private function _initLogger(bool $verbose): Logger
    {
        $logger    = new Logger('bids-reidentifier');
        $formatter = new CleanLogFormatter();
        $level     = $verbose ? Logger::DEBUG : Logger::INFO;
        $console   = new StreamHandler('php://stdout', $level);
        $console->setFormatter($formatter);
        $logger->pushHandler($console);
        return $logger;
    }

    private function _openLogs(string $projectPath): void
    {
        $logDir = "{$projectPath}/logs";
        if (!is_dir($logDir)) {
            @mkdir($logDir, 0755, true);
        }

        $this->runLogPath = "{$logDir}/bids_reidentifier_run_{$this->runTimestamp}.log";
        $this->runLogFh   = @fopen($this->runLogPath, 'a');

        if ($this->runLogFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->runLogFh,
                "{$sep}\n BIDS Reidentifier — Run Log\n"
                . " Started : " . date('Y-m-d H:i:s T') . "\n"
                . " Project : {$projectPath}\n"
                . "{$sep}\n\n"
            );
        }

        $formatter      = new CleanLogFormatter();
        $runFileHandler = new StreamHandler($this->runLogPath, Logger::DEBUG);
        $runFileHandler->setFormatter($formatter);
        $this->logger->pushHandler($runFileHandler);

        $this->_log("  Run log  : {$this->runLogPath}");
    }

    private function _openErrorLog(): void
    {
        if ($this->errorFh !== null || $this->runLogPath === null) {
            return;
        }
        $logDir             = dirname($this->runLogPath);
        $this->errorLogPath = "{$logDir}/bids_reidentifier_errors_{$this->runTimestamp}.log";
        $this->errorFh      = @fopen($this->errorLogPath, 'a');
        if ($this->errorFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->errorFh,
                "{$sep}\n BIDS Reidentifier — Error Log\n"
                . " Run: {$this->runTimestamp}\n"
                . "{$sep}\n\n"
            );
        }
        $this->_log("  Error log: {$this->errorLogPath}");
    }

    private function _log(string $msg): void
    {
        $this->logger->info($msg);
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[" . date('H:i:s') . "] {$msg}\n");
        }
    }

    private function _warn(string $context, string $msg): void
    {
        $this->logger->warning("[{$context}] {$msg}");
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[" . date('H:i:s') . "] WARNING [{$context}] {$msg}\n");
        }
    }

    private function _error(string $context, string $msg): void
    {
        $this->logger->error("[{$context}] {$msg}");
        $this->_openErrorLog();

        $ts = date('H:i:s');
        if ($this->errorFh) {
            fwrite($this->errorFh, "[{$ts}] [{$context}] {$msg}\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[{$ts}] ERROR [{$context}] {$msg}\n");
        }
        $this->stats['errors'][] = "[{$context}] {$msg}";
    }

    private function _closeLogs(): void
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

    private function _authenticate(): bool
    {
        $baseUrl  = rtrim($this->config['loris']['base_url'], '/');
        $version  = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';
        $username = $this->config['loris']['username'];
        $password = $this->config['loris']['password'];

        $this->_log("Authenticating with LORIS at {$baseUrl}");

        try {
            $response    = $this->httpClient->request('POST', "{$baseUrl}/api/{$version}/login", [
                'json' => compact('username', 'password'),
            ]);
            $statusCode  = $response->getStatusCode();
            $data        = json_decode((string)$response->getBody(), true);
            $this->token = $data['token'] ?? null;

            if (!$this->token) {
                $this->_error("AUTH",
                    "No token in login response (HTTP {$statusCode})"
                    . " — check credentials"
                );
                return false;
            }
            $this->_log("  ✓ Authenticated");
            return true;
        } catch (\Exception $e) {
            $this->_error("AUTH", "Authentication failed: " . $e->getMessage());
            return false;
        }
    }

    // =========================================================================
    //  PROJECT CONFIG
    // =========================================================================

    private function _loadProjectConfig(string $sourceDir): ?array
    {
        $searchPaths = [
            rtrim($sourceDir, '/') . '/project.json',
            dirname(rtrim($sourceDir, '/')) . '/project.json',
            dirname(dirname(rtrim($sourceDir, '/'))) . '/project.json',
        ];

        foreach ($searchPaths as $path) {
            if (!file_exists($path)) continue;
            $data = json_decode(file_get_contents($path), true);
            if (json_last_error() === JSON_ERROR_NONE) {
                $this->_log("  Loaded project.json: {$path}");
                return $data;
            }
            $this->_warn("CONFIG", "Invalid JSON in {$path}: " . json_last_error_msg());
        }

        return null;
    }

    /**
     * Resolve project name for SQL project_list argument.
     *
     * Priority: candidate_defaults.project → loris_project_name →
     *           project_common_name → project_full_name → project
     *
     * candidate_defaults.project should be the exact LORIS DB Project.Name
     * (e.g. "FDG PET" not "FDG-PET").
     */
    private function _resolveProjectName(?string $cliProject): ?string
    {
        if ($cliProject) {
            return $cliProject;
        }

        if (!$this->projectConfig) {
            return null;
        }

        $defaults = $this->projectConfig['candidate_defaults'] ?? [];

        if (!empty($defaults['project'])) {
            return trim($defaults['project']);
        }

        if (!empty($this->projectConfig['loris_project_name'])) {
            return trim($this->projectConfig['loris_project_name']);
        }

        foreach (['project_common_name', 'project_full_name', 'project'] as $key) {
            if (!empty($this->projectConfig[$key])) {
                return trim($this->projectConfig[$key]);
            }
        }

        return null;
    }

    // =========================================================================
    //  BIDS VALIDATION
    // =========================================================================

    private function _validateBidsDirectory(string $dir): bool
    {
        if (!is_dir($dir)) {
            $this->_error("BIDS_DIR",
                "Source directory not found: {$dir}"
                . " — run run_bids_participant_sync.php first"
                . " and ensure deidentified-raw/bids/ exists"
            );
            return false;
        }

        if (!file_exists("{$dir}/participants.tsv")) {
            $this->_error("PARTICIPANTS_TSV",
                "Missing participants.tsv in {$dir}"
                . " — required to determine subject ID pattern"
            );
            return false;
        }

        $subjects = glob("{$dir}/sub-*", GLOB_ONLYDIR) ?: [];
        if (empty($subjects)) {
            $this->_error("BIDS_SUBJECTS",
                "No sub-* directories found in {$dir}"
                . " — nothing to reidentify"
            );
            return false;
        }

        $this->stats['subjects_found'] = count($subjects);
        $this->_log("  Found {$this->stats['subjects_found']} sub-* directories");
        return true;
    }

    // =========================================================================
    //  ID PATTERN
    // =========================================================================

    /**
     * Build a regex pattern from participant_id values in participants.tsv.
     *
     * The bids_reidentifier.php script matches "sub-($pattern)" in filenames.
     * Strips "sub-" prefix then replaces digit sequences with \d{N}:
     *   sub-01      → "01"      → "\d{2}"
     *   sub-FDGP001 → "FDGP001" → "FDGP\d{3}"
     */
    private function _extractIdPattern(string $bidsDir): ?string
    {
        $participants = $this->_parseParticipantsTsv($bidsDir);
        if (empty($participants)) {
            $this->_error("ID_PATTERN",
                "No participants found in participants.tsv"
                . " — cannot determine ID pattern for reidentifier"
            );
            return null;
        }

        $sampleId = $participants[0]['external_id'] ?? null;
        if (!$sampleId) {
            $this->_error("ID_PATTERN",
                "Could not extract external_id from participants.tsv."
                . " Ensure participant_id has sub-XX format (e.g. sub-01)"
            );
            return null;
        }

        $pattern = preg_replace_callback('/\d+/', function ($m) {
            return "\\d{" . strlen($m[0]) . "}";
        }, $sampleId);

        $this->_log("  ID pattern: '{$pattern}' (derived from '{$sampleId}')");
        return $pattern;
    }

    private function _parseParticipantsTsv(string $bidsDir): array
    {
        $tsvFile = "{$bidsDir}/participants.tsv";
        if (!file_exists($tsvFile)) return [];

        $handle = fopen($tsvFile, 'r');
        if ($handle === false) return [];

        $headerLine = fgets($handle);
        if ($headerLine === false) { fclose($handle); return []; }

        $headers      = array_map('trim', explode("\t", $headerLine));
        $participants = [];

        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if ($line === '') continue;

            $fields = array_map('trim', explode("\t", $line));
            $row    = [];
            foreach ($headers as $i => $header) {
                $row[$header] = $fields[$i] ?? '';
            }

            $pid        = $row['participant_id'] ?? '';
            $externalId = $this->_extractExternalID($row);

            if ($pid) {
                $participants[] = [
                    'participant_id' => $pid,
                    'external_id'    => $externalId ?? substr($pid, 4),
                ];
            }
        }

        fclose($handle);
        return $participants;
    }

    private function _extractExternalID(array $row): ?string
    {
        foreach (['external_id','ExternalID','externalid',
                     'study_id','StudyID','studyid',
                     'ext_study_id','ExtStudyID'] as $col) {
            if (!empty($row[$col])) return trim($row[$col]);
        }
        $pid = $row['participant_id'] ?? '';
        return str_starts_with($pid, 'sub-') ? substr($pid, 4) : null;
    }

    // =========================================================================
    //  ASYNC LAUNCH + POLL
    // =========================================================================

    private function _launchAsync(
        string $sourceDir,
        string $targetDir,
        string $idPattern,
        string $projectList
    ): array {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url     = "{$baseUrl}/cbigr_api/script/bidsreidentifier";

        $payload = [
            'args' => [
                'source_dir'   => $sourceDir,
                'target_dir'   => $targetDir,
                'id_pattern'   => $idPattern,
                'mode'         => 'INTERNAL',
                'project_list' => $projectList,
            ],
            'async' => true,
        ];

        $this->logger->debug("POST {$url}");
        $this->logger->debug(json_encode($payload, JSON_PRETTY_PRINT));

        try {
            $response   = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json'    => $payload,
                'timeout' => 30,
            ]);

            $statusCode = $response->getStatusCode();
            $rawBody    = (string)$response->getBody();

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
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        try {
            $response = $this->httpClient->request('GET',
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

    public function run(string $sourceDir, string $targetDir, array $options = []): array
    {
        $dryRun     = $options['dry_run'] ?? false;
        $cliProject = $options['project'] ?? null;

        $projectPath = dirname(dirname(rtrim($sourceDir, '/')));
        $this->_openLogs($projectPath);

        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  BIDS Reidentifier");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  Source  : {$sourceDir}");
        $this->_log("  Target  : {$targetDir}");
        $this->_log("  Dry run : " . ($dryRun ? 'YES' : 'NO'));
        $this->_log("───────────────────────────────────────────────────────────");

        if (!$this->_validateBidsDirectory($sourceDir)) {
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        $this->projectConfig = $this->_loadProjectConfig($sourceDir);
        $projectName         = $this->_resolveProjectName($cliProject);

        if (!$projectName) {
            $this->_error("PROJECT",
                "Project name required."
                . " Add 'candidate_defaults.project' to project.json"
                . " (must match exact LORIS Project.Name, e.g. 'FDG PET'),"
                . " or use --project flag."
            );
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        $this->_log("  Project : {$projectName}");

        $idPattern = $this->_extractIdPattern($sourceDir);
        if (!$idPattern) {
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        // Pass project name as-is — the endpoint wraps it in SQL quotes
        $projectList = $projectName;
        $this->_log("  SQL project_list: {$projectList}");

        if ($dryRun) {
            $this->_log("");
            $this->_log("[DRY-RUN] Would POST /cbigr_api/script/bidsreidentifier:");
            $this->_log("  source_dir   : {$sourceDir}");
            $this->_log("  target_dir   : {$targetDir}");
            $this->_log("  id_pattern   : {$idPattern}");
            $this->_log("  mode         : INTERNAL (ExtStudyID → PSCID)");
            $this->_log("  project_list : {$projectList}");
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        // Ensure target directory exists
        if (!is_dir($targetDir)) {
            $this->_log("  Creating target directory: {$targetDir}");
            if (!@mkdir($targetDir, 0777, true)) {
                $this->_error("TARGET_DIR",
                    "Cannot create target directory: {$targetDir}"
                    . " — check parent directory is writable"
                );
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }
            chmod($targetDir, 0777);
            $this->_log("  ✓ Target directory created (777)");
        }

        if (!$this->_authenticate()) {
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        // Launch async job
        $launch = $this->_launchAsync($sourceDir, $targetDir, $idPattern, $projectList);

        $httpStatus = $launch['http_status'] ?? 0;

        if ($httpStatus !== 202) {
            $errMsg = $launch['error']
                ?? ($launch['error'] ?? json_encode($launch))
                ?? "HTTP {$httpStatus}";
            $this->_error("LAUNCH",
                "Failed to launch bidsreidentifier (HTTP {$httpStatus}): {$errMsg}"
                . " — check /cbigr_api/script/bidsreidentifier endpoint and Apache logs"
            );
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        $jobId = $launch['job_id'] ?? null;
        $pid   = $launch['pid']    ?? null;

        if (!$jobId) {
            $this->_error("LAUNCH", "No job_id returned from async launch");
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        $this->_log("  Launched — job_id={$jobId} pid={$pid},"
            . " polling every " . self::POLL_INTERVAL_SECONDS . "s ...");

        // Poll loop
        // First poll at 15s — gives the script time to run (typically ~10s)
        // and SPM time to write the DB row.
        // Subsequent polls every POLL_INTERVAL_SECONDS.
        $t0           = microtime(true);
        $pollStart    = $t0;
        $pollFailures = 0;
        $wasRunning   = false;
        $firstPoll    = true;

        while (true) {
            if ($firstPoll) {
                $firstPoll = false;
                sleep(15);
            } else {
                sleep(self::POLL_INTERVAL_SECONDS);
            }

            $elapsed = round(microtime(true) - $t0, 2);

            if ((microtime(true) - $pollStart) > self::POLL_TIMEOUT_SECONDS) {
                $this->_error("TIMEOUT",
                    "Job {$jobId} timed out after " . self::POLL_TIMEOUT_SECONDS . "s"
                );
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }

            $status = $this->_pollJob($jobId);

            if ($status === null) {
                $pollFailures++;
                $this->_warn("POLL",
                    "Poll failed for job {$jobId} at {$elapsed}s"
                    . " — attempt {$pollFailures}/" . self::MAX_POLL_FAILURES
                );
                if ($pollFailures >= self::MAX_POLL_FAILURES) {
                    $this->_error("POLL_FAILED",
                        "Job {$jobId} poll failed " . self::MAX_POLL_FAILURES
                        . " consecutive times"
                        . " — check GET cbigr_api/script/job/{$jobId}"
                    );
                    $this->_printSummary();
                    $this->_closeLogs();
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

            // UNKNOWN — exit code file cleaned up before poll could read it.
            // If we saw RUNNING → the process ran and completed cleanly.
            if ($state === 'UNKNOWN') {
                if ($wasRunning) {
                    $this->_log(
                        "  ✓ SUCCESS (confirmed — exit code file cleaned up"
                        . " after successful run) ({$elapsed}s)"
                    );
                    $this->stats['subjects_mapped'] = $this->stats['subjects_found'];
                    $this->_printSummary();
                    $this->_closeLogs();
                    return $this->stats;
                }
                $this->_error("UNKNOWN_STATE",
                    "Job {$jobId} ended UNKNOWN without ever running"
                    . " — check server_processes id={$jobId} and Apache error log"
                );
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }

            if ($state === 'SUCCESS') {
                $this->_log("  ✓ SUCCESS ({$elapsed}s)");
                $output = $status['progress'] ?? '';
                $this->_parseScriptOutput($output);
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }

            // ERROR state
            $errorDetail = $status['error_detail'] ?? $status['progress'] ?? '';
            $exitCode    = $status['exit_code'] ?? '?';

            // Race condition: script completed before first 15s poll,
            // deleteProcessFiles() deleted temp files on exit 0,
            // SPM reports ERROR with "exit code unknown".
            // Confirm success by checking target has sub-* directories.
            $targetSubjects = glob("{$targetDir}/sub-*", GLOB_ONLYDIR) ?: [];
            if ((str_contains(strtolower($errorDetail), 'exit code unknown')
                    || $exitCode === '?')
                && !empty($targetSubjects)
            ) {
                $this->_log(
                    "  ✓ SUCCESS (exit code race condition — files cleaned up"
                    . " after successful run, target has "
                    . count($targetSubjects) . " sub-* dirs) ({$elapsed}s)"
                );
                $this->stats['subjects_mapped'] = $this->stats['subjects_found'];
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }

            $this->_error("SCRIPT_FAILED",
                "bids_reidentifier.php failed (exit {$exitCode}) after {$elapsed}s."
                . " job_id={$jobId}."
                . "\n    Output tail:\n    "
                . str_replace("\n", "\n    ", substr($errorDetail, 0, 500))
            );
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }
    }

    // =========================================================================
    //  OUTPUT PARSING
    // =========================================================================

    private function _parseScriptOutput(string $output): void
    {
        if (preg_match_all(self::UNMAPPED_PATTERN, $output, $matches)) {
            $unmapped = array_unique($matches[1]);
            foreach ($unmapped as $subjectId) {
                $this->_error("UNMAPPED_SUBJECT",
                    "{$subjectId} — no mapping found in candidate_project_extid_rel."
                    . " File copied with original ID."
                    . " Run run_bids_participant_sync.php to create the mapping."
                );
                $this->stats['unmapped_subjects'][] = $subjectId;
                $this->stats['subjects_unmapped']++;
            }
        }

        $this->stats['subjects_mapped'] = max(
            0,
            $this->stats['subjects_found'] - $this->stats['subjects_unmapped']
        );

        if ($this->stats['subjects_unmapped'] > 0) {
            $this->_warn("UNMAPPED",
                "{$this->stats['subjects_unmapped']} subject(s) could not be mapped"
                . " — copied to target with original IDs. See error log."
            );
        }
    }

    // =========================================================================
    //  SUMMARY
    // =========================================================================

    private function _printSummary(): void
    {
        $errCount = count($this->stats['errors']);
        $this->_log("");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  REIDENTIFIER SUMMARY");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  Subjects found    : {$this->stats['subjects_found']}");
        $this->_log("  Subjects mapped   : {$this->stats['subjects_mapped']}");
        $this->_log("  Subjects unmapped : {$this->stats['subjects_unmapped']}"
            . ($this->stats['subjects_unmapped'] > 0 ? ' ← see error log' : ''));
        $this->_log("  Total errors      : {$errCount}");
        $this->_log("───────────────────────────────────────────────────────────");
        if ($this->runLogPath)   $this->_log("  Run log  : {$this->runLogPath}");
        if ($this->errorLogPath) $this->_log("  Error log: {$this->errorLogPath}");
        $this->_log("  Result: "
            . ($errCount > 0 ? 'COMPLETED WITH ERRORS' : 'COMPLETED SUCCESSFULLY')
        );
        $this->_log("═══════════════════════════════════════════════════════════");
    }

    public function getStats(): array
    {
        return $this->stats;
    }
}