<?php
declare(strict_types=1);

/**
 * ARCHIMEDES DICOM Import Pipeline
 *
 * Scans <projectDir>/deidentified-raw/imaging/dicoms/ for study directories
 * and imports each via POST /cbigr_api/script/importdicomstudy endpoint.
 *
 * Logging:
 *   1. Console        → stdout (always)
 *   2. Run log        → logs/dicom/dicom_run_{timestamp}.log  (per run, all detail)
 *   3. Error log      → logs/dicom/dicom_errors_{timestamp}.log (per run, only if errors)
 *   4. Email          → success or failure notification with full summary
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

    /** Timestamp for this pipeline run — shared across log files */
    private string $runTimestamp;

    /** @var resource|null Error log — opened lazily on first error */
    private $errorFh = null;
    private ?string $errorLogPath = null;

    /** @var resource|null Run log — opened per project */
    private $runLogFh = null;
    private ?string $runLogPath = null;

    private ?string $logDir = null;

    private const TRACK_FILE = '.dicom_import_processed.json';

    // ── Tracking ─────────────────────────────────────────────────────

    /** Import results per study: studyName → {status, error, elapsed} */
    private array $importResults = [];

    /** Global stats across all projects in this run */
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
        $this->runTimestamp  = date('Y-m-d_H-i-s');

        $logLevel = $verbose ? Logger::DEBUG : Logger::INFO;
        $this->logger = new Logger('dicom_import');
        $formatter = new CleanLogFormatter();

        $console = new StreamHandler('php://stdout', $logLevel);
        $console->setFormatter($formatter);
        $this->logger->pushHandler($console);

        $this->http = new HttpClient([
            'base_uri'    => rtrim($config['api']['base_url'] ?? '', '/') . '/',
            'timeout'     => $config['timeout'] ?? 1800,
            'verify'      => $config['verify_ssl'] ?? true,
            'http_errors' => false,
        ]);

        $this->notification = new Notification();
    }

    // ──────────────────────────────────────────────────────────────────
    // Main entry — processes a single project
    // ──────────────────────────────────────────────────────────────────

    public function run(
        string $projectDir,
        bool   $force   = false,
        array  $flags   = ['insert', 'verbose'],
        string $profile = 'database_config.py'
    ): array {
        // Reset per-project tracking
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

            $this->processProject($projectDir, $force, $flags, $profile);
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
        array  $flags,
        string $profile
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
            $name = basename($studyPath);

            // Skip already processed
            if (!$force && isset($processed[$name])) {
                $prev = $processed[$name];
                $this->log("  [{$name}] SKIPPED — {$prev['status']} ({$prev['timestamp']})");
                $this->importResults[$name] = [
                    'status' => 'skipped',
                    'reason' => "already {$prev['status']}",
                ];
                $this->stats['studies_skipped']++;
                continue;
            }

            $this->log("  [{$name}]");

            if ($this->dryRun) {
                $this->log("    DRY RUN — would POST importdicomstudy");
                $this->log("    source={$studyPath}");
                $this->importResults[$name] = ['status' => 'dry_run'];
                $this->stats['studies_processed']++;
                continue;
            }

            $this->importOneStudy($name, $studyPath, $dicomRoot, $processed, $flags, $profile);
        }
    }

    private function importOneStudy(
        string  $name,
        string  $studyPath,
        string  $dicomRoot,
        array  &$processed,
        array   $flags,
        string  $profile
    ): void {
        try {
            $t0      = microtime(true);
            $result  = $this->callImportDicomStudy($studyPath, $flags, $profile);
            $elapsed = round(microtime(true) - $t0, 2);

            $status    = $result['status'] ?? 'unknown';
            $http      = $result['http_status'] ?? 0;
            $errorType = $result['error_type'] ?? '';

            // Already inserted → mark and skip
            if ($errorType === 'ALREADY_EXISTS') {
                $this->log("    Already inserted — marking done ({$elapsed}s)");
                $this->markProcessed($dicomRoot, $processed, $name, 'already_exists');
                $this->importResults[$name] = [
                    'status'  => 'already_exists',
                    'elapsed' => $elapsed,
                ];
                $this->stats['studies_already_exist']++;
                return;
            }

            if ($status === 'success' || ($http >= 200 && $http < 300 && $status !== 'error')) {
                $this->log("    SUCCESS ({$elapsed}s)");
                $this->markProcessed($dicomRoot, $processed, $name, 'success');
                $this->importResults[$name] = [
                    'status'  => 'success',
                    'elapsed' => $elapsed,
                ];
                $this->stats['studies_processed']++;
                return;
            }

            $errMsg = $this->extractErrorMessage($result);
            $this->log("    FAILED ({$elapsed}s): {$errMsg}");
            $this->writeError($name, "Import failed: {$errMsg}");
            $this->writeErrorDetail("Command: " . ($result['command'] ?? 'N/A'));
            $this->writeErrorDetail("HTTP status: {$http}");
            $this->writeErrorDetail("Error type: {$errorType}");
            $this->markProcessed($dicomRoot, $processed, $name, 'failed', $errMsg);
            $this->importResults[$name] = [
                'status'  => 'failed',
                'reason'  => $errMsg,
                'elapsed' => $elapsed,
            ];
            $this->stats['studies_failed']++;

        } catch (\Exception $e) {
            $this->log("    EXCEPTION: " . $e->getMessage());
            $this->writeError($name, "Import exception: " . $e->getMessage());
            $this->importResults[$name] = [
                'status' => 'failed',
                'reason' => $e->getMessage(),
            ];
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

            $body = json_decode($resp->getBody()->getContents(), true);
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
    // Endpoint call
    // ──────────────────────────────────────────────────────────────────

    private function callImportDicomStudy(
        string $sourceDir,
        array  $flags   = ['insert', 'verbose'],
        string $profile = 'database_config.py',
        bool   $async   = false
    ): array {
        $endpoint = 'cbigr_api/script/importdicomstudy';

        $payload = [
            'args'  => ['source' => $sourceDir, 'profile' => $profile],
            'flags' => $flags,
            'async' => $async,
        ];

        $this->logger->debug("  POST {$endpoint}");
        $this->logger->debug("  source={$sourceDir} flags=" . implode(',', $flags));

        try {
            $resp = $this->http->post($endpoint, [
                'json'    => $payload,
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
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

    // ══════════════════════════════════════════════════════════════════
    //  RUN LOG — per pipeline run, all detail
    // ══════════════════════════════════════════════════════════════════

    private function openRunLog(): void
    {
        if ($this->runLogFh !== null) {
            return;
        }
        if ($this->logDir === null) {
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
                "{$sep}\n"
                . " ARCHIMEDES DICOM Import Pipeline — Run Log\n"
                . " Started: " . date('Y-m-d H:i:s T') . "\n"
                . ($this->dryRun ? " Mode: DRY RUN\n" : "")
                . "{$sep}\n\n"
            );
        }
    }

    /** Write to both console (logger) and run log file */
    private function log(string $msg): void
    {
        $this->logger->info($msg);

        if ($this->runLogFh) {
            $ts = date('H:i:s');
            fwrite($this->runLogFh, "[{$ts}] {$msg}\n");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  ERROR LOG — only created when errors occur
    // ══════════════════════════════════════════════════════════════════

    private function writeError(string $context, string $msg): void
    {
        $this->logger->error("[{$context}] {$msg}");

        // Lazy-open error log on first error
        if ($this->errorFh === null && $this->logDir !== null) {
            if (!is_dir($this->logDir)) {
                @mkdir($this->logDir, 0755, true);
            }
            $this->errorLogPath = "{$this->logDir}/dicom_errors_{$this->runTimestamp}.log";
            $this->errorFh = @fopen($this->errorLogPath, 'a');
            if ($this->errorFh) {
                $sep = str_repeat('=', 72);
                fwrite($this->errorFh,
                    "{$sep}\n"
                    . " ARCHIMEDES DICOM Import Pipeline — Error Log\n"
                    . " Run: {$this->runTimestamp}\n"
                    . "{$sep}\n\n"
                );
            }
        }

        if ($this->errorFh) {
            $ts = date('H:i:s');
            fwrite($this->errorFh, "[{$ts}] [{$context}] {$msg}\n");
        }

        // Also write to run log as ERROR
        if ($this->runLogFh) {
            $ts = date('H:i:s');
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
        if ($this->errorFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->errorFh, "\n{$sep}\n Closed: " . date('Y-m-d H:i:s T') . "\n{$sep}\n");
            fclose($this->errorFh);
            $this->errorFh = null;
        }

        if ($this->runLogFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->runLogFh, "\n{$sep}\n Completed: " . date('Y-m-d H:i:s T') . "\n{$sep}\n");
            fclose($this->runLogFh);
            $this->runLogFh = null;
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  PROJECT SUMMARY — written to run log after each project
    // ══════════════════════════════════════════════════════════════════

    private function writeProjectSummary(string $projectName): void
    {
        $this->log("");
        $this->log("──── PROJECT SUMMARY: {$projectName} ────");
        $this->log("");
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
                    $elapsed = $this->importResults[$s]['elapsed'] ?? '';
                    $this->log("      + {$s}" . ($elapsed ? " ({$elapsed}s)" : ""));
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
                    $reason = $this->importResults[$s]['reason'] ?? '?';
                    $this->log("      - {$s} — {$reason}");
                }
            }
            if (!empty($failed)) {
                $this->log("    Failed (" . count($failed) . "):");
                foreach ($failed as $s) {
                    $reason = $this->importResults[$s]['reason'] ?? '?';
                    $this->log("      ! {$s} — {$reason}");
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
        $this->log("");
        $this->log("  Studies found:          {$s['studies_found']}");
        $this->log("  Successfully imported:  {$s['studies_processed']}");
        $this->log("  Already existed:        {$s['studies_already_exist']}");
        $this->log("  Skipped:                {$s['studies_skipped']}");
        $this->log("  Failed:                 {$s['studies_failed']}");
        $this->log("");

        if ($this->runLogPath) {
            $this->log("  Run log:   {$this->runLogPath}");
        }
        if ($this->errorLogPath) {
            $this->log("  Error log: {$this->errorLogPath}");
        }

        $hasErrors = $s['studies_failed'] > 0;
        $outcome   = $hasErrors ? 'COMPLETED WITH ERRORS' : 'COMPLETED SUCCESSFULLY';
        $this->log("");
        $this->log("  Result: {$outcome}");
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
        $s = $this->stats;

        $hasFailures = $s['studies_failed'] > 0;

        // Load per-project notification config from project.json
        $projectJson = rtrim($projectDir, '/') . '/project.json';
        $projectData = [];
        if (file_exists($projectJson)) {
            $projectData = json_decode(file_get_contents($projectJson), true) ?? [];
        }

        // Read notification config from project.json → notification_emails.dicom
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

        // ── Build email body ─────────────────────────────────────────

        $body  = "Project: {$projectName}\n";
        $body .= "Modality: dicom\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n";
        $body .= "Run: {$this->runTimestamp}\n\n";

        // ── Study Import Results ─────────────────────────────────────

        $body .= "DICOM Study Import:\n";

        $importByStatus = [
            'success' => [], 'failed' => [], 'skipped' => [],
            'already_exists' => [], 'dry_run' => [],
        ];
        foreach ($this->importResults as $study => $r) {
            $importByStatus[$r['status']][] = $study;
        }

        if (empty($this->importResults)) {
            $body .= "  (no studies found)\n";
        } else {
            if (!empty($importByStatus['success'])) {
                $count = count($importByStatus['success']);
                $body .= "  ✔ Imported: {$count}\n";
                foreach ($importByStatus['success'] as $study) {
                    $elapsed = $this->importResults[$study]['elapsed'] ?? '';
                    $body .= "     {$study}" . ($elapsed ? " ({$elapsed}s)" : "") . "\n";
                }
            }

            if (!empty($importByStatus['already_exists'])) {
                $count = count($importByStatus['already_exists']);
                $body .= "  ● Already existed: {$count} ("
                    . implode(', ', $importByStatus['already_exists']) . ")\n";
            }

            if (!empty($importByStatus['failed'])) {
                $failedByReason = [];
                foreach ($importByStatus['failed'] as $study) {
                    $reason = $this->importResults[$study]['reason'] ?? 'unknown';
                    $failedByReason[$reason][] = $study;
                }
                $count = count($importByStatus['failed']);
                $failedParts = [];
                foreach ($failedByReason as $reason => $studies) {
                    $failedParts[] = implode(', ', $studies) . " [{$reason}]";
                }
                $body .= "  ✗ Failed: {$count} (" . implode('; ', $failedParts) . ")\n";
            }

            if (!empty($importByStatus['skipped'])) {
                $skippedByReason = [];
                foreach ($importByStatus['skipped'] as $study) {
                    $reason = $this->importResults[$study]['reason'] ?? 'unknown';
                    $skippedByReason[$reason][] = $study;
                }
                $count = count($importByStatus['skipped']);
                $skippedParts = [];
                foreach ($skippedByReason as $reason => $studies) {
                    $skippedParts[] = implode(', ', $studies) . " [{$reason}]";
                }
                $body .= "  ⚠ Skipped: {$count} (" . implode('; ', $skippedParts) . ")\n";
            }
        }

        $body .= "\n";

        // ── Totals ───────────────────────────────────────────────────

        $body .= str_repeat('-', 50) . "\n";
        $body .= "Totals:\n";
        $body .= "  Studies found:          {$s['studies_found']}\n";
        $body .= "  Successfully imported:  {$s['studies_processed']}\n";
        $body .= "  Already existed:        {$s['studies_already_exist']}\n";
        $body .= "  Skipped:                {$s['studies_skipped']}\n";
        $body .= "  Failed:                 {$s['studies_failed']}\n";
        $body .= "\n";

        // ── Status message ───────────────────────────────────────────

        if ($hasFailures) {
            $body .= "⚠ Some studies failed to import.\n";
            $body .= "Check logs for details.\n";
        } elseif ($s['studies_processed'] > 0) {
            $body .= "✔ Import completed successfully.\n";
        } elseif ($s['studies_skipped'] > 0 || $s['studies_already_exist'] > 0) {
            $body .= "✔ Import completed. All studies were already processed.\n";
        } else {
            $body .= "✔ Import completed. No studies to process.\n";
        }

        // ── Log paths ────────────────────────────────────────────────

        $body .= "\n";
        if ($this->runLogPath) {
            $body .= "Run log: {$this->runLogPath}\n";
        }
        if ($this->errorLogPath) {
            $body .= "Error log: {$this->errorLogPath}\n";
        }

        // ── Send ─────────────────────────────────────────────────────

        $this->log("  Sending notification to: " . implode(', ', $emailsToSend));

        foreach ($emailsToSend as $to) {
            $this->notification->send($to, $subject, $body);
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════════

    private function extractErrorMessage(array $result): string
    {
        $errors = $result['errors'] ?? [];
        if (!empty($errors)) {
            $msgs = array_map(
                fn($e) => $e['message'] ?? $e['type'] ?? 'unknown',
                $errors
            );
            return implode('; ', array_slice($msgs, 0, 3));
        }
        if (!empty($result['output'])) {
            $lines = array_filter(explode("\n", trim($result['output'])));
            return end($lines) ?: 'Unknown error';
        }
        return $result['error_type']
            ?? 'Unknown error (HTTP ' . ($result['http_status'] ?? '?') . ')';
    }
}