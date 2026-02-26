<?php
declare(strict_types=1);

namespace LORIS\Pipelines;

use LORIS\Endpoints\ClinicalClient;
use LORIS\Utils\{Notification, CleanLogFormatter};
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Psr\Log\LoggerInterface;

/**
 * Clinical Data Ingestion Pipeline
 *
 * Logging:
 *   1. Console        → stdout (always)
 *   2. Run log        → logs/clinical/clinical_run_{timestamp}.log  (per run, all detail)
 *   3. Error log      → logs/clinical/clinical_errors_{timestamp}.log (per run, only if errors)
 *   4. Email          → success or failure notification with full summary
 *
 * Run log and error log share the same timestamp so they pair up.
 */
class ClinicalPipeline
{
    private array $config;
    private LoggerInterface $logger;
    private ClinicalClient $client;
    private Notification $notification;
    private bool $dryRun;
    private bool $verbose;

    /** Timestamp for this pipeline run — shared across log files */
    private string $runTimestamp;

    /** @var resource|null Error log — opened lazily on first error */
    private $errorFh = null;
    private ?string $errorLogPath = null;

    /** @var resource|null Run log — opened per project */
    private $runLogFh = null;
    private ?string $runLogPath = null;

    private ?string $logDir = null;

    // ── Tracking ─────────────────────────────────────────────────────

    /** Install results per file: filename → {status, type, time, error} */
    private array $installResults = [];

    /** Data upload results per file: filename → {status, reason, instruments, rows, ...} */
    private array $dataResults = [];

    /** Global stats across all projects in this run */
    private array $stats = [
        'dd_files_found'        => 0,
        'dd_installed'          => 0,
        'dd_already_existed'    => 0,
        'dd_failed'             => 0,
        'data_files_found'      => 0,
        'data_uploaded'         => 0,
        'data_failed'           => 0,
        'data_skipped'          => 0,
        'rows_uploaded'         => 0,
        'rows_skipped'          => 0,
        'candidates_created'    => 0,
    ];

    /** DD extensions → instrument_type for LORIS */
    private const DD_EXTENSIONS = [
        'csv'   => 'redcap',
        'linst' => 'linst',
        'json'  => 'bids',
    ];

    /** Data file extensions → format for LORIS */
    private const DATA_EXTENSIONS = [
        'csv' => 'LORIS_CSV',
        'tsv' => 'BIDS_TSV',
    ];

    /** Admin forms to exclude */
    private const DEFAULT_EXCLUDE_FORMS = ['nip_connector', 'project_request_form'];

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
        $this->logger = new Logger('clinical');
        $formatter = new CleanLogFormatter();

        $console = new StreamHandler('php://stdout', $logLevel);
        $console->setFormatter($formatter);
        $this->logger->pushHandler($console);

        $this->client = new ClinicalClient(
            $config['api']['base_url'],
            $config['api']['username'],
            $config['api']['password'],
            $config['api']['token_expiry_minutes'] ?? 55,
            $this->logger
        );

        $this->notification = new Notification();
    }

    // ──────────────────────────────────────────────────────────────────
    // Main entry
    // ──────────────────────────────────────────────────────────────────

    public function run(array $filters = []): int
    {
        $this->logger->info("=== CLINICAL DATA INGESTION PIPELINE ===");
        $this->logger->info("Run: {$this->runTimestamp}");
        if ($this->dryRun) {
            $this->logger->info("MODE: DRY RUN");
        }

        try {
            $this->client->authenticate();

            $projects = $this->discoverProjects($filters);
            if (empty($projects)) {
                $this->logger->warning("No projects found");
                return 0;
            }

            $this->logger->info("Found " . count($projects) . " project(s)");

            foreach ($projects as $project) {
                $this->processProject($project);
            }

            $this->writeFinalSummary();
            $this->closeAllLogs();

            return ($this->stats['data_failed'] > 0 || $this->stats['dd_failed'] > 0) ? 1 : 0;

        } catch (\Exception $e) {
            $this->writeError("FATAL", $e->getMessage());
            $this->logger->debug($e->getTraceAsString());
            $this->closeAllLogs();
            return 1;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Process project
    // ──────────────────────────────────────────────────────────────────

    private function processProject(array $project): void
    {
        $name      = $project['project_common_name'] ?? basename($project['_projectPath']);
        $mountPath = $project['data_access']['mount_path'] ?? $project['_projectPath'];
        $ddDir     = "{$mountPath}/documentation/data_dictionary";
        $dataDir   = "{$mountPath}/deidentified-raw/clinical";

        $this->logDir = "{$mountPath}/logs/clinical";
        $this->openRunLog();

        $this->log("========================================");
        $this->log("Project: {$name}");
        $this->log("Run: {$this->runTimestamp}");
        $this->log("DD dir: {$ddDir}");
        $this->log("Data dir: {$dataDir}");
        $this->log("========================================");

        // Reset per-project tracking
        $this->installResults = [];
        $this->dataResults    = [];

        $this->installFromDirectory($ddDir);
        $this->uploadFromDirectory($project, $dataDir);
        $this->writeProjectSummary($name);
        $this->sendNotification($project);
    }

    // ══════════════════════════════════════════════════════════════════
    //  STEP 1: Install instruments
    // ══════════════════════════════════════════════════════════════════

    private function installFromDirectory(string $ddDir): void
    {
        $this->log("");
        $this->log("──── STEP 1: INSTALL INSTRUMENTS ────");

        if (!is_dir($ddDir)) {
            $this->log("  Directory not found: {$ddDir}");
            return;
        }

        $files = [];
        foreach (self::DD_EXTENSIONS as $ext => $type) {
            foreach (glob("{$ddDir}/*.{$ext}") as $path) {
                $files[] = ['path' => $path, 'name' => basename($path), 'type' => $type];
            }
        }

        $this->stats['dd_files_found'] += count($files);

        if (empty($files)) {
            $this->log("  No DD files found (.csv, .linst, .json)");
            return;
        }

        $this->log("  Found " . count($files) . " DD file(s)");
        $this->log("");

        foreach ($files as $f) {
            $this->installOneDDFile($f);
        }
    }

    private function installOneDDFile(array $f): void
    {
        $filePath = $f['path'];
        $filename = $f['name'];
        $type     = $f['type'];

        $this->log("  [{$type}] {$filename}");

        if ($this->dryRun) {
            $this->log("    DRY RUN — would install");
            $this->installResults[$filename] = ['status' => 'dry_run', 'type' => $type];
            return;
        }

        try {
            $t0      = microtime(true);
            $result  = $this->client->installInstrument($filePath);
            $elapsed = round(microtime(true) - $t0, 2);
            $msg     = $result['message'] ?? '';

            if ($result['success'] ?? false) {
                if (stripos($msg, 'already') !== false || stripos($msg, 'Already') !== false) {
                    $this->log("    Already installed ({$elapsed}s)");
                    $this->installResults[$filename] = ['status' => 'exists', 'type' => $type, 'time' => $elapsed];
                    $this->stats['dd_already_existed']++;
                } else {
                    $this->log("    Installed successfully ({$elapsed}s)");
                    $this->installResults[$filename] = ['status' => 'installed', 'type' => $type, 'time' => $elapsed];
                    $this->stats['dd_installed']++;
                    $this->client->clearInstrumentCache();
                }
                return;
            }

            if (stripos($msg, '409') !== false || stripos($msg, 'already') !== false) {
                $this->log("    Already installed ({$elapsed}s)");
                $this->installResults[$filename] = ['status' => 'exists', 'type' => $type, 'time' => $elapsed];
                $this->stats['dd_already_existed']++;
                return;
            }

            $this->log("    FAILED: {$msg}");
            $this->writeError($filename, "Install failed: {$msg}");
            $this->installResults[$filename] = ['status' => 'failed', 'type' => $type, 'error' => $msg];
            $this->stats['dd_failed']++;

        } catch (\Exception $e) {
            $emsg = $e->getMessage();
            if (stripos($emsg, '409') !== false) {
                $this->log("    Already installed");
                $this->installResults[$filename] = ['status' => 'exists', 'type' => $type];
                $this->stats['dd_already_existed']++;
                return;
            }
            $this->log("    EXCEPTION: {$emsg}");
            $this->writeError($filename, "Install exception: {$emsg}");
            $this->installResults[$filename] = ['status' => 'failed', 'type' => $type, 'error' => $emsg];
            $this->stats['dd_failed']++;
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  STEP 2: Upload data — file by file
    // ══════════════════════════════════════════════════════════════════

    private function uploadFromDirectory(array $project, string $dataDir): void
    {
        $this->log("");
        $this->log("──── STEP 2: UPLOAD CLINICAL DATA ────");

        if (!is_dir($dataDir)) {
            $this->log("  Directory not found: {$dataDir}");
            return;
        }

        $files = [];
        foreach (self::DATA_EXTENSIONS as $ext => $format) {
            foreach (glob("{$dataDir}/*.{$ext}") as $path) {
                $files[] = ['path' => $path, 'name' => basename($path), 'format' => $format];
            }
        }

        $this->stats['data_files_found'] += count($files);
        $excludeFiles = $project['exclude_data_files'] ?? [];

        if (empty($files)) {
            $this->log("  No data files found (.csv, .tsv)");
            return;
        }

        $this->log("  Found " . count($files) . " data file(s)");
        $this->log("");

        foreach ($files as $f) {
            $filename = $f['name'];

            if (in_array($filename, $excludeFiles, true)) {
                $this->log("  [{$filename}] SKIPPED — excluded in project.json");
                $this->dataResults[$filename] = ['status' => 'skipped', 'reason' => 'excluded'];
                $this->stats['data_skipped']++;
                continue;
            }

            if ($this->isAlreadyProcessed($project, $filename)) {
                $this->log("  [{$filename}] SKIPPED — already processed");
                $this->dataResults[$filename] = ['status' => 'skipped', 'reason' => 'already processed'];
                $this->stats['data_skipped']++;
                continue;
            }

            $this->processOneDataFile($project, $f);
        }
    }

    private function processOneDataFile(array $project, array $fileInfo): void
    {
        $filePath = $fileInfo['path'];
        $filename = $fileInfo['name'];
        $format   = $fileInfo['format'];
        $baseName = pathinfo($filename, PATHINFO_FILENAME);

        $rows = $this->countRows($filePath);

        $this->log("  [{$filename}] {$rows} rows, format: {$format}");

        if ($rows === 0) {
            $this->log("    SKIPPED — empty file");
            $this->dataResults[$filename] = ['status' => 'skipped', 'reason' => 'empty', 'rows' => 0];
            $this->stats['data_skipped']++;
            return;
        }

        // Case A: filename matches instrument
        if ($this->client->instrumentExists($baseName)) {
            $this->log("    Instrument: {$baseName} (matched by filename)");
            $result = $this->doSingleUpload($baseName, $filePath, $format, $rows);
            $this->dataResults[$filename] = array_merge($result, [
                'instruments' => [$baseName], 'rows' => $rows,
            ]);
            if ($result['status'] === 'success') {
                $this->archiveFile($project, $filePath);
            }
            return;
        }

        // Case B: detect from headers
        $instruments = $this->detectInstrumentsFromHeaders($filePath, $format);

        if (empty($instruments)) {
            $this->log("    FAILED — no matching instruments found in headers");
            $this->writeError($filename, "No matching instruments found");
            $this->dataResults[$filename] = [
                'status' => 'failed', 'reason' => 'no matching instruments',
                'instruments' => [], 'rows' => $rows,
            ];
            $this->stats['data_failed']++;
            return;
        }

        $this->log("    Instruments (" . count($instruments) . "): " . implode(', ', $instruments));

        if (count($instruments) === 1) {
            $result = $this->doSingleUpload($instruments[0], $filePath, $format, $rows);
        } else {
            $result = $this->doMultiUpload($instruments, $filePath, $format, $rows);
        }

        $this->dataResults[$filename] = array_merge($result, [
            'instruments' => $instruments, 'rows' => $rows,
        ]);

        if ($result['status'] === 'success') {
            $this->archiveFile($project, $filePath);
        }
    }

    private function detectInstrumentsFromHeaders(string $filePath, string $format): array
    {
        $delimiter = ($format === 'BIDS_TSV') ? "\t" : ',';
        $fh = fopen($filePath, 'r');
        $headerLine = fgets($fh);
        fclose($fh);

        if ($headerLine === false) {
            return [];
        }

        $columns = array_map('trim', str_getcsv(trim($headerLine), $delimiter));
        $instruments = [];

        foreach ($columns as $col) {
            if (preg_match('/^(.+)_complete$/', $col, $m)) {
                $inst = $m[1];
                if (!in_array($inst, self::DEFAULT_EXCLUDE_FORMS, true)
                    && $this->client->instrumentExists($inst)
                ) {
                    $instruments[] = $inst;
                }
            }
        }

        return array_unique($instruments);
    }

    // ══════════════════════════════════════════════════════════════════
    //  Upload execution
    // ══════════════════════════════════════════════════════════════════

    private function doSingleUpload(string $instrument, string $filePath, string $format, int $rows): array
    {
        if ($this->dryRun) {
            $this->log("    DRY RUN — would upload {$rows} rows to {$instrument}");
            return ['status' => 'success', 'reason' => 'dry run'];
        }

        try {
            $t0      = microtime(true);
            $result  = $this->client->uploadInstrumentData($instrument, $filePath, 'CREATE_SESSIONS');
            $elapsed = round(microtime(true) - $t0, 2);

            if ($result['success'] ?? false) {
                $ui = $this->extractUploadInfo($result);
                $this->logUploadSuccess($elapsed, $ui);
                $this->tallyUploadSuccess($ui);
                return array_merge(['status' => 'success', 'reason' => 'uploaded', 'elapsed' => $elapsed], $ui);
            }

            $msg = $this->firstErrorMsg($result);
            $this->log("    FAILED ({$elapsed}s): {$msg}");
            $this->writeError($instrument, "Upload failed: {$msg}");
            $this->writeUploadErrorDetails($instrument, $filePath, $result);
            $this->stats['data_failed']++;
            return ['status' => 'failed', 'reason' => $msg, 'elapsed' => $elapsed];

        } catch (\Exception $e) {
            $this->log("    EXCEPTION: " . $e->getMessage());
            $this->writeError($instrument, "Upload exception: " . $e->getMessage());
            $this->stats['data_failed']++;
            return ['status' => 'failed', 'reason' => $e->getMessage()];
        }
    }

    private function doMultiUpload(array $instruments, string $filePath, string $format, int $rows): array
    {
        $count = count($instruments);

        if ($this->dryRun) {
            $this->log("    DRY RUN — would upload {$rows} rows for {$count} instruments");
            return ['status' => 'success', 'reason' => 'dry run'];
        }

        try {
            $t0      = microtime(true);
            $result  = $this->client->uploadMultiInstrumentData($instruments, $filePath, 'CREATE_SESSIONS', $format);
            $elapsed = round(microtime(true) - $t0, 2);

            if ($result['success'] ?? false) {
                $ui = $this->extractUploadInfo($result);
                $this->logUploadSuccess($elapsed, $ui, $count);
                $this->tallyUploadSuccess($ui);
                return array_merge(['status' => 'success', 'reason' => 'uploaded', 'elapsed' => $elapsed], $ui);
            }

            $msg = $this->firstErrorMsg($result);
            $this->log("    FAILED ({$elapsed}s): {$msg}");
            $this->writeError('multi-instrument', "Upload failed: {$msg}");
            $this->writeUploadErrorDetails('multi-instrument', $filePath, $result);
            $this->stats['data_failed']++;
            return ['status' => 'failed', 'reason' => $msg, 'elapsed' => $elapsed];

        } catch (\Exception $e) {
            $this->log("    EXCEPTION: " . $e->getMessage());
            $this->writeError('multi-instrument', "Exception: " . $e->getMessage());
            $this->stats['data_failed']++;
            return ['status' => 'failed', 'reason' => $e->getMessage()];
        }
    }

    private function extractUploadInfo(array $result): array
    {
        $info = ['rows_saved' => null, 'rows_total' => null, 'rows_skipped' => 0, 'candidates' => 0];

        $msg = $result['message'] ?? null;
        if (is_string($msg) && preg_match('/Saved (\d+) out of (\d+)/', $msg, $m)) {
            $info['rows_saved']   = (int)$m[1];
            $info['rows_total']   = (int)$m[2];
            $info['rows_skipped'] = $info['rows_total'] - $info['rows_saved'];
        }

        $idMap = $result['idMapping'] ?? [];
        if (!empty($idMap) && is_array($idMap)) {
            $info['candidates'] = count($idMap);
        }

        return $info;
    }

    private function logUploadSuccess(float $elapsed, array $ui, ?int $instCount = null): void
    {
        $parts = ["SUCCESS ({$elapsed}s)"];
        if ($instCount !== null) {
            $parts[] = "{$instCount} instruments";
        }
        if ($ui['rows_saved'] !== null) {
            $parts[] = "{$ui['rows_saved']}/{$ui['rows_total']} rows saved";
        }
        if ($ui['candidates'] > 0) {
            $parts[] = "{$ui['candidates']} new candidates";
        }
        $this->log("    " . implode(' — ', $parts));
    }

    private function tallyUploadSuccess(array $ui): void
    {
        $this->stats['data_uploaded']++;
        $this->stats['rows_uploaded']      += $ui['rows_saved'] ?? 0;
        $this->stats['rows_skipped']       += $ui['rows_skipped'] ?? 0;
        $this->stats['candidates_created'] += $ui['candidates'];
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
            mkdir($this->logDir, 0755, true);
        }

        $this->runLogPath = "{$this->logDir}/clinical_run_{$this->runTimestamp}.log";
        $this->runLogFh   = fopen($this->runLogPath, 'a');

        if ($this->runLogFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->runLogFh,
                "{$sep}\n"
                . " ARCHIMEDES Clinical Pipeline — Run Log\n"
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
                mkdir($this->logDir, 0755, true);
            }
            $this->errorLogPath = "{$this->logDir}/clinical_errors_{$this->runTimestamp}.log";
            $this->errorFh = fopen($this->errorLogPath, 'a');
            if ($this->errorFh) {
                $sep = str_repeat('=', 72);
                fwrite($this->errorFh,
                    "{$sep}\n"
                    . " ARCHIMEDES Clinical Pipeline — Error Log\n"
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

    private function writeUploadErrorDetails(string $context, string $file, array $result): void
    {
        $errors = isset($result['message'])
            ? (is_array($result['message']) ? $result['message'] : [$result['message']])
            : [];

        $this->writeErrorDetail("File: {$file}");
        foreach (array_slice($errors, 0, 20) as $i => $err) {
            $msg = is_array($err) ? ($err['message'] ?? json_encode($err)) : (string)$err;
            $this->writeErrorDetail(($i + 1) . ". {$msg}");
        }
        if (count($errors) > 20) {
            $this->writeErrorDetail("... and " . (count($errors) - 20) . " more errors");
        }
    }

    private function closeAllLogs(): void
    {
        // Close error log
        if ($this->errorFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->errorFh, "\n{$sep}\n Closed: " . date('Y-m-d H:i:s T') . "\n{$sep}\n");
            fclose($this->errorFh);
            $this->errorFh = null;
        }

        // Close run log
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

        // ── Install summary ──────────────────────────────────────────
        $this->log("");
        $this->log("  INSTRUMENT INSTALLATION:");

        if (empty($this->installResults)) {
            $this->log("    (no DD files)");
        } else {
            $installed = $exists = $failed = [];
            foreach ($this->installResults as $file => $r) {
                switch ($r['status']) {
                    case 'installed': $installed[] = $file; break;
                    case 'exists':    $exists[]    = $file; break;
                    case 'failed':    $failed[]    = $file; break;
                    case 'dry_run':   $installed[] = "{$file} (dry run)"; break;
                }
            }
            if (!empty($installed)) {
                $this->log("    Newly installed (" . count($installed) . "):");
                foreach ($installed as $f) {
                    $time = $this->installResults[str_replace(' (dry run)', '', $f)]['time'] ?? '';
                    $this->log("      + {$f}" . ($time ? " ({$time}s)" : ""));
                }
            }
            if (!empty($exists)) {
                $this->log("    Already installed (" . count($exists) . "):");
                foreach ($exists as $f) {
                    $this->log("      = {$f}");
                }
            }
            if (!empty($failed)) {
                $this->log("    Failed (" . count($failed) . "):");
                foreach ($failed as $f) {
                    $err = $this->installResults[$f]['error'] ?? '?';
                    $this->log("      ! {$f} — {$err}");
                }
            }
        }

        // ── Data upload summary ──────────────────────────────────────
        $this->log("");
        $this->log("  DATA INGESTION:");

        if (empty($this->dataResults)) {
            $this->log("    (no data files)");
        } else {
            $success = $failed = $skipped = [];
            foreach ($this->dataResults as $file => $r) {
                switch ($r['status']) {
                    case 'success': $success[] = $file; break;
                    case 'failed':  $failed[]  = $file; break;
                    case 'skipped': $skipped[] = $file; break;
                }
            }

            if (!empty($success)) {
                $this->log("    Uploaded (" . count($success) . "):");
                foreach ($success as $f) {
                    $r    = $this->dataResults[$f];
                    $inst = implode(', ', $r['instruments'] ?? []);
                    $info = "rows={$r['rows']}";
                    if (isset($r['rows_saved'])) {
                        $info .= " saved={$r['rows_saved']}";
                    }
                    if (($r['rows_skipped'] ?? 0) > 0) {
                        $info .= " existing={$r['rows_skipped']}";
                    }
                    if (($r['candidates'] ?? 0) > 0) {
                        $info .= " new_candidates={$r['candidates']}";
                    }
                    if (isset($r['elapsed'])) {
                        $info .= " time={$r['elapsed']}s";
                    }
                    $this->log("      + {$f} [{$inst}] ({$info})");
                }
            }
            if (!empty($failed)) {
                $this->log("    Failed (" . count($failed) . "):");
                foreach ($failed as $f) {
                    $reason = $this->dataResults[$f]['reason'] ?? '?';
                    $this->log("      ! {$f} — {$reason}");
                }
            }
            if (!empty($skipped)) {
                $this->log("    Skipped (" . count($skipped) . "):");
                foreach ($skipped as $f) {
                    $reason = $this->dataResults[$f]['reason'] ?? '?';
                    $this->log("      - {$f} — {$reason}");
                }
            }
        }

        $this->log("");
        $this->log("────────────────────────────────────────");
    }

    // ══════════════════════════════════════════════════════════════════
    //  FINAL SUMMARY — end of entire pipeline run
    // ══════════════════════════════════════════════════════════════════

    private function writeFinalSummary(): void
    {
        $s = $this->stats;

        $this->log("");
        $this->log("========================================");
        $this->log("PIPELINE RUN SUMMARY");
        $this->log("========================================");
        $this->log("  Run: {$this->runTimestamp}");
        $this->log("");
        $this->log("  Instruments:");
        $this->log("    DD files found:     {$s['dd_files_found']}");
        $this->log("    Newly installed:    {$s['dd_installed']}");
        $this->log("    Already existed:    {$s['dd_already_existed']}");
        $this->log("    Install failures:   {$s['dd_failed']}");
        $this->log("");
        $this->log("  Data:");
        $this->log("    Data files found:   {$s['data_files_found']}");
        $this->log("    Uploaded:           {$s['data_uploaded']}");
        $this->log("    Failed:             {$s['data_failed']}");
        $this->log("    Skipped:            {$s['data_skipped']}");
        $this->log("    Rows uploaded:      {$s['rows_uploaded']}");
        $this->log("    Rows skipped:       {$s['rows_skipped']}");
        $this->log("    Candidates created: {$s['candidates_created']}");
        $this->log("");

        if ($this->runLogPath) {
            $this->log("  Run log:   {$this->runLogPath}");
        }
        if ($this->errorLogPath) {
            $this->log("  Error log: {$this->errorLogPath}");
        }

        $hasErrors = ($s['data_failed'] > 0 || $s['dd_failed'] > 0);
        $outcome   = $hasErrors ? 'COMPLETED WITH ERRORS' : 'COMPLETED SUCCESSFULLY';
        $this->log("");
        $this->log("  Result: {$outcome}");
        $this->log("========================================");
    }

    // ══════════════════════════════════════════════════════════════════
    //  EMAIL NOTIFICATION
    // ══════════════════════════════════════════════════════════════════

    private function sendNotification(array $project): void
    {
        $name = $project['project_common_name'] ?? 'Unknown';
        $s    = $this->stats;

        $hasFailures = ($s['data_failed'] > 0 || $s['dd_failed'] > 0);

        $successEmails = $project['notification_emails']['clinical']['on_success'] ?? [];
        $errorEmails   = $project['notification_emails']['clinical']['on_error'] ?? [];

        $emailsToSend = $hasFailures ? $errorEmails : $successEmails;

        if (empty($emailsToSend)) {
            $this->log("  No notification emails configured for clinical");
            return;
        }

        $status = $hasFailures ? 'FAILED' : 'SUCCESS';
        $subject = "{$status}: {$name} Clinical Ingestion";

        // ── Build email body ─────────────────────────────────────────

        $body  = "Project: {$name}\n";
        $body .= "Modality: clinical\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n";
        $body .= "Run: {$this->runTimestamp}\n\n";

        // ── Instrument Installation ──────────────────────────────────

        $body .= "Instrument Installation:\n";

        // Group install results by status
        $installByStatus = ['installed' => [], 'exists' => [], 'failed' => [], 'dry_run' => []];
        foreach ($this->installResults as $file => $r) {
            $installByStatus[$r['status']][] = $file;
        }

        $installCount = count($this->installResults);
        if ($installCount === 0) {
            $body .= "  (no DD files found)\n";
        } else {
            // Newly installed
            if (!empty($installByStatus['installed'])) {
                $count = count($installByStatus['installed']);
                $body .= "  ✔ Installed: {$count} (" . implode(', ', $installByStatus['installed']) . ")\n";
            }
            // Already existed
            if (!empty($installByStatus['exists'])) {
                $count = count($installByStatus['exists']);
                $body .= "  ● Already existed: {$count} (" . implode(', ', $installByStatus['exists']) . ")\n";
            }
            // Failed — group by error reason
            if (!empty($installByStatus['failed'])) {
                $failedByReason = [];
                foreach ($installByStatus['failed'] as $file) {
                    $reason = $this->installResults[$file]['error'] ?? 'unknown';
                    $failedByReason[$reason][] = $file;
                }
                $count = count($installByStatus['failed']);
                $failedParts = [];
                foreach ($failedByReason as $reason => $files) {
                    $failedParts[] = implode(', ', $files) . " [{$reason}]";
                }
                $body .= "  ✗ Failed: {$count} (" . implode('; ', $failedParts) . ")\n";
            }
        }

        $body .= "\n";

        // ── Data Ingestion ───────────────────────────────────────────

        $body .= "Data Ingestion:\n";

        // Group data results by status
        $dataByStatus = ['success' => [], 'failed' => [], 'skipped' => []];
        foreach ($this->dataResults as $file => $r) {
            $dataByStatus[$r['status']][] = $file;
        }

        $dataCount = count($this->dataResults);
        if ($dataCount === 0) {
            $body .= "  (no data files found)\n";
        } else {
            // Uploaded — show each with detail
            if (!empty($dataByStatus['success'])) {
                $count = count($dataByStatus['success']);
                $body .= "  ✔ Uploaded: {$count}\n";
                foreach ($dataByStatus['success'] as $file) {
                    $r = $this->dataResults[$file];
                    $inst = implode(', ', $r['instruments'] ?? []);
                    $info = [];
                    if (isset($r['rows_saved']) && isset($r['rows'])) {
                        $info[] = "{$r['rows_saved']}/{$r['rows']} rows";
                    } elseif (isset($r['rows'])) {
                        $info[] = "{$r['rows']} rows";
                    }
                    if (($r['candidates'] ?? 0) > 0) {
                        $info[] = "{$r['candidates']} new candidates";
                    }
                    $infoStr = !empty($info) ? ' (' . implode(', ', $info) . ')' : '';
                    $instStr = $inst ? " [{$inst}]" : '';
                    $body .= "     {$file}{$instStr}{$infoStr}\n";
                }
            }

            // Failed — group by reason
            if (!empty($dataByStatus['failed'])) {
                $failedByReason = [];
                foreach ($dataByStatus['failed'] as $file) {
                    $reason = $this->dataResults[$file]['reason'] ?? 'error';
                    $failedByReason[$reason][] = $file;
                }
                $count = count($dataByStatus['failed']);
                $failedParts = [];
                foreach ($failedByReason as $reason => $files) {
                    $failedParts[] = implode(', ', $files) . " [{$reason}]";
                }
                $body .= "  ✗ Failed: {$count} (" . implode('; ', $failedParts) . ")\n";
            }

            // Skipped — group by reason
            if (!empty($dataByStatus['skipped'])) {
                $skippedByReason = [];
                foreach ($dataByStatus['skipped'] as $file) {
                    $reason = $this->dataResults[$file]['reason'] ?? 'unknown';
                    $skippedByReason[$reason][] = $file;
                }
                $count = count($dataByStatus['skipped']);
                $skippedParts = [];
                foreach ($skippedByReason as $reason => $files) {
                    $skippedParts[] = implode(', ', $files) . " [{$reason}]";
                }
                $body .= "  ⚠ Skipped: {$count} (" . implode('; ', $skippedParts) . ")\n";
            }
        }

        $body .= "\n";

        // ── Totals ───────────────────────────────────────────────────

        $body .= str_repeat('-', 50) . "\n";
        $body .= "Totals:\n";
        $body .= "  DD files: {$s['dd_files_found']} found, {$s['dd_installed']} installed, {$s['dd_already_existed']} existed, {$s['dd_failed']} failed\n";
        $body .= "  Data files: {$s['data_files_found']} found, {$s['data_uploaded']} uploaded, {$s['data_failed']} failed, {$s['data_skipped']} skipped\n";

        if ($s['rows_uploaded'] > 0 || $s['rows_skipped'] > 0) {
            $totalRows = $s['rows_uploaded'] + $s['rows_skipped'];
            $body .= "  Rows: {$s['rows_uploaded']}/{$totalRows} saved";
            if ($s['rows_skipped'] > 0) {
                $body .= " ({$s['rows_skipped']} already exist)";
            }
            $body .= "\n";
        }
        if ($s['candidates_created'] > 0) {
            $body .= "  Candidates created: {$s['candidates_created']}\n";
        }

        $body .= "\n";

        // ── Status message ───────────────────────────────────────────

        if ($hasFailures) {
            $body .= "⚠ Some instruments failed to install or ingest.\n";
            $body .= "Check logs for details.\n";
        } elseif ($s['data_uploaded'] > 0) {
            $body .= "✔ Ingestion completed successfully.\n";
        } elseif ($s['data_skipped'] > 0 && $s['data_uploaded'] === 0) {
            $body .= "✔ Ingestion completed. All data files were skipped\n";
            $body .= "   (already processed or no new data available).\n";
        } else {
            $body .= "✔ Ingestion completed. No data files to process.\n";
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

    private function firstErrorMsg(array $result): string
    {
        $m = $result['message'] ?? null;
        if (is_string($m)) {
            return $m;
        }
        if (is_array($m)) {
            $first = reset($m);
            return is_array($first) ? ($first['message'] ?? json_encode($first)) : (string)$first;
        }
        return 'Unknown error';
    }

    private function countRows(string $file): int
    {
        $c  = 0;
        $fh = fopen($file, 'r');
        fgetcsv($fh); // skip header
        while (fgetcsv($fh) !== false) {
            $c++;
        }
        fclose($fh);
        return $c;
    }

    private function isAlreadyProcessed(array $project, string $filename): bool
    {
        $base = rtrim($project['data_access']['mount_path'] ?? '', '/') . '/processed/clinical';
        if (!is_dir($base)) {
            return false;
        }
        foreach (glob("{$base}/*", GLOB_ONLYDIR) as $d) {
            if (file_exists("{$d}/{$filename}")) {
                return true;
            }
        }
        return false;
    }

    private function archiveFile(array $project, string $src): void
    {
        $dest = rtrim($project['data_access']['mount_path'] ?? '', '/')
            . '/processed/clinical/' . date('Y-m-d');
        if (!is_dir($dest)) {
            mkdir($dest, 0755, true);
        }
        $target = "{$dest}/" . basename($src);
        if (file_exists($target)) {
            $target = "{$dest}/" . time() . '_' . basename($src);
        }
        if (copy($src, $target)) {
            $this->log("    Archived to processed/clinical/" . date('Y-m-d') . "/");
        }
    }

    private function discoverProjects(array $filters): array
    {
        $projects = [];

        foreach ($this->config['collections'] ?? [] as $coll) {
            if (!($coll['enabled'] ?? true)) {
                continue;
            }
            if (isset($filters['collection']) && $coll['name'] !== $filters['collection']) {
                continue;
            }

            foreach ($coll['projects'] ?? [] as $pc) {
                if (!($pc['enabled'] ?? true)) {
                    continue;
                }
                if (isset($filters['project']) && $pc['name'] !== $filters['project']) {
                    continue;
                }

                $path = $coll['base_path'] . '/' . $pc['name'];
                $json = "{$path}/project.json";

                if (!file_exists($json)) {
                    $this->logger->warning("project.json not found: {$json}");
                    continue;
                }

                $data = json_decode(file_get_contents($json), true);
                if ($data === null) {
                    $this->logger->warning("Invalid JSON: {$json}");
                    continue;
                }

                $data['_collection']  = $coll['name'];
                $data['_projectPath'] = $path;
                $projects[] = $data;
            }
        }

        return $projects;
    }
}