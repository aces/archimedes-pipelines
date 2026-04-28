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
 * Read-only contract:
 *   The user-shared input subdirectories are treated as READ-ONLY. The
 *   pipeline NEVER writes to:
 *     - deidentified-raw/clinical/      (clinical data files)
 *     - documentation/data_dictionary/  (DD files)
 *   The backing storage is irrelevant (local mount, S3, file transfer drop,
 *   etc.); the contract is the same. Transient work (DoB normalization)
 *   uses sys_get_temp_dir() and the temp copy is unlinked after upload.
 *   The pipeline only writes to its own controlled subdirectories:
 *     - processed/clinical/  (tracking JSON, daily snapshots)
 *     - logs/clinical/       (run log, error log)
 *
 * Logging:
 *   1. Console        -> stdout (always)
 *   2. Run log        -> logs/clinical/clinical_run_{timestamp}.log    (per run, all detail)
 *   3. Error log      -> logs/clinical/clinical_errors_{timestamp}.log (per run, only if errors)
 *   4. Email          -> success or failure notification with full summary
 *   Run log and error log share the same timestamp so they pair up.
 *
 * Reingestion tracking:
 *   processed/clinical/.clinical_tracking.json stores an MD5 hash per file.
 *   On each run the current file hash is compared to the stored hash:
 *     - No entry         -> first insertion, upload everything
 *     - Hash matches     -> skip (no changes)
 *     - Hash differs     -> re-ingestion, upload full file (LORIS skips already-existing rows)
 *   Use --force to bypass the hash check and always re-upload.
 *
 * DoB normalization:
 *   Before upload, the DoB column is rewritten to YYYY-MM-01 per ARCHIMEDES
 *   privacy policy (day jittered to 01, missing parts default to 01). The
 *   rewrite goes into a temp file in sys_get_temp_dir(); the original
 *   source file is never modified. Hash tracking and snapshot archiving
 *   continue to use the original source file; only the upload step sees
 *   the normalized copy.
 */
class ClinicalPipeline
{
    // -- Dependencies and flags ------------------------------------------

    private array $config;
    private LoggerInterface $logger;
    private ClinicalClient $client;
    private Notification $notification;

    private bool $dryRun;
    private bool $verbose;
    private bool $force;

    /** Timestamp for this pipeline run, shared across all log files. */
    private string $runTimestamp;

    // -- Log file handles ------------------------------------------------

    /** @var resource|null Error log handle, opened lazily on first error. */
    private $errorFh = null;
    private ?string $errorLogPath = null;

    /** @var resource|null Run log handle, opened per project. */
    private $runLogFh = null;
    private ?string $runLogPath = null;

    private ?string $logDir = null;

    // -- Reingestion tracking --------------------------------------------

    /** Loaded from .clinical_tracking.json, written back after each project. */
    private array $trackingData = [];
    private ?string $trackingFilePath = null;

    // -- Per-run results -------------------------------------------------

    /** filename -> {status, type, time, error} */
    private array $installResults = [];

    /** filename -> {status, reason, instruments, rows, ...} */
    private array $dataResults = [];

    /** Aggregate stats across all projects in this run. */
    private array $stats = [
        'dd_files_found'     => 0,
        'dd_installed'       => 0,
        'dd_already_existed' => 0,
        'dd_failed'          => 0,
        'data_files_found'   => 0,
        'data_uploaded'      => 0,
        'data_failed'        => 0,
        'data_skipped'       => 0,
        'rows_inserted'      => 0,  // net-new rows saved by LORIS
        'rows_existed'       => 0,  // rows already in LORIS, skipped
        'pairs_processed'    => 0,  // candidate-session pairs touched
    ];

    /**
     * CandIDs that exist in LORIS BEFORE this project's uploads begin.
     * Captured once per project in loadExistingCandidatesForProject().
     * Any CandID returned by an upload that is NOT in this set is treated
     * as a newly-created candidate for the email's "new candidates" count.
     */
    private array $existingCandIdsAtProjectStart = [];

    /**
     * True only when the pre-run snapshot call succeeded. When false,
     * the email reports "classification unavailable" instead of guessing.
     */
    private bool $candidateClassificationAvailable = false;

    // -- Constants -------------------------------------------------------

    /** DD file extension -> instrument_type recognized by LORIS. */
    private const DD_EXTENSIONS = [
        'csv'   => 'redcap',
        'linst' => 'linst',
        'json'  => 'bids',
    ];

    /** Data file extension -> upload format expected by LORIS. */
    private const DATA_EXTENSIONS = [
        'csv' => 'LORIS_CSV',
        'tsv' => 'BIDS_TSV',
    ];

    /** Admin forms excluded from header-based instrument detection. */
    private const DEFAULT_EXCLUDE_FORMS = ['nip_connector', 'project_request_form'];

    /** Column names recognized for DoB normalization (case-insensitive). */
    private const DOB_COLUMN_NAMES = ['dob', 'date_of_birth', 'birth_date'];

    // ──────────────────────────────────────────────────────────────────
    // Constructor
    // ──────────────────────────────────────────────────────────────────

    /**
     * Initialize pipeline state, set up console logging, and build the
     * LORIS API client and notification helper from config.
     */
    public function __construct(
        array $config,
        bool $dryRun = false,
        bool $verbose = false,
        bool $force = false
    ) {
        $this->config       = $config;
        $this->dryRun       = $dryRun;
        $this->verbose      = $verbose;
        $this->force        = $force;
        $this->runTimestamp = date('Y-m-d_H-i-s');

        $logLevel  = $verbose ? Logger::DEBUG : Logger::INFO;
        $formatter = new CleanLogFormatter();
        $console   = new StreamHandler('php://stdout', $logLevel);
        $console->setFormatter($formatter);

        $this->logger = new Logger('clinical');
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

    /**
     * Pipeline entry point. Authenticates, discovers projects matching
     * the filters, processes each, writes the final summary, and returns
     * the process exit code (0 on success, 1 on any failure).
     */
    public function run(array $filters = []): int
    {
        $this->logger->info("=== CLINICAL DATA INGESTION PIPELINE ===");
        $this->logger->info("Run: {$this->runTimestamp}");
        if ($this->dryRun) {
            $this->logger->info("MODE: DRY RUN");
        }
        if ($this->force) {
            $this->logger->info("MODE: FORCE - hash check bypassed, all files will be re-uploaded");
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
    // Project orchestration
    // ──────────────────────────────────────────────────────────────────

    /**
     * Run the full ingestion flow for one project: open log, snapshot
     * existing candidates, load hash tracking, install instruments,
     * upload data, flush tracking, write summary, send notification.
     */
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
        $this->log("DD dir   (read-only): {$ddDir}");
        $this->log("Data dir (read-only): {$dataDir}");
        $this->log("========================================");

        // Reset per-project state.
        $this->installResults = [];
        $this->dataResults    = [];

        $this->loadExistingCandidatesForProject();
        $this->loadTrackingFile($project);

        $this->installFromDirectory($ddDir);
        $this->uploadFromDirectory($project, $dataDir);

        $this->saveTrackingFile();

        $this->writeProjectSummary($name);
        $this->sendNotification($project);
    }

    /**
     * Capture every CandID in LORIS before any upload runs. Used later to
     * decide which CandIDs returned by uploads are new vs already-existing.
     * If the snapshot call fails, classification disables itself and the
     * email reports the total count without guessing the new/existing split.
     */
    private function loadExistingCandidatesForProject(): void
    {
        $this->existingCandIdsAtProjectStart    = [];
        $this->candidateClassificationAvailable = false;

        try {
            $candidates = $this->client->getCandidates();
            foreach ($candidates as $c) {
                $cid = $c['CandID'] ?? $c['candid'] ?? $c['candId'] ?? null;
                if ($cid !== null) {
                    $this->existingCandIdsAtProjectStart[] = (string)$cid;
                }
            }
            $this->existingCandIdsAtProjectStart = array_values(
                array_unique($this->existingCandIdsAtProjectStart)
            );
            $this->candidateClassificationAvailable = true;
            $this->log("  Pre-run candidate snapshot: "
                . count($this->existingCandIdsAtProjectStart)
                . " CandID(s) already in LORIS");
        } catch (\Exception $e) {
            $this->log("  Pre-run candidate snapshot FAILED: " . $e->getMessage()
                . " - new-candidate count unavailable for this run");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  STEP 1: Install instruments
    // ══════════════════════════════════════════════════════════════════

    /**
     * Find every DD file (.csv / .linst / .json) in the data dictionary
     * directory and install each one into LORIS.
     */
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

    /**
     * Install one DD file. Treats "already installed" (including HTTP 409)
     * as a non-error. After a fresh install the client's instrument cache
     * is cleared so subsequent uploads see the new instrument.
     */
    private function installOneDDFile(array $f): void
    {
        $filePath = $f['path'];
        $filename = $f['name'];
        $type     = $f['type'];

        $this->log("  [{$type}] {$filename}");

        if ($this->dryRun) {
            $this->log("    DRY RUN - would install");
            $this->installResults[$filename] = ['status' => 'dry_run', 'type' => $type];
            return;
        }

        try {
            $t0      = microtime(true);
            $result  = $this->client->installInstrument($filePath);
            $elapsed = round(microtime(true) - $t0, 2);
            $msg     = $result['message'] ?? '';

            if ($result['success'] ?? false) {
                if (stripos($msg, 'already') !== false) {
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
    //  STEP 2: Upload data, file by file
    // ══════════════════════════════════════════════════════════════════

    /**
     * Find every data file (.csv / .tsv) in the clinical data directory.
     * Skip files that are excluded in project.json or whose hash matches
     * the last upload; process the rest.
     */
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
                $this->log("  [{$filename}] SKIPPED - excluded in project.json");
                $this->dataResults[$filename] = ['status' => 'skipped', 'reason' => 'excluded'];
                $this->stats['data_skipped']++;
                continue;
            }

            $changeStatus = $this->detectFileChange($filename, $f['path']);

            if ($changeStatus === 'unchanged') {
                // Pull last-known row counts from tracking so the summary
                // still tells the operator what is currently in LORIS.
                $tracked      = $this->trackingData[$filename] ?? [];
                $lastInserted = $tracked['rows_saved']       ?? null;
                $lastExisted  = $tracked['rows_existed']     ?? null;
                $lastRun      = $tracked['last_uploaded_at'] ?? null;
                $lastTotal    = isset($lastInserted, $lastExisted)
                    ? $lastInserted + $lastExisted
                    : null;

                $detail = '';
                if ($lastTotal !== null) {
                    $detail  = " (last run: {$lastInserted} new, {$lastExisted} existed";
                    $detail .= $lastRun ? ", uploaded {$lastRun})" : ")";
                }

                $this->log("  [{$filename}] SKIPPED - no changes since last upload (hash match){$detail}");

                $this->dataResults[$filename] = [
                    'status'        => 'skipped',
                    'reason'        => 'no changes',
                    'last_inserted' => $lastInserted,
                    'last_existed'  => $lastExisted,
                    'last_run'      => $lastRun,
                ];

                $this->stats['rows_existed']  += $lastExisted  ?? 0;
                $this->stats['rows_inserted'] += $lastInserted ?? 0;
                $this->stats['data_skipped']++;
                continue;
            }

            // Pass first_upload / reingestion label through for summary reporting.
            $this->processOneDataFile($project, $f, $changeStatus);
        }
    }

    /**
     * Process one data file end-to-end: count rows, normalize DoB into a
     * temp copy, pick instrument(s), upload, and on success update tracking
     * and archive a snapshot. The temp copy is always cleaned up.
     */
    private function processOneDataFile(array $project, array $fileInfo, string $changeStatus = 'first_upload'): void
    {
        $filePath = $fileInfo['path'];
        $filename = $fileInfo['name'];
        $format   = $fileInfo['format'];
        $baseName = pathinfo($filename, PATHINFO_FILENAME);

        $rows  = $this->countRows($filePath);
        $label = ($changeStatus === 'reingestion') ? 'RE-INGEST' : 'NEW';

        $this->log("  [{$filename}] {$rows} rows, format: {$format}, mode: {$label}");

        if ($rows === 0) {
            $this->log("    SKIPPED - empty file");
            $this->dataResults[$filename] = ['status' => 'skipped', 'reason' => 'empty', 'rows' => 0];
            $this->stats['data_skipped']++;
            return;
        }

        $uploadPath = $this->normalizeDobInFile($filePath, $format);
        $usingTemp  = ($uploadPath !== $filePath);

        try {
            // Case A: filename matches an existing instrument exactly.
            if ($this->client->instrumentExists($baseName)) {
                $this->log("    Instrument: {$baseName} (matched by filename)");
                $result = $this->doSingleUpload($baseName, $uploadPath, $format, $rows);
                $this->dataResults[$filename] = array_merge($result, [
                    'instruments'   => [$baseName],
                    'rows'          => $rows,
                    'change_status' => $changeStatus,
                    'pairs'         => $result['pairs']        ?? 0,
                    'cand_ids'      => $result['cand_ids']     ?? [],
                    'rows_existed'  => $result['rows_existed'] ?? 0,
                ]);
                if ($result['status'] === 'success') {
                    $this->updateTracking($filename, $filePath, $result);
                    $this->archiveSnapshot($project, $filePath);
                }
                return;
            }

            // Case B: detect instruments from `*_complete` columns in the header.
            $instruments = $this->detectInstrumentsFromHeaders($uploadPath, $format);

            if (empty($instruments)) {
                $this->log("    FAILED - no matching instruments found in headers");
                $this->writeError($filename, "No matching instruments found");
                $this->dataResults[$filename] = [
                    'status'        => 'failed',
                    'reason'        => 'no matching instruments',
                    'instruments'   => [],
                    'rows'          => $rows,
                    'change_status' => $changeStatus,
                ];
                $this->stats['data_failed']++;
                return;
            }

            $this->log("    Instruments (" . count($instruments) . "): " . implode(', ', $instruments));

            if (count($instruments) === 1) {
                $result = $this->doSingleUpload($instruments[0], $uploadPath, $format, $rows);
            } else {
                $result = $this->doMultiUpload($instruments, $uploadPath, $format, $rows);
            }

            $this->dataResults[$filename] = array_merge($result, [
                'instruments'   => $instruments,
                'rows'          => $rows,
                'change_status' => $changeStatus,
                'pairs'         => $result['pairs']        ?? 0,
                'cand_ids'      => $result['cand_ids']     ?? [],
                'rows_existed'  => $result['rows_existed'] ?? 0,
            ]);

            if ($result['status'] === 'success') {
                $this->updateTracking($filename, $filePath, $result);
                $this->archiveSnapshot($project, $filePath);
            }
        } finally {
            // Only the temp normalized copy is unlinked; the read-only
            // source file is never touched.
            if ($usingTemp && file_exists($uploadPath)) {
                @unlink($uploadPath);
            }
        }
    }

    /**
     * Read the file's header row and return instrument names matching
     * `{instrument}_complete` columns, filtered to those that actually
     * exist in LORIS. Admin forms are excluded.
     */
    private function detectInstrumentsFromHeaders(string $filePath, string $format): array
    {
        $delimiter = ($format === 'BIDS_TSV') ? "\t" : ',';

        $fh         = fopen($filePath, 'r');
        $headerLine = fgets($fh);
        fclose($fh);

        if ($headerLine === false) {
            return [];
        }

        $columns     = array_map('trim', str_getcsv(trim($headerLine), $delimiter));
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

    /**
     * Upload one file targeting a single instrument. Logs, updates global
     * stats, and returns a normalized result array.
     */
    private function doSingleUpload(string $instrument, string $filePath, string $format, int $rows): array
    {
        if ($this->dryRun) {
            $this->log("    DRY RUN - would upload {$rows} rows to {$instrument}");
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

    /**
     * Upload one file spanning multiple instruments via the multi-instrument
     * endpoint. Same logging, stats, and result contract as doSingleUpload().
     */
    private function doMultiUpload(array $instruments, string $filePath, string $format, int $rows): array
    {
        $count = count($instruments);

        if ($this->dryRun) {
            $this->log("    DRY RUN - would upload {$rows} rows for {$count} instruments");
            return ['status' => 'success', 'reason' => 'dry run'];
        }

        try {
            $t0      = microtime(true);
            $result  = $this->client->uploadMultiInstrumentData($instruments, $filePath, 'CREATE_SESSIONS');
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

    /**
     * Parse a LORIS upload response into rows_saved / rows_total /
     * rows_existed (from the "Saved N out of M" message) and the
     * candidate-session pair count and CandID list (from idMapping).
     */
    private function extractUploadInfo(array $result): array
    {
        $info = [
            'rows_saved'   => null,
            'rows_total'   => null,
            'rows_existed' => 0,
            'pairs'        => 0,
            'cand_ids'     => [],
        ];

        $msg = $result['message'] ?? null;
        if (is_string($msg) && preg_match('/Saved (\d+) out of (\d+)/', $msg, $m)) {
            $info['rows_saved']   = (int)$m[1];
            $info['rows_total']   = (int)$m[2];
            $info['rows_existed'] = $info['rows_total'] - $info['rows_saved'];
        }

        $idMap = $result['idMapping'] ?? [];
        if (!empty($idMap) && is_array($idMap)) {
            $info['pairs'] = count($idMap);
            foreach ($idMap as $m) {
                $cid = $m['CandID'] ?? $m['candid'] ?? $m['candId'] ?? null;
                if ($cid) {
                    $info['cand_ids'][] = (string)$cid;
                }
            }
        }

        return $info;
    }

    /**
     * Print a one-line success summary covering elapsed time, instrument
     * count (multi-upload only), row counts, candidate-session pair count,
     * and a truncated CandID list.
     */
    private function logUploadSuccess(float $elapsed, array $ui, ?int $instCount = null): void
    {
        $parts = ["SUCCESS ({$elapsed}s)"];
        if ($instCount !== null) {
            $parts[] = "{$instCount} instruments";
        }

        if ($ui['rows_saved'] !== null) {
            if ($ui['rows_saved'] === 0 && $ui['rows_existed'] > 0) {
                $parts[] = "0 new rows - {$ui['rows_existed']} already existed in LORIS";
            } elseif ($ui['rows_existed'] > 0) {
                $parts[] = "{$ui['rows_saved']} new rows inserted, {$ui['rows_existed']} already existed";
            } else {
                $parts[] = "{$ui['rows_saved']} rows inserted";
            }
        }

        if ($ui['pairs'] > 0) {
            $parts[] = "{$ui['pairs']} candidate-session pair(s) processed";
        }

        $this->log("    " . implode(' - ', $parts));

        if (!empty($ui['cand_ids'])) {
            $display = array_slice($ui['cand_ids'], 0, 10);
            $suffix  = count($ui['cand_ids']) > 10 ? ' ... +' . (count($ui['cand_ids']) - 10) . ' more' : '';
            $this->log("    CandIDs: " . implode(', ', $display) . $suffix);
        }
    }

    /** Add this upload's row counts and pair count to the run-wide stats. */
    private function tallyUploadSuccess(array $ui): void
    {
        $this->stats['data_uploaded']++;
        $this->stats['rows_inserted']    += $ui['rows_saved']   ?? 0;
        $this->stats['rows_existed']     += $ui['rows_existed'] ?? 0;
        $this->stats['pairs_processed']  += $ui['pairs']        ?? 0;
    }

    /**
     * Classify CandIDs touched this run as new vs already-existing using
     * the pre-run snapshot. If the snapshot was unavailable, returns the
     * full list with available=false so the renderer can be honest about
     * not knowing the split.
     *
     * @return array{new_count:int, new_candids:string[], existing_count:int, total_candids:string[], available:bool}
     */
    private function computeNewCandidates(): array
    {
        $allCandIds = [];
        foreach ($this->dataResults as $r) {
            if (($r['status'] ?? '') !== 'success') {
                continue;
            }
            foreach ($r['cand_ids'] ?? [] as $cid) {
                $allCandIds[] = (string)$cid;
            }
        }
        $allCandIds = array_values(array_unique($allCandIds));

        if (!$this->candidateClassificationAvailable) {
            return [
                'new_count'      => 0,
                'new_candids'    => [],
                'existing_count' => 0,
                'total_candids'  => $allCandIds,
                'available'      => false,
            ];
        }

        $newIds = array_values(array_diff($allCandIds, $this->existingCandIdsAtProjectStart));

        return [
            'new_count'      => count($newIds),
            'new_candids'    => $newIds,
            'existing_count' => count($allCandIds) - count($newIds),
            'total_candids'  => $allCandIds,
            'available'      => true,
        ];
    }

    // ══════════════════════════════════════════════════════════════════
    //  Hash-based reingestion tracking (writes to processed/clinical/)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Load the per-project tracking dictionary
     * (filename -> {hash, timestamps, last row counts}) from disk.
     * Creates the directory if missing; in-memory state is empty if no
     * tracking file exists yet.
     */
    private function loadTrackingFile(array $project): void
    {
        $base = rtrim($project['data_access']['mount_path'] ?? '', '/') . '/processed/clinical';

        if (!is_dir($base)) {
            mkdir($base, 0755, true);
        }

        $this->trackingFilePath = "{$base}/.clinical_tracking.json";
        $this->trackingData     = [];

        if (file_exists($this->trackingFilePath)) {
            $raw     = file_get_contents($this->trackingFilePath);
            $decoded = json_decode($raw, true);
            $this->trackingData = is_array($decoded) ? $decoded : [];
            $this->log("  Tracking: loaded " . count($this->trackingData)
                . " file(s) from {$this->trackingFilePath}");
        } else {
            $this->log("  Tracking: no existing tracking file - all files treated as first upload");
        }
    }

    /** Persist the in-memory tracking dictionary to disk as pretty-printed JSON. */
    private function saveTrackingFile(): void
    {
        if ($this->trackingFilePath === null) {
            return;
        }
        file_put_contents(
            $this->trackingFilePath,
            json_encode($this->trackingData, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
        );
    }

    /**
     * Compare current MD5 to the stored hash.
     * Returns 'first_upload' | 'reingestion' | 'unchanged'.
     * --force always returns 'reingestion'.
     */
    private function detectFileChange(string $filename, string $filePath): string
    {
        if ($this->force) {
            return 'reingestion';
        }

        $currentHash = md5_file($filePath);
        $stored      = $this->trackingData[$filename] ?? null;

        if ($stored === null) {
            return 'first_upload';
        }

        return ($currentHash !== ($stored['hash'] ?? '')) ? 'reingestion' : 'unchanged';
    }

    /**
     * Refresh a file's tracking entry after a successful upload: new hash,
     * bumped upload_count, latest counts and timestamps. first_uploaded_at
     * is preserved across runs.
     */
    private function updateTracking(string $filename, string $filePath, array $uploadResult): void
    {
        $existing = $this->trackingData[$filename] ?? null;
        $now      = date('Y-m-d\TH:i:s');

        $this->trackingData[$filename] = [
            'hash'              => md5_file($filePath),
            'first_uploaded_at' => $existing['first_uploaded_at'] ?? $now,
            'last_uploaded_at'  => $now,
            'run_timestamp'     => $this->runTimestamp,
            'upload_count'      => ($existing['upload_count'] ?? 0) + 1,
            'rows_total'        => $uploadResult['rows_total']   ?? null,
            'rows_saved'        => $uploadResult['rows_saved']   ?? null,
            'rows_existed'      => $uploadResult['rows_existed'] ?? 0,
        ];

        $this->saveTrackingFile();
    }

    /**
     * Archive a copy of the source file under processed/clinical/{date}/
     * for audit. The read-only source is never touched. If a same-named
     * file already exists for today, a timestamp prefix is added.
     */
    private function archiveSnapshot(array $project, string $src): void
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

        // Copy out to processed/; the source file is read-only and untouched.
        if (copy($src, $target)) {
            $this->log("    Snapshot archived -> processed/clinical/"
                . date('Y-m-d') . "/" . basename($target));
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  Run log + error log (writes to logs/clinical/)
    // ══════════════════════════════════════════════════════════════════

    /**
     * Open the per-run log file with a header banner including start time
     * and any active modes. Idempotent within a single run.
     */
    private function openRunLog(): void
    {
        if ($this->runLogFh !== null || $this->logDir === null) {
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
                . " ARCHIMEDES Clinical Pipeline - Run Log\n"
                . " Started: " . date('Y-m-d H:i:s T') . "\n"
                . ($this->dryRun ? " Mode: DRY RUN\n" : "")
                . ($this->force  ? " Mode: FORCE (hash check bypassed)\n" : "")
                . "{$sep}\n\n"
            );
        }
    }

    /** Write a line to the console (via Monolog) and the per-run log file. */
    private function log(string $msg): void
    {
        $this->logger->info($msg);

        if ($this->runLogFh) {
            $ts = date('H:i:s');
            fwrite($this->runLogFh, "[{$ts}] {$msg}\n");
        }
    }

    /**
     * Log an error to console + run log + error log. The error log is
     * opened lazily on the first error so successful runs leave no
     * error log file behind.
     */
    private function writeError(string $context, string $msg): void
    {
        $this->logger->error("[{$context}] {$msg}");

        if ($this->errorFh === null && $this->logDir !== null) {
            if (!is_dir($this->logDir)) {
                mkdir($this->logDir, 0755, true);
            }
            $this->errorLogPath = "{$this->logDir}/clinical_errors_{$this->runTimestamp}.log";
            $this->errorFh      = fopen($this->errorLogPath, 'a');

            if ($this->errorFh) {
                $sep = str_repeat('=', 72);
                fwrite($this->errorFh,
                    "{$sep}\n"
                    . " ARCHIMEDES Clinical Pipeline - Error Log\n"
                    . " Run: {$this->runTimestamp}\n"
                    . "{$sep}\n\n"
                );
            }
        }

        if ($this->errorFh) {
            $ts = date('H:i:s');
            fwrite($this->errorFh, "[{$ts}] [{$context}] {$msg}\n");
        }

        if ($this->runLogFh) {
            $ts = date('H:i:s');
            fwrite($this->runLogFh, "[{$ts}] ERROR [{$context}] {$msg}\n");
        }
    }

    /**
     * Append an indented continuation line to the error log + run log.
     * Used by writeUploadErrorDetails() to elaborate on a previous
     * writeError() entry.
     */
    private function writeErrorDetail(string $text): void
    {
        if ($this->errorFh) {
            fwrite($this->errorFh, "  {$text}\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "  ERROR-DETAIL: {$text}\n");
        }
    }

    /**
     * Dump up to the first 20 LORIS error messages from a failed upload,
     * with an "and N more" line if the list was truncated.
     */
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

    /** Write closing banners and close the run + error log handles. */
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
    //  Project + final summaries
    // ══════════════════════════════════════════════════════════════════

    /**
     * Write the per-project summary block to the run log: instrument
     * installs, data uploads, and candidates touched.
     */
    private function writeProjectSummary(string $projectName): void
    {
        $this->log("");
        $this->log("──── PROJECT SUMMARY: {$projectName} ────");

        // -- Install summary --
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
                    $this->log("      ! {$f} - {$err}");
                }
            }
        }

        // -- Data upload summary --
        $this->log("");
        $this->log("  DATA INGESTION:");

        if (empty($this->dataResults)) {
            $this->log("    (no data files)");
        } else {
            $firstUpload = [];
            $reingested  = [];
            $failed      = [];
            $skipped     = [];

            foreach ($this->dataResults as $file => $r) {
                switch ($r['status']) {
                    case 'success':
                        if (($r['change_status'] ?? '') === 'reingestion') {
                            $reingested[] = $file;
                        } else {
                            $firstUpload[] = $file;
                        }
                        break;
                    case 'failed':  $failed[]  = $file; break;
                    case 'skipped': $skipped[] = $file; break;
                }
            }

            if (!empty($firstUpload)) {
                $this->log("    First upload (" . count($firstUpload) . "):");
                foreach ($firstUpload as $f) {
                    $this->log("      + " . $this->formatDataResultLine($f));
                }
            }
            if (!empty($reingested)) {
                $this->log("    Re-ingested - file changed, new rows only (" . count($reingested) . "):");
                foreach ($reingested as $f) {
                    $this->log("      ↺ " . $this->formatDataResultLine($f));
                }
            }
            if (!empty($failed)) {
                $this->log("    Failed (" . count($failed) . "):");
                foreach ($failed as $f) {
                    $reason = $this->dataResults[$f]['reason'] ?? '?';
                    $this->log("      ! {$f} - {$reason}");
                }
            }
            if (!empty($skipped)) {
                $this->log("    Skipped - no changes (" . count($skipped) . "):");
                foreach ($skipped as $f) {
                    $r      = $this->dataResults[$f];
                    $reason = $r['reason'] ?? '?';
                    if ($reason === 'no changes' && isset($r['last_inserted'], $r['last_existed'])) {
                        $lastRun = $r['last_run'] ? " @ {$r['last_run']}" : '';
                        $this->log("      - {$f} - {$r['last_inserted']} new / {$r['last_existed']} existed (last run{$lastRun})");
                    } else {
                        $this->log("      - {$f} - {$reason}");
                    }
                }
            }
        }

        // -- Candidate breakdown (full CandID lists in run log) --
        $nc = $this->computeNewCandidates();
        if (!empty($nc['total_candids'])) {
            $this->log("");
            $this->log("  CANDIDATES TOUCHED THIS RUN:");
            $this->log("    Total distinct CandIDs: " . count($nc['total_candids']));
            if ($nc['available']) {
                $this->log("    Newly created in LORIS: {$nc['new_count']}");
                if (!empty($nc['new_candids'])) {
                    $this->log("      CandIDs: " . implode(', ', $nc['new_candids']));
                }
                if ($nc['existing_count'] > 0) {
                    $existingIds = array_values(array_diff($nc['total_candids'], $nc['new_candids']));
                    $this->log("    Existing, data refreshed: {$nc['existing_count']}");
                    $this->log("      CandIDs: " . implode(', ', $existingIds));
                }
            } else {
                $this->log("    (classification unavailable - pre-run LORIS snapshot failed)");
                $this->log("    All CandIDs: " . implode(', ', $nc['total_candids']));
            }
        }

        $this->log("");
        $this->log("────────────────────────────────────────");
    }

    /**
     * Render one data file's outcome as a single human-readable line for
     * use in run-log summaries and the email body.
     */
    private function formatDataResultLine(string $file): string
    {
        $r    = $this->dataResults[$file];
        $inst = implode(', ', $r['instruments'] ?? []);

        $rowParts = [];
        if (isset($r['rows_saved'])) {
            if ($r['rows_saved'] === 0 && ($r['rows_existed'] ?? 0) > 0) {
                $rowParts[] = "0 new rows - {$r['rows_existed']} already existed";
            } elseif (($r['rows_existed'] ?? 0) > 0) {
                $rowParts[] = "{$r['rows_saved']} new, {$r['rows_existed']} existed";
            } else {
                $rowParts[] = "{$r['rows_saved']} rows inserted";
            }
        } elseif (isset($r['rows'])) {
            $rowParts[] = "{$r['rows']} rows";
        }

        if (($r['pairs'] ?? 0) > 0) {
            $rowParts[] = "{$r['pairs']} candidate-session pair(s)";
        }

        $candStr = '';
        if (!empty($r['cand_ids'])) {
            $display = array_slice($r['cand_ids'], 0, 8);
            $suffix  = count($r['cand_ids']) > 8 ? ' +' . (count($r['cand_ids']) - 8) . ' more' : '';
            $candStr = " [" . implode(', ', $display) . $suffix . "]";
        }

        if (isset($r['elapsed'])) {
            $rowParts[] = "{$r['elapsed']}s";
        }

        $infoStr = !empty($rowParts) ? ' (' . implode(', ', $rowParts) . ')' : '';
        $instStr = $inst ? " [{$inst}]" : '';

        return "{$file}{$instStr}{$infoStr}{$candStr}";
    }

    /**
     * End-of-run aggregate across all projects: file counts, row counts,
     * log paths, overall pass/fail outcome.
     *
     * Note: a "rows inserted (new)" line is intentionally omitted. The
     * LORIS multi-instrument endpoint does not report net-new row counts,
     * so the value is meaningless across runs. rows_existed is kept since
     * it reflects last-known state from skipped files (genuine info).
     */
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
        $this->log("    Data files found:    {$s['data_files_found']}");
        $this->log("    Data files processed:{$s['data_uploaded']}");
        $this->log("    Data files failed:   {$s['data_failed']}");
        $this->log("    Data files skipped:  {$s['data_skipped']} (hash unchanged)");
        $this->log("    Rows existed:        {$s['rows_existed']} (already in LORIS - includes last-known from skipped files)");
        $this->log("    Candidate-session pairs touched: {$s['pairs_processed']}");
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
    //  Email notification
    // ══════════════════════════════════════════════════════════════════

    /**
     * Send the project's clinical-modality notification email to the
     * appropriate recipient list (on_success or on_error from
     * project.json). Notification failures are logged but never crash
     * the pipeline since ingestion is already complete.
     */
    private function sendNotification(array $project): void
    {
        $name        = $project['project_common_name'] ?? 'Unknown';
        $s           = $this->stats;
        $hasFailures = ($s['data_failed'] > 0 || $s['dd_failed'] > 0);

        $successEmails = $project['notification_emails']['clinical']['on_success'] ?? [];
        $errorEmails   = $project['notification_emails']['clinical']['on_error']   ?? [];
        $emailsToSend  = $hasFailures ? $errorEmails : $successEmails;

        if (empty($emailsToSend)) {
            $this->log("  No notification emails configured for clinical");
            return;
        }

        $status  = $hasFailures ? 'FAILED' : 'SUCCESS';
        $subject = "{$status}: {$name} Clinical Ingestion";

        // -- Build email body --

        $body  = "Project: {$name}\n";
        $body .= "Modality: clinical\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n";
        $body .= "Run: {$this->runTimestamp}\n";
        if ($this->force) {
            $body .= "Mode: FORCE (hash check bypassed)\n";
        }
        $body .= "\n";

        // -- Instrument Installation --

        $body .= "Instrument Installation:\n";

        $installByStatus = ['installed' => [], 'exists' => [], 'failed' => [], 'dry_run' => []];
        foreach ($this->installResults as $file => $r) {
            $installByStatus[$r['status']][] = $file;
        }

        if (count($this->installResults) === 0) {
            $body .= "  (no DD files found)\n";
        } else {
            if (!empty($installByStatus['installed'])) {
                $count = count($installByStatus['installed']);
                $body .= "  ✔ Installed: {$count} (" . implode(', ', $installByStatus['installed']) . ")\n";
            }
            if (!empty($installByStatus['exists'])) {
                $count = count($installByStatus['exists']);
                $body .= "  ● Already existed: {$count} (" . implode(', ', $installByStatus['exists']) . ")\n";
            }
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

        // -- Data Ingestion --

        $body .= "Data Ingestion:\n";

        $firstUpload = $reingested = $failed = $skipped = [];
        foreach ($this->dataResults as $file => $r) {
            switch ($r['status']) {
                case 'success':
                    if (($r['change_status'] ?? '') === 'reingestion') {
                        $reingested[] = $file;
                    } else {
                        $firstUpload[] = $file;
                    }
                    break;
                case 'failed':  $failed[]  = $file; break;
                case 'skipped': $skipped[] = $file; break;
            }
        }

        if (count($this->dataResults) === 0) {
            $body .= "  (no data files found)\n";
        } else {
            if (!empty($firstUpload)) {
                $body .= "  ✔ First upload (" . count($firstUpload) . "):\n";
                foreach ($firstUpload as $file) {
                    $body .= "     " . $this->formatDataResultLine($file) . "\n";
                }
            }
            if (!empty($reingested)) {
                $body .= "  ↺ Re-ingested - file changed, new rows only (" . count($reingested) . "):\n";
                foreach ($reingested as $file) {
                    $body .= "     " . $this->formatDataResultLine($file) . "\n";
                }
            }
            if (!empty($failed)) {
                $failedByReason = [];
                foreach ($failed as $file) {
                    $reason = $this->dataResults[$file]['reason'] ?? 'error';
                    $failedByReason[$reason][] = $file;
                }
                $count = count($failed);
                $failedParts = [];
                foreach ($failedByReason as $reason => $files) {
                    $failedParts[] = implode(', ', $files) . " [{$reason}]";
                }
                $body .= "  ✗ Failed: {$count} (" . implode('; ', $failedParts) . ")\n";
            }
            if (!empty($skipped)) {
                $skippedByReason = [];
                foreach ($skipped as $file) {
                    $reason = $this->dataResults[$file]['reason'] ?? 'unknown';
                    $skippedByReason[$reason][] = $file;
                }
                $count = count($skipped);
                $skippedParts = [];
                foreach ($skippedByReason as $reason => $files) {
                    $skippedParts[] = implode(', ', $files) . " [{$reason}]";
                }
                $body .= "  ⚠ Skipped: {$count} (" . implode('; ', $skippedParts) . ")\n";
            }
        }

        $body .= "\n";

        // -- Totals --

        $body .= str_repeat('-', 50) . "\n";
        $body .= "Totals:\n";
        $body .= "  DD files: {$s['dd_files_found']} found, {$s['dd_installed']} installed, "
            . "{$s['dd_already_existed']} existed, {$s['dd_failed']} failed\n";
        $body .= "  Data files: {$s['data_files_found']} found, {$s['data_uploaded']} processed, "
            . "{$s['data_failed']} failed, {$s['data_skipped']} skipped\n";

        if ($s['rows_existed'] > 0) {
            $body .= "  Rows existed (skipped LORIS): {$s['rows_existed']}\n";
        }
        if ($s['pairs_processed'] > 0) {
            $body .= "  Candidate-session pairs touched: {$s['pairs_processed']}\n";
        }

        $body .= "\n";

        // -- Candidates breakdown (counts only; full lists live in run log) --

        $nc = $this->computeNewCandidates();
        if (!empty($nc['total_candids'])) {
            $body .= "Candidates:\n";
            if ($nc['available']) {
                $body .= "  New candidates created:        {$nc['new_count']}\n";
                if ($nc['existing_count'] > 0) {
                    $body .= "  Existing candidates refreshed: {$nc['existing_count']}\n";
                }
            } else {
                $body .= "  Total candidates touched: " . count($nc['total_candids'])
                    . " (new/existing split unavailable)\n";
            }
            $body .= "\n";
        }

        // -- Status message --

        if ($hasFailures) {
            $body .= "⚠ Some instruments failed to install or ingest.\n";
            $body .= "Check logs for details.\n";
        } elseif ($s['data_uploaded'] > 0) {
            $body .= "✔ Ingestion completed successfully.\n";
        } elseif ($s['data_skipped'] > 0 && $s['data_uploaded'] === 0) {
            $body .= "✔ Ingestion completed. All files skipped - no content changes detected (hash match).\n";
        } else {
            $body .= "✔ Ingestion completed. No data files to process.\n";
        }

        // -- Log paths --

        $body .= "\n";
        if ($this->runLogPath) {
            $body .= "Run log: {$this->runLogPath}\n";
        }
        if ($this->errorLogPath) {
            $body .= "Error log: {$this->errorLogPath}\n";
        }

        // -- Send --

        $this->log("  Sending notification to: " . implode(', ', $emailsToSend));

        foreach ($emailsToSend as $to) {
            try {
                $this->notification->send($to, $subject, $body);
            } catch (\Exception $e) {
                // Notification failures must not crash the pipeline; ingestion
                // already succeeded by this point. Log so the operator can
                // investigate (SMTP, address, etc.).
                $this->writeError('notification', "Failed to send to {$to}: " . $e->getMessage());
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════════

    /**
     * Pull a single error string out of a LORIS response, whether the
     * 'message' field is a plain string or a list of error objects.
     */
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

    /** Count data rows in a CSV/TSV (header line excluded). */
    private function countRows(string $file): int
    {
        $count = 0;
        $fh    = fopen($file, 'r');
        fgetcsv($fh); // skip header
        while (fgetcsv($fh) !== false) {
            $count++;
        }
        fclose($fh);
        return $count;
    }

    // ══════════════════════════════════════════════════════════════════
    //  DoB normalization
    //  Reads source (read-only) and writes the normalized copy to a temp
    //  file in sys_get_temp_dir(). Original is never modified.
    // ══════════════════════════════════════════════════════════════════

    /**
     * Write a copy of the source file with the DoB column rewritten to
     * YYYY-MM-01 into a temp file, and return the temp path. If no DoB
     * column is present the original path is returned unchanged.
     * Caller must unlink the temp file when done.
     */
    private function normalizeDobInFile(string $srcPath, string $format): string
    {
        $delimiter = ($format === 'BIDS_TSV') ? "\t" : ',';

        $in = fopen($srcPath, 'r');
        if ($in === false) {
            return $srcPath;
        }

        $headers = fgetcsv($in, 0, $delimiter);
        if ($headers === false) {
            fclose($in);
            return $srcPath;
        }

        $dobIdx = null;
        foreach ($headers as $i => $h) {
            $norm = strtolower(trim((string)$h));
            if (in_array($norm, self::DOB_COLUMN_NAMES, true)) {
                $dobIdx = $i;
                break;
            }
        }

        if ($dobIdx === null) {
            fclose($in);
            return $srcPath;
        }

        // Write to system temp; the source file is read-only and never modified.
        $tmpPath = tempnam(sys_get_temp_dir(), 'clinical_dob_') . '_' . basename($srcPath);
        $out     = fopen($tmpPath, 'w');
        fputcsv($out, $headers, $delimiter);

        $changed = 0;
        $total   = 0;
        while (($row = fgetcsv($in, 0, $delimiter)) !== false) {
            $total++;
            if (array_key_exists($dobIdx, $row)) {
                $orig = (string)$row[$dobIdx];
                $norm = $this->normalizeDobValue($orig);
                if ($norm !== $orig) {
                    $changed++;
                }
                $row[$dobIdx] = $norm;
            }
            fputcsv($out, $row, $delimiter);
        }

        fclose($in);
        fclose($out);

        $this->log("    DoB normalized: column '{$headers[$dobIdx]}' - {$changed}/{$total} row(s) rewritten to YYYY-MM-01");

        return $tmpPath;
    }

    /**
     * Coerce one DoB string to YYYY-MM-01.
     * Accepts: full YYYY-MM-DD (day -> 01), YYYY-MM (day -> 01), or
     * YYYY only (month and day -> 01-01). Anything unrecognized is
     * returned unchanged.
     */
    private function normalizeDobValue(string $dob): string
    {
        $dob = trim($dob);
        if ($dob === '') {
            return $dob;
        }

        if (preg_match('/^(\d{4})-(\d{2})-\d{2}$/', $dob, $m)) {
            return "{$m[1]}-{$m[2]}-01";
        }
        if (preg_match('/^(\d{4})-(\d{2})$/', $dob, $m)) {
            return "{$m[1]}-{$m[2]}-01";
        }
        if (preg_match('/^(\d{4})$/', $dob, $m)) {
            return "{$m[1]}-01-01";
        }

        return $dob;
    }

    /**
     * Walk the config's collections and projects, load each project.json,
     * attach the collection name and project path, and return only the
     * enabled entries that match any --collection / --project filter.
     */
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