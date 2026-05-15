<?php
declare(strict_types=1);

namespace LORIS\Pipelines;

use LORIS\Endpoints\{ClinicalClient, EviDataClient};
use LORIS\Utils\{Notification, CleanLogFormatter, MountHealthCheck};
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
 *   etc.); the contract is the same. Transient work (date normalization)
 *   uses sys_get_temp_dir() and the temp copy is unlinked after upload.
 *   The pipeline only writes to its own controlled subdirectories:
 *     - processed/clinical/  (tracking JSON, daily snapshots)
 *     - logs/clinical/       (run log, error log)
 *     - logs/evidata/        (EviData artifacts — see EviData section below)
 *
 * Logging:
 *   1. Console        -> stdout (always)
 *   2. Run log        -> logs/clinical/clinical_run_{timestamp}.log    (per run, all detail)
 *   3. Error log      -> logs/clinical/clinical_errors_{timestamp}.log (per run, only if errors)
 *   4. Email          -> success or failure notification with full summary
 *   Run log and error log share the same timestamp so they pair up.
 *
 * EviData privacy pre-flight gate:
 *   When evidata.enabled=true in loris_client_config.json, every project
 *   starts with a privacy check against every CSV/TSV in deidentified-raw/
 *   clinical/. Files are uploaded to the EviData service, scored, and the
 *   structured results + bundled report ZIP are saved to logs/evidata/.
 *   If ANY file fails or errors, the project aborts entirely — no DD
 *   install, no data upload, no tracking update. A dedicated failure
 *   email goes to the project's evidata recipients with the failed
 *   reports attached.
 *
 *   When evidata.enabled=false (or the block is absent), the gate is
 *   bypassed silently and the pipeline behaves exactly as it did before
 *   EviData was added.
 *
 *   See: scripts/test_mail_attachment.php   (host-level mail-attachment test)
 *        loris_client_config.json -> evidata block
 *        project.json -> notification_emails.evidata
 *
 * Reingestion tracking:
 *   processed/clinical/.clinical_tracking.json stores an MD5 hash per file.
 *   On each run the current file hash is compared to the stored hash:
 *     - No entry         -> first insertion, upload everything
 *     - Hash matches     -> skip (no changes)
 *     - Hash differs     -> re-ingestion, upload full file (LORIS skips already-existing rows)
 *   Use --force to bypass the hash check and always re-upload.
 *
 * Date normalization (DoB and DoD):
 *   Before upload, the DoB and DoD columns are rewritten to YYYY-MM-01
 *   per ARCHIMEDES privacy policy (day jittered to 01, missing parts
 *   default to 01). The rewrite goes into a temp file in
 *   sys_get_temp_dir(); the original source file is never modified.
 *   Hash tracking and snapshot archiving continue to use the original
 *   source file; only the upload step sees the normalized copy.
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

    // -- EviData state ---------------------------------------------------

    /**
     * Per-run directory for EviData artifacts: results JSON, report ZIPs,
     * and run_summary.json. Set when the preflight starts; null otherwise.
     */
    private ?string $evidataLogDir = null;

    /** Once-per-project guard against duplicate failure notifications. */
    private bool $evidataNotificationSent = false;

    // -- Per-run results -------------------------------------------------

    /** filename -> {status, type, time, error} */
    private array $installResults = [];

    /** filename -> {status, reason, instruments, rows, ...} */
    private array $dataResults = [];

    /** Aggregate stats across all projects in this run. */
    private array $stats = [
        'dd_files_found'        => 0,
        'dd_installed'          => 0,
        'dd_already_existed'    => 0,
        'dd_failed'             => 0,
        'data_files_found'      => 0,
        'data_uploaded'         => 0,
        'data_failed'           => 0,
        'data_skipped'          => 0,
        'rows_inserted'         => 0,  // net-new rows saved by LORIS
        'rows_existed'          => 0,  // rows already in LORIS, skipped
        'pairs_processed'       => 0,  // candidate-session pairs touched
        // EviData stats (populated by preflight; zero on disabled runs)
        'evidata_files_checked' => 0,
        'evidata_files_passed'  => 0,
        'evidata_files_failed'  => 0,
        'evidata_results'       => [],
    ];

    /**
     * CandIDs that exist in LORIS BEFORE this project's uploads begin.
     * Captured once per project in loadExistingCandidatesForProject().
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

    /**
     * Date columns that get YYYY-MM-01 normalization (case-insensitive).
     * Both DoB and DoD are jittered to the first of the month per
     * ARCHIMEDES privacy policy.
     */
    private const DATE_COLUMN_NAMES = [
        'dob', 'date_of_birth', 'birth_date',
        'dod', 'date_of_death', 'death_date',
    ];

    /**
     * Maximum total bytes of EviData attachments before the notification
     * falls back to link-only mode. Conservative — most upstream SMTP
     * relays cap at 20-25 MB after base64 inflation; 15 MB raw leaves
     * headroom.
     */
    private const EVIDATA_MAX_ATTACH_BYTES = 15 * 1024 * 1024;

    // ──────────────────────────────────────────────────────────────────
    // Constructor
    // ──────────────────────────────────────────────────────────────────

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

    public function run(array $filters = []): int
    {
        $this->logger->info("=== CLINICAL DATA INGESTION PIPELINE ===");
        $this->logger->info("Run: {$this->runTimestamp}");
        if ($this->dryRun) {
            // Loud, unmissable banner so even a glance at the first
            // screen of output makes it obvious nothing will be
            // written. Mount-failure alerts are explicitly called out
            // because they DO still fire (different audience, different
            // urgency — see MountHealthCheck::guardOrReport).
            $this->logger->info("╔══════════════════════════════════════════════════════════╗");
            $this->logger->info("║  MODE: DRY RUN                                           ║");
            $this->logger->info("║  - No data will be ingested into LORIS                   ║");
            $this->logger->info("║  - No outcome notifications will be sent                 ║");
            $this->logger->info("║  - Mount-failure alerts still go to the tech team        ║");
            $this->logger->info("║  - Run again without --dry-run to actually ingest data   ║");
            $this->logger->info("╚══════════════════════════════════════════════════════════╝");
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
     * Run the full ingestion flow for one project.
     *
     * EviData pre-flight runs FIRST. If it fails, the project aborts
     * entirely with no LORIS writes and no clinical-channel email.
     * If it passes (or is disabled), normal ingestion proceeds.
     */
    private function processProject(array $project): void
    {
        $name      = $project['project_common_name'] ?? basename($project['_projectPath']);
        $mountPath = $project['data_access']['mount_path'] ?? $project['_projectPath'];

        // ══════════════════════════════════════════════════════════════
        //  GATE 0: Mount sanity check
        //
        //  Bounded stat against the project root BEFORE any other
        //  filesystem call. If the mount is hung (NFS server down,
        //  stale handle, network glitch), filesystem syscalls block
        //  uninterruptibly at the kernel level — PHP timeouts cannot
        //  escape them.
        //
        //  guardOrReport() handles everything: bounded stat with
        //  10s SIGKILL, standardized log message, email to recipients
        //  from notification_defaults.default_on_error, and dedup so
        //  one hung mount doesn't spam N emails.
        //
        //  All values used by the email (recipients, From: address)
        //  come from the config — no hardcoded addresses in code.
        // ══════════════════════════════════════════════════════════════
        if (!MountHealthCheck::guardOrReport(
            $mountPath,
            $this->config,
            $this->logger,
            "Clinical pipeline / project {$name}"
        )) {
            $this->stats['data_failed']++;
            return;
        }

        $ddDir   = "{$mountPath}/documentation/data_dictionary";
        $dataDir = "{$mountPath}/deidentified-raw/clinical";

        $this->logDir = "{$mountPath}/logs/clinical";
        $this->openRunLog();

        $this->log("========================================");
        $this->log("Project: {$name}");
        $this->log("Run: {$this->runTimestamp}");
        $this->log("DD dir   (read-only): {$ddDir}");
        $this->log("Data dir (read-only): {$dataDir}");
        $this->log("========================================");
        $this->log("  ✓ Data accessible: {$mountPath}");

        // Reset per-project state
        $this->installResults          = [];
        $this->dataResults             = [];
        $this->evidataLogDir           = null;
        $this->evidataNotificationSent = false;

        // ══════════════════════════════════════════════════════════════
        //  GATE 1: EviData privacy pre-flight
        //
        //  Runs against every CSV/TSV in deidentified-raw/clinical/
        //  BEFORE any LORIS write. Fail-closed: any failure aborts
        //  the entire project run. Returns a three-state outcome so
        //  the caller can tell "skipped" (don't announce a PASS) apart
        //  from "actually passed".
        // ══════════════════════════════════════════════════════════════
        $evidataOutcome = $this->runEvidataPreflight($project, $mountPath, $dataDir);

        if ($evidataOutcome === 'failed') {
            $this->log("");
            $this->log("!! EviData check FAILED — clinical ingestion ABORTED for {$name}");
            $this->log("!! No DD install, no data upload, no tracking update");
            $this->stats['data_failed']++;
            $this->closeAllLogs();
            return;
        }

        // Only announce a "passed" outcome when we actually ran the
        // check. Skipped runs (disabled / no CSV files) already logged
        // their own reason inside runEvidataPreflight() — adding a
        // misleading "PASSED" line on top would imply something ran
        // and cleared, which it didn't.
        if ($evidataOutcome === 'passed') {
            $this->log("");
            $this->log("✓ EviData check PASSED — proceeding with ingestion");
            $this->log("");
        }

        $this->loadExistingCandidatesForProject();
        $this->loadTrackingFile($project);

        $this->installFromDirectory($ddDir);
        $this->uploadFromDirectory($project, $dataDir);

        $this->saveTrackingFile();

        $this->writeProjectSummary($name);
        $this->sendNotification($project);
    }

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
    //  EviData pre-flight
    //
    //  Three-step flow:
    //    A. resolveEvidataConfig() — pull merged config, or null if
    //       disabled at the global level (silent skip).
    //    B. validateEvidataQiHeaders() — local check that every CSV has
    //       the configured QI columns, BEFORE any network call.
    //    C. EviDataClient::checkBatch() — upload, score, fetch results
    //       and report ZIPs.
    //
    //  After each step, artifacts are persisted under
    //  {mount_path}/logs/evidata/{timestamp}/, regardless of run outcome.
    //  Failure triggers a dedicated email (with ZIPs attached, when size
    //  allows) to the project's evidata recipients.
    // ══════════════════════════════════════════════════════════════════

    /**
     * Run the EviData pre-flight against every CSV/TSV in $clinicalDir.
     *
     * Three-state return so the caller can distinguish "we skipped this"
     * from "we ran and everything cleared". Both used to be `true`, which
     * led to a misleading "✓ EviData check PASSED" message on disabled
     * runs. Now:
     *
     *   'skipped' — EviData is disabled / no clinical dir / no CSV files
     *               Caller should proceed silently with no PASS message.
     *   'passed'  — Every file passed the check.
     *               Caller may announce the PASS to the run log.
     *   'failed'  — One or more files failed, or a client-setup error.
     *               Caller MUST abort downstream ingestion.
     *
     * Side effects on failure:
     *   - stats['evidata_*'] populated for the run-log summary
     *   - per-file artifacts written to logs/evidata/{timestamp}/
     *   - run_summary.json written
     *   - failure email sent to project's evidata recipients
     */
    private function runEvidataPreflight(array $project, string $mountPath, string $clinicalDir): string
    {
        $evi = $this->resolveEvidataConfig();
        if ($evi === null) {
            $this->log("  EviData: not enabled in evidata_config.json — skipping pre-flight");
            return 'skipped';
        }

        if (!is_dir($clinicalDir)) {
            $this->log("  EviData: no clinical dir at {$clinicalDir} — nothing to check");
            return 'skipped';
        }

        $csvFiles = array_merge(
            glob("{$clinicalDir}/*.csv") ?: [],
            glob("{$clinicalDir}/*.tsv") ?: []
        );
        sort($csvFiles);

        if (empty($csvFiles)) {
            $this->log("  EviData: no CSV/TSV files in {$clinicalDir} — nothing to check");
            return 'skipped';
        }

        $this->log("");
        $this->log("──── EVIDATA PRE-FLIGHT" . ($this->dryRun ? " [DRY RUN]" : "") . " ────");
        $this->log("  Checking " . count($csvFiles) . " file(s) against EviData");
        $this->log("  API endpoint: {$evi['api_base_url']}");

        // Open the per-run artifact directory before any check runs.
        $artifactDir = $this->openEvidataLogDir($mountPath);
        $this->log("  Artifact dir: {$artifactDir}");

        // ── Step A: local QI presence check ─────────────────────────
        // Cheap (no network) and catches config drift before paying
        // for upload + report. A mismatch here means either the CSV
        // is missing a column or evidata.qis in config is wrong.
        $qiErrors = $this->validateEvidataQiHeaders($csvFiles, $evi['qis']);
        if (!empty($qiErrors)) {
            foreach ($qiErrors as $name => $missing) {
                $this->stats['evidata_files_checked']++;
                $this->stats['evidata_files_failed']++;
                $errMsg = ($missing === ['__unreadable__'])
                    ? 'CSV unreadable'
                    : 'Configured QI columns missing in CSV: ' . implode(', ', $missing);
                $result = [
                    'passed'     => false,
                    'error'      => $errMsg,
                    'report_id'  => null,
                    'results'    => null,
                    'report_zip' => null,
                ];
                $this->stats['evidata_results'][$name] = $result;
                $this->log("  ✗ {$name} — {$errMsg}");
                $this->writeError('evidata', "{$name}: {$errMsg}");
                $this->persistEvidataArtifacts($name, $result);
            }
            $this->writeEvidataRunSummary($project, false);
            $this->sendEvidataFailureNotification($project);
            return 'failed';
        }

        // ── Step B: remote check via EviData API ────────────────────
        // checkBatch() captures per-file exceptions internally. The
        // try/catch here is only for client setup errors (constructor,
        // missing env vars, etc) — anything that prevents the batch
        // from running at all.
        try {
            $client  = new EviDataClient($evi);
            $results = $client->checkBatch($csvFiles);
        } catch (\Throwable $e) {
            $this->log("  !! EviData client setup error: " . $e->getMessage());
            $this->writeError('evidata', "Client setup error: " . $e->getMessage());
            $this->stats['evidata_results']['__client_error__'] = [
                'passed'     => false,
                'error'      => $e->getMessage(),
                'report_id'  => null,
                'results'    => null,
                'report_zip' => null,
            ];
            $this->writeEvidataRunSummary($project, false);
            $this->sendEvidataFailureNotification(
                $project,
                "EviData client error before any file could be checked:\n" . $e->getMessage()
            );
            return 'failed';
        }

        // ── Step C: process each file's result, persist artifacts ──
        $allPassed = true;
        foreach ($results as $name => $r) {
            $this->stats['evidata_files_checked']++;
            if ($r['passed']) {
                $this->stats['evidata_files_passed']++;
                $this->log("  ✓ {$name} (report_id={$r['report_id']})");
            } else {
                $this->stats['evidata_files_failed']++;
                $allPassed = false;
                $detail = $r['error'] !== null
                    ? "ERROR: {$r['error']}"
                    : "overall_passed=false (report_id={$r['report_id']})";
                $this->log("  ✗ {$name} — {$detail}");
                $this->writeError('evidata', "{$name}: {$detail}");
            }
            $this->stats['evidata_results'][$name] = $r;

            $written = $this->persistEvidataArtifacts($name, $r);
            foreach ($written as $path) {
                $this->log("    artifact: " . basename($path));
            }
        }

        // Roll-up summary regardless of outcome.
        $this->writeEvidataRunSummary($project, $allPassed);

        if (!$allPassed) {
            $this->log("");
            $this->log("  !! EviData pre-flight FAILED "
                . "({$this->stats['evidata_files_failed']} of "
                . "{$this->stats['evidata_files_checked']} file(s))");
            $this->log("  !! Artifacts: {$this->evidataLogDir}");
            $this->sendEvidataFailureNotification($project);
            return 'failed';
        }

        $this->log("  ✓ All " . count($csvFiles) . " file(s) passed EviData");
        $this->log("  ✓ Audit artifacts: {$this->evidataLogDir}");
        return 'passed';
    }

    /**
     * Resolve the global EviData config from loris_client_config.json.
     * Returns null when disabled / missing — caller treats null as
     * "skip pre-flight" and ingestion proceeds normally.
     *
     * Throws if EviData is enabled but 'qis' is missing — that's almost
     * certainly a misconfiguration, and silently passing every file
     * would give privacy theatre instead of a real check.
     */
    private function resolveEvidataConfig(): ?array
    {
        $evi = $this->config['evidata'] ?? [];

        if (empty($evi['enabled'])) {
            return null;
        }
        if (empty($evi['qis']) || !is_array($evi['qis'])) {
            throw new \RuntimeException(
                "evidata.qis missing from loris_client_config.json — "
                . "list the QI columns shared by all clinical CSVs, "
                . "or set evidata.enabled=false to disable the privacy check."
            );
        }
        return $evi;
    }

    /**
     * Local pre-check: verify every configured QI column exists in
     * every CSV/TSV's header row before any network call.
     *
     * Returns a map of {basename -> [missing columns]} for failing
     * files. Empty array = all files are good to send to EviData.
     * An unreadable file is reported as ['__unreadable__'] for a
     * clean error message.
     */
    private function validateEvidataQiHeaders(array $csvFiles, array $qis): array
    {
        $bad = [];
        foreach ($csvFiles as $path) {
            $delim   = str_ends_with(strtolower($path), '.tsv') ? "\t" : ',';
            $fh      = @fopen($path, 'r');
            if ($fh === false) {
                $bad[basename($path)] = ['__unreadable__'];
                continue;
            }
            $headers = fgetcsv($fh, 0, $delim);
            fclose($fh);
            if (!is_array($headers)) {
                $bad[basename($path)] = ['__unreadable__'];
                continue;
            }
            $headers = array_map('trim', $headers);
            $missing = array_values(array_diff($qis, $headers));
            if (!empty($missing)) {
                $bad[basename($path)] = $missing;
            }
        }
        return $bad;
    }

    // ══════════════════════════════════════════════════════════════════
    //  EviData artifact persistence + notification
    //
    //  Per-run audit trail under {mount_path}/logs/evidata/{timestamp}/.
    //  Independent of run outcome — pass or fail, the artifacts persist.
    //  Passing runs leave evidence the gate ran cleanly; failing runs
    //  leave the reports compliance needs to remediate.
    // ══════════════════════════════════════════════════════════════════

    /**
     * Open the per-run artifact directory and remember it for the rest
     * of the project's preflight.
     */
    private function openEvidataLogDir(string $mountPath): string
    {
        $dir = rtrim($mountPath, '/') . "/logs/evidata/{$this->runTimestamp}";
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }
        $this->evidataLogDir = $dir;
        return $dir;
    }

    /**
     * Persist one file's artifacts to disk. Writes whatever subset is
     * available: results.json (when EviData returned a payload),
     * report.zip (when EviData returned the bundle), error.json (when
     * the file errored).
     *
     * Returns the absolute paths of files actually written so the
     * caller can log them.
     */
    private function persistEvidataArtifacts(string $sourceName, array $result): array
    {
        if ($this->evidataLogDir === null) {
            return [];
        }

        // Strip extension so we don't end up with names like
        // "redcap_data.csv.results.json".
        $stem    = pathinfo($sourceName, PATHINFO_FILENAME);
        $written = [];

        if (!empty($result['results']) && is_array($result['results'])) {
            $path = "{$this->evidataLogDir}/{$stem}.results.json";
            file_put_contents(
                $path,
                json_encode($result['results'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
            );
            $written[] = $path;
        }

        if (!empty($result['report_zip'])) {
            $path = "{$this->evidataLogDir}/{$stem}.report.zip";
            file_put_contents($path, $result['report_zip']);
            $written[] = $path;
        }

        if ($result['error'] !== null) {
            $path = "{$this->evidataLogDir}/{$stem}.error.json";
            file_put_contents($path, json_encode([
                'source_file' => $sourceName,
                'error'       => $result['error'],
                'report_id'   => $result['report_id'] ?? null,
                'timestamp'   => date('c'),
            ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
            $written[] = $path;
        }
        return $written;
    }

    /**
     * Write the top-level run_summary.json roll-up. Called once at the
     * end of every preflight run (pass or fail). Operators read this
     * first when triaging "what happened with EviData this run".
     */
    private function writeEvidataRunSummary(array $project, bool $allPassed): void
    {
        if ($this->evidataLogDir === null) {
            return;
        }

        $perFile = [];
        foreach (($this->stats['evidata_results'] ?? []) as $name => $r) {
            $perFile[$name] = [
                'passed'    => $r['passed']    ?? false,
                'report_id' => $r['report_id'] ?? null,
                'error'     => $r['error']     ?? null,
            ];
        }

        $summary = [
            'project'        => $project['project_common_name']
                ?? basename($project['_projectPath']),
            'run_timestamp'  => $this->runTimestamp,
            'completed_at'   => date('c'),
            'dry_run'        => $this->dryRun,
            'overall_passed' => $allPassed,
            'files_checked'  => $this->stats['evidata_files_checked'] ?? 0,
            'files_passed'   => $this->stats['evidata_files_passed']  ?? 0,
            'files_failed'   => $this->stats['evidata_files_failed']  ?? 0,
            'results'        => $perFile,
        ];

        file_put_contents(
            "{$this->evidataLogDir}/run_summary.json",
            json_encode($summary, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
        );
    }

    /**
     * Send the EviData failure notification.
     *
     * Bypasses LORIS\Utils\Notification (which is text-only) and uses
     * PHP mail() directly so we can attach the report ZIPs for failed
     * files. Confirmed working on this host via
     * scripts/test_mail_attachment.php.
     *
     * Recipient lookup: project's evidata.on_check_failed list first,
     * then the global default_on_evidata_failed fallback.
     */
    private function sendEvidataFailureNotification(
        array $project,
        ?string $clientErrorOverride = null
    ): void {
        if ($this->evidataNotificationSent) {
            return;
        }

        $recipients = $project['notification_emails']['evidata']['on_check_failed']
            ?? $this->config['notification_defaults']['default_on_evidata_failed']
            ?? [];

        if (empty($recipients)) {
            $this->log("  No EviData failure recipients configured — not emailing");
            return;
        }

        $projectName = $project['project_common_name']
            ?? basename($project['_projectPath']);
        $subject     = "PRIVACY CHECK FAILED: {$projectName} Clinical Pipeline";
        $body        = $this->buildEvidataFailureBody($projectName, $clientErrorOverride);

        // ── Collect ZIPs for failed files only ──────────────────────
        // Compliance opens the failed report, not all of them. Passing
        // files already have artifacts on disk and don't need email.
        $attachments = [];
        $totalBytes  = 0;
        if ($clientErrorOverride === null && $this->evidataLogDir !== null) {
            foreach (($this->stats['evidata_results'] ?? []) as $name => $r) {
                if ($r['passed'] ?? false) {
                    continue;
                }
                $stem = pathinfo($name, PATHINFO_FILENAME);
                $zip  = "{$this->evidataLogDir}/{$stem}.report.zip";
                if (is_file($zip)) {
                    $attachments[] = [
                        'path' => $zip,
                        'name' => "{$stem}_evidata_report.zip",
                    ];
                    $totalBytes += filesize($zip);
                }
            }
        }

        // ── Size guard ─────────────────────────────────────────────
        // Fall back to a link-only body when total payload exceeds the
        // ceiling. Better than letting the MTA reject the message.
        if ($totalBytes > self::EVIDATA_MAX_ATTACH_BYTES) {
            $mb = round($totalBytes / 1024 / 1024, 1);
            $this->log("  EviData attachments total {$mb}MB — over "
                . (self::EVIDATA_MAX_ATTACH_BYTES / 1024 / 1024)
                . "MB ceiling, sending link-only email");
            $body .= "\n\nReports too large to attach by email ({$mb}MB total).\n"
                . "Find them on the pipeline host at:\n"
                . "  {$this->evidataLogDir}\n";
            $attachments = [];
        } elseif (!empty($attachments)) {
            $body .= "\n\nReport ZIPs for failed files are attached.\n"
                . "Full audit artifacts (including passing reports) at:\n"
                . "  {$this->evidataLogDir}\n";
        } elseif ($this->evidataLogDir !== null) {
            $body .= "\n\nAudit artifacts at:\n  {$this->evidataLogDir}\n";
        }

        // ── Dry-run preview ─────────────────────────────────────────
        if ($this->dryRun) {
            $this->log("");
            $this->log("  ── EviData notification [DRY RUN — not sent] ──");
            $this->log("  To         : " . implode(', ', $recipients));
            $this->log("  Subject    : {$subject}");
            $this->log("  Attachments: " . (empty($attachments)
                    ? "(none)"
                    : count($attachments) . " file(s), " . round($totalBytes / 1024) . " KB"));
            $this->log("  Body:");
            foreach (preg_split('/\R/', $body) as $line) {
                $this->log("    {$line}");
            }
            $this->evidataNotificationSent = true;
            return;
        }

        // ── Send ───────────────────────────────────────────────────
        $attachLabel = empty($attachments)
            ? ''
            : ' (with ' . count($attachments) . ' attachment(s), '
            . round($totalBytes / 1024) . ' KB)';
        $this->log("  Sending EviData failure notification to: "
            . implode(', ', $recipients) . $attachLabel);

        foreach ($recipients as $to) {
            $ok = $this->sendEvidataMailWithAttachments($to, $subject, $body, $attachments);
            if (!$ok) {
                $this->writeError('evidata-mail', "Local MTA rejected message for {$to}");
            }
        }
        $this->evidataNotificationSent = true;
    }

    /**
     * Send mail with optional multipart attachments via PHP mail().
     *
     * Why direct mail():
     *   LORIS\Utils\Notification is text-only. We can't modify it
     *   (LORIS upstream). Rather than fork, this one notification
     *   uses mail() directly for the attachment path. All other
     *   pipeline notifications continue to use Notification::send().
     *
     * Returns true if mail() accepted the message for delivery.
     * False means the local MTA refused — not that the recipient
     * didn't receive it. Caller logs either way.
     */
    private function sendEvidataMailWithAttachments(
        string $to,
        string $subject,
        string $body,
        array $attachments
    ): bool {
        // No attachments → simple text email shape.
        if (empty($attachments)) {
            $headers = "From: " . $this->evidataFromAddress() . "\r\n"
                . "Content-Type: text/plain; charset=UTF-8\r\n";
            return mail($to, $subject, $body, $headers);
        }

        $boundary = '=_evidata_' . md5(uniqid('', true));
        $headers  = "From: " . $this->evidataFromAddress() . "\r\n"
            . "MIME-Version: 1.0\r\n"
            . "Content-Type: multipart/mixed; boundary=\"{$boundary}\"\r\n";

        // Part 1: plain-text body
        $message  = "--{$boundary}\r\n"
            . "Content-Type: text/plain; charset=UTF-8\r\n"
            . "Content-Transfer-Encoding: 8bit\r\n\r\n"
            . $body . "\r\n";

        // Part 2..N: attachments
        foreach ($attachments as $att) {
            $path = $att['path'] ?? null;
            if (empty($path) || !is_readable($path)) {
                continue;
            }
            $name     = $att['name'] ?? basename($path);
            $contents = file_get_contents($path);
            if ($contents === false) {
                continue;
            }
            $encoded  = chunk_split(base64_encode($contents), 76, "\r\n");
            $mime     = $this->mimeForPath($path);

            $message .= "--{$boundary}\r\n"
                . "Content-Type: {$mime}; name=\"{$name}\"\r\n"
                . "Content-Transfer-Encoding: base64\r\n"
                . "Content-Disposition: attachment; filename=\"{$name}\"\r\n\r\n"
                . $encoded . "\r\n";
        }
        $message .= "--{$boundary}--\r\n";

        return mail($to, $subject, $message, $headers);
    }

    /**
     * Resolve From: address. Prefers an explicit config value so
     * deployments can set a real domain for better deliverability;
     * falls back to a hostname-based default.
     */
    private function evidataFromAddress(): string
    {
        return $this->config['evidata']['from_address']
            ?? ('archimedes-pipeline@' . (gethostname() ?: 'localhost'));
    }

    /**
     * Lightweight extension-to-MIME mapping. Avoids the fileinfo
     * extension dependency that mime_content_type() needs.
     */
    private function mimeForPath(string $path): string
    {
        return match (strtolower(pathinfo($path, PATHINFO_EXTENSION))) {
            'zip'  => 'application/zip',
            'pdf'  => 'application/pdf',
            'json' => 'application/json',
            'csv'  => 'text/csv',
            'txt'  => 'text/plain',
            default => 'application/octet-stream',
        };
    }

    /**
     * Construct the email body for the EviData failure notification.
     * Used by both the live send path and the dry-run preview.
     */
    private function buildEvidataFailureBody(
        string $projectName,
        ?string $clientErrorOverride
    ): string {
        $body  = "EviData privacy pre-flight check failed for clinical ingestion.\n";
        $body .= "Project   : {$projectName}\n";
        $body .= "Run       : {$this->runTimestamp}"
            . ($this->dryRun ? " (DRY RUN)" : "") . "\n";
        $body .= "Timestamp : " . date('Y-m-d H:i:s') . "\n";
        $body .= "Status    : " . ($this->dryRun
                ? "PREVIEW — no LORIS writes attempted (dry run)"
                : "INGESTION ABORTED — no records written to LORIS") . "\n\n";

        if ($clientErrorOverride !== null) {
            $body .= "Client error:\n{$clientErrorOverride}\n\n";
            $body .= $this->evidataActionFooter();
            return $body;
        }

        $body .= "Per-file results:\n" . str_repeat('-', 50) . "\n";
        foreach (($this->stats['evidata_results'] ?? []) as $name => $r) {
            $mark = $r['passed'] ? '✔' : '✗';
            if ($r['passed']) {
                $detail = "(report_id={$r['report_id']})";
            } elseif ($r['error'] !== null) {
                $detail = "ERROR: {$r['error']}";
            } else {
                $detail = "overall_passed=false (report_id={$r['report_id']})";
            }
            $body .= "  {$mark} {$name}  {$detail}\n";
        }
        $body .= "\n" . $this->evidataActionFooter();

        if ($this->runLogPath) {
            $body .= "\nClinical run log: {$this->runLogPath}\n";
        }

        return $body;
    }

    private function evidataActionFooter(): string
    {
        return "Action required: review the failed report(s) in the EviData UI\n"
            . "and re-export the dataset with adequate de-identification before\n"
            . "re-running the clinical pipeline.\n";
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
                $tracked      = $this->trackingData[$filename] ?? [];
                $lastInserted = $tracked['rows_saved']       ?? null;
                $lastExisted  = $tracked['rows_existed']     ?? null;
                $lastRun      = $tracked['last_uploaded_at'] ?? null;
                $lastTotal    = isset($lastInserted, $lastExisted)
                    ? $lastInserted + $lastExisted : null;

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

            $this->processOneDataFile($project, $f, $changeStatus);
        }
    }

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

        $uploadPath = $this->normalizeDatesInFile($filePath, $format);
        $usingTemp  = ($uploadPath !== $filePath);

        try {
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
            if ($usingTemp && file_exists($uploadPath)) {
                @unlink($uploadPath);
            }
        }
    }

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

    private function tallyUploadSuccess(array $ui): void
    {
        $this->stats['data_uploaded']++;
        $this->stats['rows_inserted']    += $ui['rows_saved']   ?? 0;
        $this->stats['rows_existed']     += $ui['rows_existed'] ?? 0;
        $this->stats['pairs_processed']  += $ui['pairs']        ?? 0;
    }

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
    //  Hash-based reingestion tracking
    // ══════════════════════════════════════════════════════════════════

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

        if (copy($src, $target)) {
            $this->log("    Snapshot archived -> processed/clinical/"
                . date('Y-m-d') . "/" . basename($target));
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  Run log + error log
    // ══════════════════════════════════════════════════════════════════

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

    private function log(string $msg): void
    {
        // Prefix every log line in dry-run mode so partial log snippets
        // are unambiguous AND the on-disk run-log file is marked too.
        // A line viewed in isolation should always be self-identifying
        // about which mode it came from.
        $prefixed = $this->dryRun ? "[DRY RUN] {$msg}" : $msg;

        $this->logger->info($prefixed);

        if ($this->runLogFh) {
            $ts = date('H:i:s');
            fwrite($this->runLogFh, "[{$ts}] {$prefixed}\n");
        }
    }

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

    private function writeProjectSummary(string $projectName): void
    {
        $this->log("");
        $this->log("──── PROJECT SUMMARY: {$projectName} ────");

        // EviData summary (if the gate ran)
        if (($this->stats['evidata_files_checked'] ?? 0) > 0) {
            $this->log("");
            $this->log("  EVIDATA PRE-FLIGHT:");
            $this->log("    Files checked:     {$this->stats['evidata_files_checked']}");
            $this->log("    Files passed:      {$this->stats['evidata_files_passed']}");
            $this->log("    Files failed:      {$this->stats['evidata_files_failed']}");
            if ($this->evidataLogDir !== null) {
                $this->log("    Artifacts:         {$this->evidataLogDir}");
            }
        }

        // Install summary
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

        // Data upload summary
        $this->log("");
        $this->log("  DATA INGESTION:");

        if (empty($this->dataResults)) {
            $this->log("    (no data files)");
        } else {
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

        // Candidate breakdown
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

    private function writeFinalSummary(): void
    {
        $s = $this->stats;

        $this->log("");
        $this->log("========================================");
        $this->log("PIPELINE RUN SUMMARY");
        $this->log("========================================");
        $this->log("  Run: {$this->runTimestamp}");
        $this->log("");

        if (($s['evidata_files_checked'] ?? 0) > 0) {
            $this->log("  EviData pre-flight:");
            $this->log("    Files checked:      {$s['evidata_files_checked']}");
            $this->log("    Files passed:       {$s['evidata_files_passed']}");
            $this->log("    Files failed:       {$s['evidata_files_failed']}");
            $this->log("");
        }

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
    //  Clinical-channel email notification
    //
    //  Sent at the end of every project run, on success or failure.
    //  EviData failures bypass this — they have their own notification
    //  via sendEvidataFailureNotification().
    //
    //  Mount-failure emails are handled separately by
    //  LORIS\Utils\MountHealthCheck::guardOrReport() — they fire from
    //  inside the utility before this method is reached, using the
    //  same notification_defaults.default_on_error recipient list.
    // ══════════════════════════════════════════════════════════════════

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

        $body  = "Project: {$name}\n";
        $body .= "Modality: clinical\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n";
        $body .= "Run: {$this->runTimestamp}\n";
        if ($this->force) {
            $body .= "Mode: FORCE (hash check bypassed)\n";
        }
        $body .= "\n";

        // EviData section (only when the gate ran)
        if (($s['evidata_files_checked'] ?? 0) > 0) {
            $body .= "EviData Pre-flight:\n";
            $body .= "  Files checked: {$s['evidata_files_checked']}, "
                . "passed: {$s['evidata_files_passed']}, "
                . "failed: {$s['evidata_files_failed']}\n";
            if ($this->evidataLogDir !== null) {
                $body .= "  Artifacts: {$this->evidataLogDir}\n";
            }
            $body .= "\n";
        }

        // Instrument Installation
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

        // Data Ingestion
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

        // Totals
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

        // Candidates breakdown
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

        // Status message
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

        // Log paths
        $body .= "\n";
        if ($this->runLogPath) {
            $body .= "Run log: {$this->runLogPath}\n";
        }
        if ($this->errorLogPath) {
            $body .= "Error log: {$this->errorLogPath}\n";
        }

        // Send
        $this->log("  Sending notification to: " . implode(', ', $emailsToSend));

        // In dry-run mode, log what WOULD be sent and bail. Subject is
        // enough to confirm "right project, right outcome label" — the
        // full body is already in the run log via the earlier $this->log()
        // calls that built up the summary. Mount-failure alerts (from
        // MountHealthCheck) are NOT suppressed in dry-run, because those
        // are infrastructure alerts that go to the tech team regardless
        // of pipeline mode.
        if ($this->dryRun) {
            $this->log("  [no email sent — dry run]");
            $this->log("  Subject would be: {$subject}");
            return;
        }

        foreach ($emailsToSend as $to) {
            try {
                $this->notification->send($to, $subject, $body);
            } catch (\Exception $e) {
                $this->writeError('notification', "Failed to send to {$to}: " . $e->getMessage());
            }
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
    //  Date normalization (DoB and DoD)
    // ══════════════════════════════════════════════════════════════════

    private function normalizeDatesInFile(string $srcPath, string $format): string
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

        $dateCols = [];
        foreach ($headers as $i => $h) {
            $norm = strtolower(trim((string)$h));
            if (in_array($norm, self::DATE_COLUMN_NAMES, true)) {
                $dateCols[$i] = (string)$h;
            }
        }

        if (empty($dateCols)) {
            fclose($in);
            return $srcPath;
        }

        $tmpPath = tempnam(sys_get_temp_dir(), 'clinical_dates_') . '_' . basename($srcPath);
        $out     = fopen($tmpPath, 'w');
        fputcsv($out, $headers, $delimiter);

        $changedPerCol = array_fill_keys(array_keys($dateCols), 0);
        $total         = 0;

        while (($row = fgetcsv($in, 0, $delimiter)) !== false) {
            $total++;
            foreach ($dateCols as $idx => $_label) {
                if (array_key_exists($idx, $row)) {
                    $orig = (string)$row[$idx];
                    $norm = $this->normalizeDateValue($orig);
                    if ($norm !== $orig) {
                        $changedPerCol[$idx]++;
                    }
                    $row[$idx] = $norm;
                }
            }
            fputcsv($out, $row, $delimiter);
        }

        fclose($in);
        fclose($out);

        $parts = [];
        foreach ($dateCols as $idx => $label) {
            $parts[] = "{$label} {$changedPerCol[$idx]}/{$total}";
        }
        $this->log("    Date columns normalized: " . implode(', ', $parts)
            . " row(s) rewritten to YYYY-MM-01");

        return $tmpPath;
    }

    private function normalizeDateValue(string $value): string
    {
        $value = trim($value);
        if ($value === '') {
            return $value;
        }

        if (preg_match('/^(\d{4})-(\d{2})-\d{2}$/', $value, $m)) {
            return "{$m[1]}-{$m[2]}-01";
        }
        if (preg_match('/^(\d{4})-(\d{2})$/', $value, $m)) {
            return "{$m[1]}-{$m[2]}-01";
        }
        if (preg_match('/^(\d{4})$/', $value, $m)) {
            return "{$m[1]}-01-01";
        }

        return $value;
    }

    // ══════════════════════════════════════════════════════════════════
    //  Project discovery
    //
    //  Walks the configured collections, reads each project.json,
    //  returns a list of project descriptors. Mount-checks every
    //  collection's base_path FIRST so a hung NFS mount can't wedge
    //  the discovery phase — without this, file_exists() on the
    //  project.json hangs uninterruptibly when the mount is dead,
    //  before processProject() (and its own mount check) ever runs.
    // ══════════════════════════════════════════════════════════════════

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

            // Mount-check the collection's base_path BEFORE any
            // filesystem call against its children. If the mount is
            // hung, guardOrReport() logs + emails + dedups, and we
            // skip the entire collection.
            if (!MountHealthCheck::guardOrReport(
                $coll['base_path'],
                $this->config,
                $this->logger,
                "Clinical pipeline / collection '{$coll['name']}'"
            )) {
                continue;
            }
            $basePath = $coll['base_path'];

            foreach ($coll['projects'] ?? [] as $pc) {
                if (!($pc['enabled'] ?? true)) {
                    continue;
                }
                if (isset($filters['project']) && $pc['name'] !== $filters['project']) {
                    continue;
                }

                $path = $basePath . '/' . $pc['name'];
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