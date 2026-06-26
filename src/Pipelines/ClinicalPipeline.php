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
 *   pipeline NEVER writes to deidentified-raw/clinical/ or
 *   documentation/data_dictionary/. It only writes to its own
 *   subdirectories: processed/clinical/, logs/clinical/, logs/evidata/.
 *
 * EviData privacy pre-flight gate — quasi-identifier (qis) resolution:
 *   QI lists are resolved with the following precedence:
 *     1. project.json -> evidata.qis  (per-project override, non-empty;
 *        the project fully owns its QI policy when present).
 *     2. config/evidata_config.json -> qis  (global baseline, non-empty;
 *        used only when a project does not define its own).
 *     3. ALL-HEADERS DEFAULT (privacy-policy approved): when qis is
 *        empty/absent at BOTH levels, every CSV column header is used as
 *        a QI, per file, MINUS:
 *          - columns listed in evidata.exclude_qis (two-level,
 *            case-insensitive), and
 *          - columns whose every value is unique-per-row (identifiers /
 *            pseudonyms / timestamps), removed automatically because
 *            EviData treats them as QIs and they force maximum risk.
 *   Levels 1 and 2 each accept a flat array OR a map ('_default' +
 *   per-filename). EviData's server-side QI validation is case-sensitive.
 *   The per-file log records which columns were used and which excluded.
 *
 * Reingestion tracking, date normalization, logging: see per-method docs.
 */
class ClinicalPipeline
{
    private array $config;
    private LoggerInterface $logger;
    private ClinicalClient $client;
    private Notification $notification;

    private bool $dryRun;
    private bool $verbose;
    private bool $force;

    private string $runTimestamp;

    /** @var resource|null */
    private $errorFh = null;
    private ?string $errorLogPath = null;

    /** @var resource|null */
    private $runLogFh = null;
    private ?string $runLogPath = null;

    private ?string $logDir = null;

    private array $trackingData = [];
    private ?string $trackingFilePath = null;

    private ?string $evidataLogDir = null;
    private bool $evidataNotificationSent = false;

    /**
     * Basenames of files that did NOT pass EviData this project (failed
     * verdict or errored). Populated by runEvidataPreflight(); consumed
     * by uploadFromDirectory() to skip them. Passed files ingest; these
     * are skipped and retried next run.
     */
    private array $evidataFailedFiles = [];

    private array $installResults = [];
    private array $dataResults = [];

    private array $stats = [
        'dd_files_found'        => 0,
        'dd_installed'          => 0,
        'dd_already_existed'    => 0,
        'dd_failed'             => 0,
        'data_files_found'      => 0,
        'data_uploaded'         => 0,
        'data_failed'           => 0,
        'data_skipped'          => 0,
        'rows_inserted'         => 0,
        'rows_existed'          => 0,
        'pairs_processed'       => 0,
        'evidata_files_checked' => 0,
        'evidata_files_passed'  => 0,
        'evidata_files_failed'  => 0,
        'evidata_results'       => [],
    ];

    private array $existingCandIdsAtProjectStart = [];
    private bool $candidateClassificationAvailable = false;

    private const DD_EXTENSIONS = [
        'csv'   => 'redcap',
        'linst' => 'linst',
        'json'  => 'bids',
    ];

    private const DATA_EXTENSIONS = [
        'csv' => 'LORIS_CSV',
        'tsv' => 'BIDS_TSV',
    ];

    private const DEFAULT_EXCLUDE_FORMS = ['nip_connector', 'project_request_form'];

    /** Key in a qis map supplying the QI list for files with no exact match. */
    private const QIS_DEFAULT_KEY = '_default';

    /**
     * Sentinel value for a per-file qis map entry meaning "use ALL of
     * this file's headers" (resolved via resolveAllHeaderQis(), so
     * exclude_qis and unique-per-row pruning still apply). Valid ONLY
     * as a per-filename value inside a qis map — not for '_default'
     * and not as a flat-list element.
     */
    private const QIS_ALL_HEADERS = '*';

    private const DATE_COLUMN_NAMES = [
        'dob', 'date_of_birth', 'birth_date',
        'dod', 'date_of_death', 'death_date',
    ];

    /**
     * Default MTA message_size_limit in MB, used when the host's
     * evidata_config.json does not set evidata.mta_message_size_limit_mb.
     * The real per-host value (from `postconf message_size_limit`) should
     * be configured per server — msruthy-dev, the EviData host, and the
     * production targets may each differ. See maxAttachBytes().
     */
    private const EVIDATA_DEFAULT_MTA_LIMIT_MB = 10;

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

    public function run(array $filters = []): int
    {
        $this->logger->info("=== CLINICAL DATA INGESTION PIPELINE ===");
        $this->logger->info("Run: {$this->runTimestamp}");
        if ($this->dryRun) {
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

            return ($this->stats['data_failed'] > 0
                || $this->stats['dd_failed'] > 0
                || ($this->stats['evidata_files_failed'] ?? 0) > 0) ? 1 : 0;

        } catch (\Exception $e) {
            $this->writeError("FATAL", $e->getMessage());
            $this->logger->debug($e->getTraceAsString());
            $this->closeAllLogs();
            return 1;
        }
    }

    private function processProject(array $project): void
    {
        $name      = $project['project_common_name'] ?? basename($project['_projectPath']);
        $mountPath = $project['data_access']['mount_path'] ?? $project['_projectPath'];

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

        $this->installResults          = [];
        $this->dataResults             = [];
        $this->evidataLogDir           = null;
        $this->evidataNotificationSent = false;
        $this->evidataFailedFiles      = [];

        // Per-file privacy gate: each file is checked individually.
        // Files that pass are ingested; files that fail (bad verdict)
        // or error (no verdict) are skipped and retried next run. The
        // project is NOT aborted as a whole — passing files proceed.
        $evidataOutcome = $this->runEvidataPreflight($project, $mountPath, $dataDir);

        if ($evidataOutcome === 'all_passed') {
            $this->log("");
            $this->log("✓ EviData check PASSED for all files — proceeding with ingestion");
            $this->log("");
        } elseif ($evidataOutcome === 'partial') {
            $this->log("");
            $this->log("⚠ EviData: some files failed — ingesting only the files that passed");
            $this->log("  Skipped (failed/errored EviData): "
                . implode(', ', $this->evidataFailedFiles));
            $this->log("");
        } elseif ($evidataOutcome === 'all_failed') {
            $this->log("");
            $this->log("✗ EviData: all files failed — nothing will be ingested");
            $this->log("");
        }
        // 'skipped' (gate disabled / no files) falls through to normal
        // ingestion with no EviData restriction.

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
    // ══════════════════════════════════════════════════════════════════

    /**
     * Run the EviData pre-flight against every CSV/TSV in $clinicalDir,
     * PER FILE. Records the basenames that did NOT pass in
     * $this->evidataFailedFiles so ingestion can skip them.
     *
     * Returns one of:
     *   'skipped'    — gate disabled / no clinical dir / no CSV files
     *   'all_passed' — every file passed
     *   'partial'    — some passed, some failed/errored
     *   'all_failed' — no file passed
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

        // ── Choose the QI source for this project ───────────────────
        // Returns a config (array) for explicit qis, or null to signal
        // ALL-HEADERS mode (no qis defined at project or global level).
        try {
            [$qisConfig, $qisSource] = $this->resolveProjectQisConfig($project, $evi);
        } catch (\RuntimeException $e) {
            // QI policy is broken for the whole project — no file can be
            // assessed, so every file fails (nothing ingests).
            $this->log("  !! EviData QI config error: " . $e->getMessage());
            $this->writeError('evidata', "QI config error: " . $e->getMessage());
            $this->openEvidataLogDir($mountPath);
            $this->stats['evidata_results']['__config_error__'] = [
                'passed'     => false,
                'error'      => $e->getMessage(),
                'report_id'  => null,
                'results'    => null,
                'report_zip' => null,
            ];
            foreach ($csvFiles as $p) {
                $this->evidataFailedFiles[] = basename($p);
            }
            $this->writeEvidataRunSummary($project, false);
            $this->sendEvidataFailureNotification(
                $project,
                "EviData QI configuration error — preflight could not run:\n" . $e->getMessage()
            );
            return 'all_failed';
        }
        $this->log("  QI policy source: {$qisSource}");

        $artifactDir = $this->openEvidataLogDir($mountPath);
        $this->log("  Log dir: {$artifactDir}");

        // ── Resolve the QI list per file up front ───────────────────
        // In all-headers mode ($qisConfig === null) each file's headers
        // are read and pruned; otherwise the explicit config is used.
        $excludeSet   = $this->resolveExcludeQis($project, $evi);
        $qisByPath    = [];
        $qiResolveErr = [];
        foreach ($csvFiles as $path) {
            try {
                if ($qisConfig === null) {
                    // ALL-HEADERS mode (privacy-policy approved default)
                    $qisByPath[$path] = $this->resolveAllHeaderQis(
                        $path, $excludeSet, basename($path)
                    );
                } else {
                    // Explicit qis config: resolve the configured list,
                    // then drop any column that isn't actually in THIS
                    // file. A configured QI absent from the file is
                    // reported and skipped — the file is still checked
                    // against the QIs that ARE present. Only an empty
                    // remainder is fatal (nothing left to assess).
                    $resolved = $this->resolveQisForFile(
                        $qisConfig, basename($path)
                    );
                    if ($resolved === [self::QIS_ALL_HEADERS]) {
                        // Per-file all-headers sentinel ("*"): this file
                        // uses every header (minus exclude_qis and
                        // unique-per-row), exactly like project-wide
                        // all-headers mode, even though other files in
                        // the same project use explicit lists.
                        $qisByPath[$path] = $this->resolveAllHeaderQis(
                            $path, $excludeSet, basename($path)
                        );
                    } else {
                        $qisByPath[$path] = $this->pruneMissingQis(
                            $path, $resolved, basename($path)
                        );
                    }
                }
            } catch (\RuntimeException $e) {
                $qiResolveErr[basename($path)] = $e->getMessage();
            }
        }

        // ── Local QI presence check (no network) ────────────────────
        // In all-headers mode the QIs ARE the file's headers, so the
        // check trivially passes; it still catches unreadable files.
        $qiErrors = $this->validateEvidataQiHeaders($qisByPath);
        foreach ($qiResolveErr as $name => $msg) {
            $qiErrors[$name] = ['__resolve_error__' => $msg];
        }

        // Files that failed LOCAL QI validation (unreadable, missing
        // configured QI columns, or resolve error) are recorded as
        // failed now and removed from the remote check. Files that
        // passed local validation still go on to EviData.
        if (!empty($qiErrors)) {
            foreach ($qiErrors as $name => $missing) {
                $this->stats['evidata_files_checked']++;
                $this->stats['evidata_files_failed']++;

                if (isset($missing['__resolve_error__'])) {
                    $errMsg = $missing['__resolve_error__'];
                } elseif ($missing === ['__unreadable__']) {
                    $errMsg = 'CSV unreadable';
                } else {
                    $errMsg = 'Configured QI columns missing in CSV: '
                        . implode(', ', $missing);
                }

                $result = [
                    'passed'     => false,
                    'error'      => $errMsg,
                    'report_id'  => null,
                    'results'    => null,
                    'report_zip' => null,
                ];
                $this->stats['evidata_results'][$name] = $result;
                $this->evidataFailedFiles[] = $name;   // skip in ingestion
                $this->log("  ✗ {$name} — {$errMsg}");
                $this->writeError('evidata', "{$name}: {$errMsg}");
                $this->persistEvidataArtifacts($name, $result);

                // Drop this file from the remote-check set by basename.
                foreach (array_keys($qisByPath) as $p) {
                    if (basename($p) === $name) {
                        unset($qisByPath[$p]);
                    }
                }
            }
        }

        // If local validation knocked out every file, there is nothing
        // to send remotely — all files failed.
        if (empty($qisByPath)) {
            $this->writeEvidataRunSummary($project, false);
            $this->sendEvidataFailureNotification($project);
            $this->log("");
            $this->log("  ✗ EviData: no files passed local QI validation — none ingested");
            return 'all_failed';
        }

        // ── Remote check via EviData API ────────────────────────────
        try {
            $client  = new EviDataClient($evi);
            $results = $client->checkBatch($qisByPath);
        } catch (\Throwable $e) {
            // Client setup failed before any remote check — the files
            // that reached this stage cannot be verified, so they fail.
            $this->log("  !! EviData client setup error: " . $e->getMessage());
            $this->writeError('evidata', "Client setup error: " . $e->getMessage());
            $this->stats['evidata_results']['__client_error__'] = [
                'passed'     => false,
                'error'      => $e->getMessage(),
                'report_id'  => null,
                'results'    => null,
                'report_zip' => null,
            ];
            foreach (array_keys($qisByPath) as $p) {
                $this->evidataFailedFiles[] = basename($p);
            }
            $this->writeEvidataRunSummary($project, false);
            $this->sendEvidataFailureNotification(
                $project,
                "EviData client error before any file could be checked:\n" . $e->getMessage()
            );
            return $this->evidataOutcomeFromCounts();
        }

        // ── Process each file's result, persist artifacts ───────────
        // A file is INGESTED only if overall_passed=true. A false
        // verdict OR an error (no verdict) marks it failed -> skipped.
        foreach ($results as $name => $r) {
            $this->stats['evidata_files_checked']++;
            if ($r['passed']) {
                $this->stats['evidata_files_passed']++;
                $this->log("  ✓ {$name} (report_id={$r['report_id']})");
            } else {
                $this->stats['evidata_files_failed']++;
                $this->evidataFailedFiles[] = $name;   // skip in ingestion
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

        $this->evidataFailedFiles = array_values(array_unique($this->evidataFailedFiles));
        $allPassed = empty($this->evidataFailedFiles);
        $this->writeEvidataRunSummary($project, $allPassed);

        if (!$allPassed) {
            $this->log("");
            $this->log("  ⚠ EviData pre-flight: "
                . "{$this->stats['evidata_files_failed']} of "
                . "{$this->stats['evidata_files_checked']} file(s) failed/errored "
                . "— those files will be skipped, passing files will ingest");
            $this->log("  Artifacts: {$this->evidataLogDir}");
            $this->sendEvidataFailureNotification($project);
            return $this->evidataOutcomeFromCounts();
        }

        $this->log("  ✓ All " . count($csvFiles) . " file(s) passed EviData");
        $this->log("  ✓ Audit artifacts: {$this->evidataLogDir}");
        return 'all_passed';
    }

    /**
     * Map the per-file pass/fail tallies to an outcome label:
     *   'all_passed' | 'partial' | 'all_failed'.
     * Used after the remote check to tell processProject() and the
     * notification heading what happened.
     */
    private function evidataOutcomeFromCounts(): string
    {
        $passed = $this->stats['evidata_files_passed'] ?? 0;
        $failed = $this->stats['evidata_files_failed'] ?? 0;

        if ($failed === 0) {
            return 'all_passed';
        }
        if ($passed === 0) {
            return 'all_failed';
        }
        return 'partial';
    }

    /**
     * Resolve the global EviData service config. Returns null when
     * disabled. A non-empty global qis is validated; an empty/absent
     * qis is allowed and signals (with an empty/absent project qis)
     * the all-headers default.
     */
    private function resolveEvidataConfig(): ?array
    {
        $evi = $this->config['evidata'] ?? [];

        if (empty($evi['enabled'])) {
            return null;
        }

        $qis = $evi['qis'] ?? null;
        if ($qis !== null && $qis !== []) {
            $this->validateQisShape($qis, 'evidata_config.json -> qis');
        }

        return $evi;
    }

    /**
     * Decide which QI configuration applies to one project.
     *
     *   project.json evidata.qis (non-empty) -> per-project override
     *   else global evidata.qis (non-empty)  -> global baseline
     *   else                                 -> ALL-HEADERS mode (null)
     *
     * @return array{0: array|null, 1: string} [qisConfig|null, sourceLabel]
     *         A null config signals all-headers mode to the caller.
     * @throws \RuntimeException if a defined qis is structurally invalid.
     */
    private function resolveProjectQisConfig(array $project, array $evi): array
    {
        $projectQis = $project['evidata']['qis'] ?? null;
        if ($projectQis !== null && $projectQis !== []) {
            $this->validateQisShape($projectQis, 'project.json -> evidata.qis');
            return [$projectQis, 'project.json (per-project override)'];
        }

        $globalQis = $evi['qis'] ?? null;
        if ($globalQis !== null && $globalQis !== []) {
            return [$globalQis, 'evidata_config.json (global default)'];
        }

        // Privacy-policy approved default: no qis defined anywhere ->
        // use every CSV header as a QI, minus exclude_qis and minus
        // unique-per-row identifier columns.
        return [null, 'ALL HEADERS (no qis defined)'];
    }

    /**
     * Resolve the exclude_qis list (column names to drop in all-headers
     * mode). Two-level like qis: project.json overrides global. Returned
     * as a lowercased set for CASE-INSENSITIVE matching — exclusion is
     * deliberately liberal.
     *
     * @return array<string,true>  lowercased-name => true
     */
    private function resolveExcludeQis(array $project, array $evi): array
    {
        $list = $project['evidata']['exclude_qis']
            ?? $evi['exclude_qis']
            ?? [];

        $set = [];
        foreach ((array)$list as $name) {
            if (is_string($name) && $name !== '') {
                $set[strtolower(trim($name))] = true;
            }
        }
        return $set;
    }

    /**
     * Resolve QIs for ONE file in ALL-HEADERS mode: every column header,
     * minus the configured exclude_qis, minus unique-per-row columns
     * (identifiers / pseudonyms / timestamps). EviData's own report
     * recommends unselecting unique-per-row columns, as they force
     * maximum re-identification risk. Logs exactly what was used and
     * excluded so every all-headers run is auditable.
     *
     * @param array<string,true> $excludeSet  lowercased exclude set.
     * @return array  QI column names (original casing) to send.
     * @throws \RuntimeException if unreadable or nothing remains.
     */
    private function resolveAllHeaderQis(string $path, array $excludeSet, string $basename): array
    {
        $delim   = str_ends_with(strtolower($path), '.tsv') ? "\t" : ',';
        $fh      = @fopen($path, 'r');
        if ($fh === false) {
            throw new \RuntimeException("CSV unreadable: {$basename}");
        }
        $headers = fgetcsv($fh, 0, $delim);
        fclose($fh);
        if (!is_array($headers) || $headers === []) {
            throw new \RuntimeException("CSV unreadable: {$basename}");
        }
        $headers = array_map(fn($h) => (string)$h, $headers);

        $uniqueCols = $this->detectUniquePerRowColumns($path);

        $kept = [];
        $excludedByConfig = [];
        $excludedAsUnique = [];
        foreach ($headers as $h) {
            $key = strtolower(trim($h));
            if ($key === '') {
                continue;
            }
            if (isset($excludeSet[$key])) {
                $excludedByConfig[] = $h;
            } elseif (isset($uniqueCols[$key])) {
                $excludedAsUnique[] = $h;
            } else {
                $kept[] = $h;
            }
        }

        $this->log(sprintf(
            "    %s — ALL HEADERS: %d of %d columns used"
            . " (excluded %d by config, %d unique-per-row)",
            $basename, count($kept), count($headers),
            count($excludedByConfig), count($excludedAsUnique)
        ));
        if ($excludedByConfig) {
            $this->log("      excluded (config): " . implode(', ', $excludedByConfig));
        }
        if ($excludedAsUnique) {
            $this->log("      excluded (unique-per-row): " . implode(', ', $excludedAsUnique));
        }

        if ($kept === []) {
            throw new \RuntimeException(
                "All headers excluded for {$basename} — nothing left to "
                . "assess. Loosen exclude_qis or define an explicit qis list."
            );
        }
        return array_values($kept);
    }

    /**
     * Identify columns whose every populated value is distinct across
     * all rows (unique-per-row) — almost always identifiers/pseudonyms/
     * timestamps. Returns a lowercased set of such column names. Needs
     * at least 2 rows for the notion to be meaningful.
     *
     * @return array<string,true>
     */
    private function detectUniquePerRowColumns(string $path): array
    {
        $delim = str_ends_with(strtolower($path), '.tsv') ? "\t" : ',';
        $fh    = @fopen($path, 'r');
        if ($fh === false) {
            return [];
        }
        $headers = fgetcsv($fh, 0, $delim);
        if (!is_array($headers)) {
            fclose($fh);
            return [];
        }

        $n        = count($headers);
        $seen     = array_fill(0, $n, []);
        $nonEmpty = array_fill(0, $n, 0);
        $rowCount = 0;

        while (($row = fgetcsv($fh, 0, $delim)) !== false) {
            $rowCount++;
            for ($i = 0; $i < $n; $i++) {
                $v = isset($row[$i]) ? trim((string)$row[$i]) : '';
                if ($v === '') {
                    continue;
                }
                $nonEmpty[$i]++;
                $seen[$i][$v] = true;
            }
        }
        fclose($fh);

        if ($rowCount < 2) {
            return [];
        }

        $unique = [];
        for ($i = 0; $i < $n; $i++) {
            if ($nonEmpty[$i] === $rowCount && count($seen[$i]) === $rowCount) {
                $unique[strtolower(trim((string)$headers[$i]))] = true;
            }
        }
        return $unique;
    }

    /**
     * Decide which QI configuration applies — see resolveProjectQisConfig.
     * (Validation helper retained for both config levels.)
     *
     * @throws \RuntimeException on any structural problem.
     */
    private function validateQisShape($qis, string $where): void
    {
        if (!is_array($qis) || empty($qis)) {
            throw new \RuntimeException(
                "{$where} must be a non-empty array (a flat list of QI "
                . "column names, or a map with a '" . self::QIS_DEFAULT_KEY
                . "' key plus optional per-filename overrides)."
            );
        }

        $isFlatList = array_keys($qis) === range(0, count($qis) - 1);

        if ($isFlatList) {
            foreach ($qis as $q) {
                if (!is_string($q) || $q === '') {
                    throw new \RuntimeException(
                        "{$where} flat array must contain only non-empty "
                        . "column-name strings."
                    );
                }
            }
            return;
        }

        foreach ($qis as $key => $list) {
            // A per-file entry may be the all-headers sentinel ("*"),
            // meaning "use every header in that file". Not allowed for
            // '_default' — the default must be a concrete list.
            if (is_string($list) && $list === self::QIS_ALL_HEADERS) {
                if ($key === self::QIS_DEFAULT_KEY) {
                    throw new \RuntimeException(
                        "{$where}['" . self::QIS_DEFAULT_KEY . "'] cannot be '"
                        . self::QIS_ALL_HEADERS . "' — the default must be a "
                        . "concrete list of QI column names. The '"
                        . self::QIS_ALL_HEADERS . "' sentinel is only valid for "
                        . "a specific filename entry."
                    );
                }
                continue;
            }

            if (!is_array($list) || empty($list)) {
                throw new \RuntimeException(
                    "{$where}['{$key}'] must be a non-empty array of QI "
                    . "column-name strings (or the string '"
                    . self::QIS_ALL_HEADERS . "' to use all of that file's headers)."
                );
            }
            foreach ($list as $q) {
                if (!is_string($q) || $q === '') {
                    throw new \RuntimeException(
                        "{$where}['{$key}'] must contain only non-empty "
                        . "column-name strings."
                    );
                }
            }
        }
    }

    /**
     * Resolve the QI list for ONE file from an explicit (non-null)
     * project/global qis config.
     *
     * Flat array -> applies to every file.
     * Map        -> exact-filename key wins; else '_default'; else a
     *               hard error (no silent skip; the chosen source is
     *               authoritative).
     *
     * @throws \RuntimeException when a file matches no key and there is
     *         no '_default'.
     */
    private function resolveQisForFile(array $qisConfig, string $basename): array
    {
        $isFlatList = array_keys($qisConfig) === range(0, count($qisConfig) - 1);

        if ($isFlatList) {
            return array_values($qisConfig);
        }

        if (isset($qisConfig[$basename])) {
            // A per-file entry may be the all-headers sentinel string.
            // Return it wrapped so the caller can detect it and route
            // the file through resolveAllHeaderQis() instead.
            if ($qisConfig[$basename] === self::QIS_ALL_HEADERS) {
                return [self::QIS_ALL_HEADERS];
            }
            return array_values($qisConfig[$basename]);
        }

        if (isset($qisConfig[self::QIS_DEFAULT_KEY])) {
            return array_values($qisConfig[self::QIS_DEFAULT_KEY]);
        }

        throw new \RuntimeException(
            "No QI list for '{$basename}' — the chosen qis config has no "
            . "entry for this file and no '" . self::QIS_DEFAULT_KEY . "' "
            . "fallback. Add an entry for this file, or a '"
            . self::QIS_DEFAULT_KEY . "' key, in the project's project.json "
            . "evidata.qis (or the global evidata_config.json)."
        );
    }

    /**
     * Drop configured QIs that are not present in THIS file's header,
     * reporting each dropped column. The file is still assessed against
     * the QIs that ARE present; a missing configured QI is no longer a
     * hard failure. Throws only if the file is unreadable or if NONE of
     * the configured QIs exist in it (nothing left to assess).
     *
     * Header match is case-INSENSITIVE (courtesy); kept QI names keep
     * the configured casing, which is what EviData validates against
     * server-side.
     *
     * @param array<string> $qis  Resolved QI list for this file.
     * @return array<string>      QIs that exist in the file's header.
     * @throws \RuntimeException  If unreadable, or every QI is missing.
     */
    private function pruneMissingQis(string $path, array $qis, string $basename): array
    {
        $delim   = str_ends_with(strtolower($path), '.tsv') ? "\t" : ',';
        $fh      = @fopen($path, 'r');
        if ($fh === false) {
            throw new \RuntimeException("CSV unreadable: {$basename}");
        }
        $headers = fgetcsv($fh, 0, $delim);
        fclose($fh);
        if (!is_array($headers) || $headers === []) {
            throw new \RuntimeException("CSV unreadable: {$basename}");
        }

        $headersLower = array_map(
            fn($h) => strtolower(trim((string)$h)),
            $headers
        );

        $kept    = [];
        $missing = [];
        foreach ($qis as $qi) {
            $qiLower = strtolower(trim((string)$qi));
            if (in_array($qiLower, $headersLower, true)) {
                $kept[] = $qi;
            } else {
                $missing[] = $qi;
            }
        }

        if (!empty($missing)) {
            $this->log(sprintf(
                "    %s — configured QI(s) not in file, skipped: %s"
                . " (continuing with %d of %d)",
                $basename,
                implode(', ', $missing),
                count($kept),
                count($qis)
            ));
        }

        if ($kept === []) {
            throw new \RuntimeException(
                "None of the configured QI columns exist in {$basename}: "
                . implode(', ', $qis)
                . ". Correct the project's evidata.qis to match the file's headers."
            );
        }

        return array_values($kept);
    }

    /**
     * Local pre-check: verify each file's resolved QI columns exist in
     * that file's header row. Case-INSENSITIVE (courtesy). In all-headers
     * mode the QIs are the headers, so this trivially passes; it still
     * catches unreadable files.
     *
     * @param array<string, array<string>> $qisByPath  path => QI list.
     * @return array<string, array<string>>  {basename -> missing cols}.
     */
    private function validateEvidataQiHeaders(array $qisByPath): array
    {
        $bad = [];
        foreach ($qisByPath as $path => $qis) {
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

            $headersLower = array_map(
                fn($h) => strtolower(trim((string)$h)),
                $headers
            );

            $missing = [];
            foreach ($qis as $qi) {
                $qiLower = strtolower(trim((string)$qi));
                if (!in_array($qiLower, $headersLower, true)) {
                    $missing[] = $qi;
                }
            }
            if (!empty($missing)) {
                $bad[basename($path)] = array_values($missing);
            }
        }
        return $bad;
    }

    private function openEvidataLogDir(string $mountPath): string
    {
        if ($this->evidataLogDir !== null) {
            return $this->evidataLogDir;
        }
        $dir = rtrim($mountPath, '/') . "/logs/evidata/{$this->runTimestamp}";
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }
        $this->evidataLogDir = $dir;
        return $dir;
    }

    private function persistEvidataArtifacts(string $sourceName, array $result): array
    {
        if ($this->evidataLogDir === null) {
            return [];
        }

        $stem    = pathinfo($sourceName, PATHINFO_FILENAME);
        $stem    = preg_replace('/[^A-Za-z0-9._-]/', '_', $stem) ?: 'artifact';
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
     * Extract a named PDF entry from a file's report ZIP into the log
     * dir and return its path, or null if the zip or entry is missing.
     *
     * Used to attach the human-readable summary PDF to the failure
     * email instead of the whole (multi-MB) ZIP. EviData's report.zip
     * bundles report.pdf (full report) and summary_letter.pdf (short
     * summary); the summary is smaller and is what a reviewer reads
     * first.
     *
     * @param string $stem      Sanitised file stem (matches the .report.zip).
     * @param string $entryName Name of the PDF inside the zip.
     * @param string $outSuffix Suffix for the extracted file (e.g. '_summary.pdf').
     * @return ?string Absolute path to the extracted PDF, or null.
     */
    private function extractReportPdf(string $stem, string $entryName, string $outSuffix): ?string
    {
        if ($this->evidataLogDir === null) {
            return null;
        }
        $zipPath = "{$this->evidataLogDir}/{$stem}.report.zip";
        if (!is_file($zipPath)) {
            return null;
        }
        $outPath = "{$this->evidataLogDir}/{$stem}{$outSuffix}";

        $za = new \ZipArchive();
        if ($za->open($zipPath) !== true) {
            return null;
        }
        $bytes = $za->getFromName($entryName);
        $za->close();

        if ($bytes === false) {
            return null;   // entry not present in this zip
        }
        if (file_put_contents($outPath, $bytes) === false) {
            return null;
        }
        return $outPath;
    }

    /**
     * Locate the mutool binary, or null if it is not installed/usable.
     * Resolved once and cached on the instance. The path is overridable
     * via config (evidata.mutool_path) for non-standard installs; by
     * default it probes PATH via `command -v mutool`.
     *
     * @return ?string Absolute path to mutool, or null if not found.
     */
    private function mutoolPath(): ?string
    {
        // Cache: false = not yet checked, null = checked & absent,
        // string = resolved path.
        static $resolved = false;
        if ($resolved !== false) {
            return $resolved;
        }

        // Explicit override from config wins, if it points at a real file.
        $configured = $this->config['evidata']['mutool_path'] ?? null;
        if (is_string($configured) && $configured !== '' && is_executable($configured)) {
            return $resolved = $configured;
        }

        // Otherwise probe PATH. `command -v` prints the path and exits 0
        // when found, exits non-zero when not.
        $out = [];
        $rc  = 0;
        exec('command -v mutool 2>/dev/null', $out, $rc);
        if ($rc === 0 && !empty($out[0]) && is_executable(trim($out[0]))) {
            return $resolved = trim($out[0]);
        }

        return $resolved = null;
    }

    /**
     * Compress a PDF in place via mutool (MuPDF), so EviData report
     * attachments fit comfortably under the local MTA's size cap.
     *
     * EviData's failed-report PDFs ship with high-DPI rasterised charts
     * that make each file 30 MB+. mutool garbage-collects unused
     * objects (-g), deflate-compresses streams (-z), and downsamples
     * embedded images above a DPI threshold, typically pulling the file
     * well under the attachment ceiling while keeping risk-distribution
     * charts legible for a privacy reviewer.
     *
     * Replaces the previous Ghostscript implementation: mutool has no
     * PostScript interpreter and a smaller attack surface for the
     * untrusted-PDF input this handles.
     *
     * Returns the path to the compressed file on success, or the
     * ORIGINAL $srcPath on any failure — so missing/broken `mutool`, a
     * non-zero exit, a zero-byte output, or a result no smaller than the
     * source never blocks the email. Pipeline degrades gracefully:
     * emails get larger, not absent.
     *
     * @param string $srcPath  Input PDF on disk.
     * @param string $dstPath  Output PDF path (created alongside).
     * @param string $setting  Image-downsample target DPI as a string
     *                         ('150', '200', '300'); validated against a
     *                         whitelist. Empty/unknown -> structural
     *                         compression only (no downsampling).
     * @return string  The path to use for the attachment.
     */
    private function compressPdf(string $srcPath, string $dstPath, string $setting): string
    {
        if (!is_file($srcPath)) {
            return $srcPath;   // can't compress nothing
        }

        // Clear, upfront check: is mutool actually installed? Without
        // this, a missing binary only shows up as the cryptic shell exit
        // 127 ("command not found"). Say so plainly so the operator knows
        // to `sudo apt install mupdf-tools` rather than guessing.
        if ($this->mutoolPath() === null) {
            $this->log("    compressPdf: mutool not found on host — cannot compress "
                . basename($srcPath) . ". Install it with 'sudo apt install mupdf-tools' "
                . "(package: mupdf-tools). Attaching the original uncompressed file.");
            return $srcPath;
        }

        // Only allow known-safe DPI tokens; never interpolate arbitrary
        // strings into the shell. An unrecognised value falls back to
        // structural compression only (still safe, just less shrink).
        $allowedDpi = ['150', '200', '300'];
        $dpi        = in_array($setting, $allowedDpi, true) ? $setting : null;

        // mutool clean flags:
        //   -g  garbage-collect unused objects (repeat = more aggressive)
        //   -z  deflate-compress streams
        //   -D  decompress-then-recompress (normalises existing streams)
        //   -L <dpi>  downsample images above the given DPI (the part
        //             that actually shrinks image-heavy reports)
        $flags = '-ggg -z -D';
        if ($dpi !== null) {
            $flags .= ' -L ' . escapeshellarg($dpi);
        }

        $cmd = sprintf(
            '%s clean %s %s %s 2>&1',
            escapeshellarg($this->mutoolPath()),
            $flags,
            escapeshellarg($srcPath),
            escapeshellarg($dstPath)
        );

        $output = [];
        $rc     = 0;
        exec($cmd, $output, $rc);

        if ($rc !== 0 || !is_file($dstPath) || filesize($dstPath) < 1024) {
            $msg = $rc !== 0
                ? "mutool exit={$rc}"
                : (!is_file($dstPath) ? "no output" : "output too small");
            $this->log("    compressPdf: {$msg}, falling back to original "
                . basename($srcPath));
            if (is_file($dstPath)) {
                @unlink($dstPath);
            }
            return $srcPath;
        }

        $orig = filesize($srcPath);
        $new  = filesize($dstPath);

        // If mutool did not actually reduce the file, keep the original
        // so we never attach a larger copy than the source.
        if ($new >= $orig) {
            $this->log(sprintf(
                "    compressPdf: no reduction (%d KB -> %d KB), using original %s",
                (int)round($orig / 1024), (int)round($new / 1024), basename($srcPath)
            ));
            @unlink($dstPath);
            return $srcPath;
        }

        $pct = $orig > 0 ? round(100 * (1 - $new / $orig)) : 0;
        $this->log(sprintf(
            "    compressPdf: %s  %d KB -> %d KB (%d%% smaller, mutool%s)",
            basename($srcPath),
            (int)round($orig / 1024),
            (int)round($new / 1024),
            $pct,
            $dpi !== null ? " -L {$dpi}" : ""
        ));
        return $dstPath;
    }

    /**
     * Raw attachment-size ceiling in bytes, derived from the host's MTA
     * message_size_limit. The limit is read (in MB) from config:
     *   evidata.mta_message_size_limit_mb  (matches `postconf
     *   message_size_limit` / 1024 / 1024 on this host),
     * defaulting to EVIDATA_DEFAULT_MTA_LIMIT_MB when absent.
     *
     * The limit applies to the WHOLE MIME message AFTER encoding. MIME
     * base64 inflates attachments by ~37%, and headers/boundaries add a
     * little more, so the RAW attachment total must stay under
     * limit / 1.4 for the encoded message to fit. Per-host: each server
     * may set a different limit in its own evidata_config.json.
     */
    private function maxAttachBytes(): int
    {
        $limitMb    = $this->config['evidata']['mta_message_size_limit_mb']
            ?? self::EVIDATA_DEFAULT_MTA_LIMIT_MB;
        $limitBytes = (int)($limitMb * 1024 * 1024);

        // Reserve ~37% for base64 plus a small margin for MIME headers
        // and boundary strings: divide by 1.4.
        return (int)($limitBytes / 1.4);
    }

    /**
     * Collect one named PDF (extracted from each failed file's report
     * ZIP) into an attachment list, and return [attachments, totalBytes].
     * Used by the tiered attachment ladder in the failure email:
     * try full report.pdf first, then summary_letter.pdf.
     *
     * Each extracted PDF is returned RAW (uncompressed). Compression is
     * applied later, to the whole batch, and only when the batch total
     * exceeds the attachment cap — see sendEvidataFailureNotification().
     *
     * @param string $entryName  PDF name inside the zip (e.g. 'report.pdf').
     * @param string $outSuffix  Extracted-file suffix (e.g. '_report.pdf').
     * @param string $mailSuffix Suffix for the name shown in the email.
     * @return array{0: array<array{path:string,name:string}>, 1: int}
     */
    private function collectEvidataPdfs(string $entryName, string $outSuffix, string $mailSuffix): array
    {
        $attachments = [];
        $totalBytes  = 0;
        foreach (($this->stats['evidata_results'] ?? []) as $name => $r) {
            if ($r['passed'] ?? false) {
                continue;   // only failed files
            }
            $stem = pathinfo($name, PATHINFO_FILENAME);
            $stem = preg_replace('/[^A-Za-z0-9._-]/', '_', $stem) ?: 'artifact';
            $pdf  = $this->extractReportPdf($stem, $entryName, $outSuffix);
            if ($pdf === null || !is_file($pdf)) {
                continue;
            }

            // Raw extracted PDF. Compression (if needed) is decided on the
            // WHOLE batch later, not here — a batch that already fits is
            // attached as-is. The raw extracted PDF stays in the log dir.
            $attachments[] = ['path' => $pdf, 'name' => "{$stem}{$mailSuffix}"];
            $totalBytes   += filesize($pdf);
        }
        return [$attachments, $totalBytes];
    }

    /**
     * Compress every PDF in an attachment batch via mutool, returning
     * [newAttachments, newTotalBytes]. Called ONLY when a batch's raw
     * total exceeds the attachment cap. Each file that fails to compress,
     * or would grow, keeps its original (compressPdf() guarantees this).
     * The compressed copies live alongside the originals in the log dir;
     * the raw extracted PDFs are left untouched.
     *
     * @param array<array{path:string,name:string}> $attachments
     * @return array{0: array<array{path:string,name:string}>, 1: int}
     */
    private function compressAttachmentBatch(array $attachments): array
    {
        $compressionSetting = $this->config['evidata']['pdf_compression']
            ?? '200';

        // Compression disabled -> return the batch unchanged.
        if ($compressionSetting === null || $compressionSetting === '') {
            $total = 0;
            foreach ($attachments as $att) {
                $total += is_file($att['path']) ? filesize($att['path']) : 0;
            }
            return [$attachments, $total];
        }

        // One clear, batch-level clue when mutool is missing: the whole
        // over-cap batch cannot be shrunk, so the email will fall back to
        // summaries / contact-team. Say why, once, instead of leaving the
        // operator to infer it from per-file lines.
        if ($this->mutoolPath() === null) {
            $total = 0;
            foreach ($attachments as $att) {
                $total += is_file($att['path']) ? filesize($att['path']) : 0;
            }
            $this->log(sprintf(
                "  EviData attachments: batch is over the email cap but mutool is "
                . "NOT installed — cannot compress %d file(s) (%.1f MB). Install it "
                . "with 'sudo apt install mupdf-tools'. Falling back to smaller "
                . "attachments or a contact-the-team note.",
                count($attachments), $total / 1024 / 1024
            ));
            return [$attachments, $total];
        }

        $out   = [];
        $total = 0;
        foreach ($attachments as $att) {
            $src = $att['path'];
            if (!is_file($src)) {
                continue;
            }
            $base       = pathinfo($src, PATHINFO_FILENAME);
            $compressed = dirname($src) . "/{$base}_compressed.pdf";
            $use        = $this->compressPdf($src, $compressed, $compressionSetting);
            $out[]      = ['path' => $use, 'name' => $att['name']];
            $total     += filesize($use);
        }
        return [$out, $total];
    }

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

        $recipients = array_values(array_unique($recipients));

        if (empty($recipients)) {
            $this->log("  No EviData failure recipients configured — not emailing");
            return;
        }

        $projectName = $project['project_common_name']
            ?? basename($project['_projectPath']);
        $subject     = "PRIVACY CHECK FAILED: {$projectName} Clinical Pipeline";
        $body        = $this->buildEvidataFailureBody($projectName, $clientErrorOverride);

        $attachments = [];
        $totalBytes  = 0;
        $attachTier  = 'none';   // 'full' | 'summary' | 'none'
        if ($clientErrorOverride === null && $this->evidataLogDir !== null) {
            // Graceful fallback ladder for attachments. The email size
            // limit applies to the WHOLE message, so the decision is
            // made on each batch's TOTAL size, not per file:
            //   1. full report.pdf set — attach as-is if the total fits;
            //      if the total exceeds the cap, compress the whole set
            //      and use it if the compressed total now fits.
            //   2. summary_letter.pdf set — same rule (as-is if it fits,
            //      else compress and re-check).
            //   3. nothing — tell the recipient to contact the team.
            // EviData's report.zip bundles both report.pdf (full) and
            // summary_letter.pdf (summary) per file.
            [$fullAtt,    $fullBytes]    = $this->collectEvidataPdfs('report.pdf', '_report.pdf', '_evidata_report.pdf');
            [$summaryAtt, $summaryBytes] = $this->collectEvidataPdfs('summary_letter.pdf', '_summary.pdf', '_evidata_summary.pdf');

            // Per-host attachment ceiling, derived from the MTA limit in
            // config (post-base64 headroom already applied).
            $maxBytes = $this->maxAttachBytes();
            $capMb    = $maxBytes / 1024 / 1024;

            // ── Tier 1: full reports ────────────────────────────────
            if (!empty($fullAtt)) {
                if ($fullBytes <= $maxBytes) {
                    // Already fits — no compression needed.
                    $attachments = $fullAtt;
                    $totalBytes  = $fullBytes;
                    $attachTier  = 'full';
                } else {
                    // Batch total is over the cap — compress the whole set.
                    $this->log(sprintf(
                        "  EviData attachments: full report batch is %.1f MB, "
                        . "exceeds the %.1f MB email cap — compressing the batch",
                        $fullBytes / 1024 / 1024, $capMb
                    ));
                    [$fullC, $fullCBytes] = $this->compressAttachmentBatch($fullAtt);
                    if ($fullCBytes <= $maxBytes) {
                        $attachments = $fullC;
                        $totalBytes  = $fullCBytes;
                        $attachTier  = 'full';
                    }
                }
            }

            // ── Tier 2: summaries (only if tier 1 didn't land) ──────
            if ($attachTier === 'none' && !empty($summaryAtt)) {
                if ($summaryBytes <= $maxBytes) {
                    $attachments = $summaryAtt;
                    $totalBytes  = $summaryBytes;
                    $attachTier  = 'summary';
                } else {
                    $this->log(sprintf(
                        "  EviData attachments: summary batch is %.1f MB, "
                        . "exceeds the %.1f MB email cap — compressing the batch",
                        $summaryBytes / 1024 / 1024, $capMb
                    ));
                    [$sumC, $sumCBytes] = $this->compressAttachmentBatch($summaryAtt);
                    if ($sumCBytes <= $maxBytes) {
                        $attachments = $sumC;
                        $totalBytes  = $sumCBytes;
                        $attachTier  = 'summary';
                    }
                }
            }

            // ── Tier 3: nothing fits ────────────────────────────────
            if ($attachTier === 'none') {
                $attachments = [];
            }
        }

        $ceilingMb = $this->maxAttachBytes() / 1024 / 1024;
        if ($attachTier === 'full') {
            $this->log("  EviData attachments: full report PDF(s), "
                . round($totalBytes / 1024 / 1024, 1) . "MB total");
            $body .= "\n\nThe full privacy report PDF(s) for the failed file(s) are attached.\n"
                . "All audit artifacts are also on the pipeline host at:\n"
                . "  {$this->evidataLogDir}\n";
        } elseif ($attachTier === 'summary') {
            $this->log("  EviData attachments: full reports over {$ceilingMb}MB — "
                . "attaching summary PDF(s) instead, "
                . round($totalBytes / 1024 / 1024, 1) . "MB total");
            $body .= "\n\nThe full reports were too large to email, so the shorter\n"
                . "summary report PDF(s) for the failed file(s) are attached instead.\n"
                . "The full reports are on the pipeline host at:\n"
                . "  {$this->evidataLogDir}\n";
        } else {
            // Tier 3: even summaries don't fit (or no PDFs found).
            $this->log("  EviData attachments: reports exceed {$ceilingMb}MB even as "
                . "summaries — sending contact-the-team email (no attachment)");
            $body .= "\n\nThe privacy reports are too large to send by email.\n"
                . "Please contact the ARCHIMEDES team to obtain the report(s) for\n"
                . "the failed file(s). For reference, they are stored on the\n"
                . "pipeline host at:\n"
                . "  {$this->evidataLogDir}\n";
        }

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

    private function sendEvidataMailWithAttachments(
        string $to,
        string $subject,
        string $body,
        array $attachments
    ): bool {
        if (empty($attachments)) {
            $headers = "From: " . $this->evidataFromAddress() . "\r\n"
                . "Content-Type: text/plain; charset=UTF-8\r\n";
            return mail($to, $subject, $body, $headers);
        }

        $boundary = '=_evidata_' . md5(uniqid('', true));
        $headers  = "From: " . $this->evidataFromAddress() . "\r\n"
            . "MIME-Version: 1.0\r\n"
            . "Content-Type: multipart/mixed; boundary=\"{$boundary}\"\r\n";

        $message  = "--{$boundary}\r\n"
            . "Content-Type: text/plain; charset=UTF-8\r\n"
            . "Content-Transfer-Encoding: 8bit\r\n\r\n"
            . $body . "\r\n";

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

    private function evidataFromAddress(): string
    {
        return $this->config['evidata']['from_address']
            ?? ('archimedes-pipeline@' . (gethostname() ?: 'localhost'));
    }

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

    private function buildEvidataFailureBody(
        string $projectName,
        ?string $clientErrorOverride
    ): string {
        // Per-file gate: files that did not pass EviData are SKIPPED
        // (not ingested) and retried next run; files that passed are
        // still ingested. No whole-project abort.
        $failedCount  = $this->stats['evidata_files_failed']  ?? 0;
        $passedCount  = $this->stats['evidata_files_passed']  ?? 0;
        $checkedCount = $this->stats['evidata_files_checked'] ?? 0;

        $body  = "EviData privacy pre-flight: one or more files did not pass.\n";
        $body .= "Project   : {$projectName}\n";
        $body .= "Run       : {$this->runTimestamp}"
            . ($this->dryRun ? " (DRY RUN)" : "") . "\n";
        $body .= "Timestamp : " . date('Y-m-d H:i:s') . "\n";

        if ($this->dryRun) {
            $body .= "Status    : PREVIEW — no LORIS writes attempted (dry run)\n\n";
        } else {
            $body .= "Status    : The file(s) listed below were NOT ingested.\n";
            $body .= "            They will be retried on the next pipeline run.\n";
            if ($checkedCount > 0) {
                $body .= "            Outcome: {$passedCount} passed, {$failedCount} failed/errored"
                    . " (of {$checkedCount} checked).\n";
            }
            $body .= "\n";
        }

        if ($clientErrorOverride !== null) {
            $body .= "Client error:\n{$clientErrorOverride}\n\n";
            $body .= $this->evidataActionFooter(false);
            return $body;
        }

        $hasAnyReport = false;
        foreach (($this->stats['evidata_results'] ?? []) as $r) {
            if (!empty($r['report_id'])) {
                $hasAnyReport = true;
                break;
            }
        }

        // Show every file's outcome, but lead with the failures since
        // this is the privacy email.
        $body .= "Per-file results:\n" . str_repeat('-', 50) . "\n";
        $allResults = $this->stats['evidata_results'] ?? [];
        // Failures first, then any passers (for context).
        uksort($allResults, function ($a, $b) use ($allResults) {
            $pa = $allResults[$a]['passed'] ?? false;
            $pb = $allResults[$b]['passed'] ?? false;
            if ($pa === $pb) return strcmp($a, $b);
            return $pa ? 1 : -1;
        });
        foreach ($allResults as $name => $r) {
            $mark = $r['passed'] ? '✔' : '✗';
            if ($r['passed']) {
                $detail = "passed (report_id={$r['report_id']}) — ingested";
            } elseif ($r['error'] !== null) {
                $detail = "ERROR: {$r['error']} — NOT ingested";
            } else {
                $detail = "overall_passed=false (report_id={$r['report_id']}) — NOT ingested";
            }
            $body .= "  {$mark} {$name}  {$detail}\n";
        }
        $body .= "\n" . $this->evidataActionFooter($hasAnyReport);

        if ($this->runLogPath) {
            $body .= "\nClinical run log: {$this->runLogPath}\n";
        }

        return $body;
    }

    private function evidataActionFooter(bool $hasReport = true): string
    {
        if ($hasReport) {
            return "Action required: review the failed report(s) for the file(s)\n"
                . "listed above and re-export them with adequate de-identification.\n"
                . "The next pipeline run will automatically re-check those files;\n"
                . "files that pass on the next run will be ingested then.\n";
        }

        return "Action required: the failure occurred before an EviData report\n"
            . "was produced, so there is no report to review. The configured QI\n"
            . "columns were not found in the CSV header, the qis config did not\n"
            . "cover the file, or the file was unreadable. Fix the CSV column\n"
            . "names, or correct the 'qis' list (project.json evidata.qis, or\n"
            . "the global config/evidata_config.json) to match the actual\n"
            . "headers. The next pipeline run will re-check the file(s).\n";
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

            // Per-file privacy gate: a file that did not pass EviData
            // (failed verdict or errored — no verdict) is never ingested.
            // It is skipped here and retried on the next run.
            if (in_array($filename, $this->evidataFailedFiles, true)) {
                $this->log("  [{$filename}] SKIPPED - did not pass EviData privacy check");
                $this->dataResults[$filename] = ['status' => 'skipped', 'reason' => 'evidata failed'];
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
        // Atomic write: temp + rename, so a crash or mount loss mid-write
        // never leaves a half-written / corrupt tracking file.
        $tmp = $this->trackingFilePath . '.tmp';
        $ok  = file_put_contents(
            $tmp,
            json_encode($this->trackingData, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
        );
        if ($ok !== false) {
            @rename($tmp, $this->trackingFilePath);
        }
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

        $hasErrors = ($s['data_failed'] > 0 || $s['dd_failed'] > 0
            || ($s['evidata_files_failed'] ?? 0) > 0);
        $outcome   = $hasErrors ? 'COMPLETED WITH ERRORS' : 'COMPLETED SUCCESSFULLY';
        $this->log("");
        $this->log("  Result: {$outcome}");
        $this->log("========================================");
    }

    // ══════════════════════════════════════════════════════════════════
    //  Clinical-channel email notification
    // ══════════════════════════════════════════════════════════════════

    private function sendNotification(array $project): void
    {
        $name = $project['project_common_name'] ?? 'Unknown';
        $s    = $this->stats;

        // Heading rule (SUCCESS / SUMMARY / FAILURE) is based on the
        // OVERALL outcome of the run, counting any of:
        //   - EviData skips (file did not pass the privacy check)
        //   - install/ingest errors (DD or data upload failure)
        // as "errors". Hash-unchanged skips and config-excluded files
        // are NOT errors (steady-state runs stay SUCCESS).
        $eviPassed   = $s['evidata_files_passed']  ?? 0;
        $eviFailed   = $s['evidata_files_failed']  ?? 0;
        $ingestOk    = $s['data_uploaded']         ?? 0;
        $ingestFail  = $s['data_failed']           ?? 0;
        $ddFail      = $s['dd_failed']             ?? 0;

        $successCount = $eviPassed + $ingestOk;   // anything that went right
        $errorCount   = $eviFailed + $ingestFail + $ddFail;

        if ($errorCount === 0) {
            $status = 'SUCCESS';
        } elseif ($successCount === 0) {
            $status = 'FAILURE';
        } else {
            $status = 'SUMMARY';
        }

        // Anything other than a clean SUCCESS goes to the error list.
        $successEmails = $project['notification_emails']['clinical']['on_success'] ?? [];
        $errorEmails   = $project['notification_emails']['clinical']['on_error']   ?? [];
        $emailsToSend  = ($status === 'SUCCESS') ? $successEmails : $errorEmails;

        $emailsToSend = array_values(array_unique($emailsToSend));

        if (empty($emailsToSend)) {
            $this->log("  No notification emails configured for clinical");
            return;
        }

        $subject = "{$status}: {$name} Clinical Ingestion";

        $body  = "Project: {$name}\n";
        $body .= "Modality: clinical\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n";
        $body .= "Run: {$this->runTimestamp}\n";
        if ($this->force) {
            $body .= "Mode: FORCE (hash check bypassed)\n";
        }
        $body .= "\n";

        if (($s['evidata_files_checked'] ?? 0) > 0) {
            $body .= "EviData Pre-flight:\n";
            $body .= "  Files checked: {$s['evidata_files_checked']}, "
                . "passed: {$s['evidata_files_passed']}, "
                . "failed: {$s['evidata_files_failed']}\n";
            if (!empty($this->evidataFailedFiles)) {
                $body .= "  Skipped (did not pass EviData, not ingested): "
                    . implode(', ', $this->evidataFailedFiles) . "\n";
            }
            if ($this->evidataLogDir !== null) {
                $body .= "  Artifacts: {$this->evidataLogDir}\n";
            }
            $body .= "\n";
        }

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

        if ($status === 'SUMMARY') {
            $body .= "⚠ Partial ingestion. Files that passed EviData were ingested; "
                . "files that failed or errored were skipped (not ingested) and "
                . "will be retried next run.\n";
            $body .= "See the EviData section above for the skipped files, and the "
                . "attached/linked report(s) for why they failed.\n";
        } elseif ($status === 'FAILURE' && ($s['evidata_files_checked'] ?? 0) > 0
            && ($s['evidata_files_passed'] ?? 0) === 0) {
            $body .= "✗ No files passed the EviData privacy check — nothing was ingested.\n";
            $body .= "See the attached/linked report(s) for details.\n";
        } elseif ($status === 'FAILURE') {
            $body .= "⚠ Some instruments failed to install or ingest.\n";
            $body .= "Check logs for details.\n";
        } elseif ($s['data_uploaded'] > 0) {
            $body .= "✔ Ingestion completed successfully.\n";
        } elseif ($s['data_skipped'] > 0 && $s['data_uploaded'] === 0) {
            $body .= "✔ Ingestion completed. All files skipped - no content changes detected (hash match).\n";
        } else {
            $body .= "✔ Ingestion completed. No data files to process.\n";
        }

        $body .= "\n";
        if ($this->runLogPath) {
            $body .= "Run log: {$this->runLogPath}\n";
        }
        if ($this->errorLogPath) {
            $body .= "Error log: {$this->errorLogPath}\n";
        }

        $this->log("  Sending notification to: " . implode(', ', $emailsToSend));

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
        fgetcsv($fh);
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