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
 *   pipeline NEVER writes to deidentified-raw/clinical/,
 *   deidentified-raw/bids/phenotype/ or documentation/data_dictionary/.
 *   It only writes to its own subdirectories: processed/clinical/,
 *   logs/clinical/, logs/evidata/.
 *
 * Data sources:
 *   deidentified-raw/clinical/        .csv and .tsv data files
 *   deidentified-raw/bids/phenotype/  BIDS phenotype .tsv data files
 *   documentation/data_dictionary/    ALL dictionaries: .linst / REDCap
 *                                     .csv / BIDS .json
 *   Both data directories feed one list (discoverDataFiles) so the
 *   privacy gate and the upload step always see the same files.
 *   Filenames must be unique across the two data directories.
 *
 * EviData privacy pre-flight gate — quasi-identifier (qis) resolution:
 *   QI lists are resolved with the following precedence:
 *     1. project.json -> evidata.qis  (per-project override, non-empty;
 *        the project fully owns its QI policy when present).
 *     2. config/evidata_config.json -> qis  (global baseline, non-empty;
 *        used only when a project does not define its own).
 *     3. ALL-HEADERS DEFAULT (privacy-policy approved): when qis is
 *        empty/absent at BOTH levels, every CSV column header is used as
 *        a QI, per file, MINUS columns listed in evidata.exclude_qis
 *        (two-level, case-insensitive). No column is removed on any
 *        other basis: the data is assessed as it stands, and inclusion
 *        or exclusion is a configuration decision only.
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

    /**
     * instrument name => its lowercased data-field list, populated by
     * instrumentDataFields(). Cached for the run so the field-matching
     * fallback costs one template fetch per instrument rather than one
     * per data file.
     *
     * @var array<string, array<string>>
     */
    private array $instrumentFieldCache = [];

    /**
     * LORIS project name for the project currently being processed,
     * taken from project.json -> candidate_defaults.project.
     *
     * Instrument availability is per PROJECT: /projects/{name}/instruments
     * returns that project's test battery. Without this, the client fell
     * back to whichever project the API happened to list first, so on a
     * multi-project LORIS every instrument check could be made against
     * the wrong battery. Null means the project did not configure a
     * project name and the client's fallback applies, with a warning.
     */
    private ?string $lorisProjectName = null;

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

    /**
     * Columns LORIS treats as STRUCTURAL — used to locate or create the
     * candidate and session — rather than as instrument data.
     *
     * Mirrors RedcapCSVParser::getEssentialHeaders() (with creation
     * columns) on the LORIS side, plus the equivalent identifiers used
     * by the non-REDCap formats. Kept in sync with that method: if
     * LORIS adds an essential column, add it here too.
     *
     * Used by detectInstrumentsFromFields(): every instrument template
     * carries these columns, so they carry NO signal about which
     * instrument a data file targets and must be excluded before
     * comparing field lists.
     */
    private const STRUCTURAL_COLUMNS = [
        // RedcapCSVParser::getEssentialHeaders() — creation columns
        'study_id', 'dob', 'sex', 'project', 'site',
        'redcap_event_name', 'cohort', 'edc',
        // REDCap structural extras
        'redcap_repeat_instrument', 'redcap_repeat_instance',
        'redcap_data_access_group',
        // LORIS_CSV / BIDS_TSV equivalents
        'pscid', 'candid', 'record_id', 'participant_id',
        'visit_label', 'session_id',
    ];

    /**
     * Metadata fields LORIS injects into EVERY LINST instrument —
     * see InstrumentDataParser::writeStandardLINSTFields(). They appear
     * in every instrument's expected-header template but are supplied
     * by LORIS, not by the data file: a REDCap export never carries
     * Date_taken or the static age/window fields.
     *
     * Excluded when matching field lists for the same reason as
     * STRUCTURAL_COLUMNS — present in all templates, so no signal — and
     * because requiring them would match NOTHING.
     *
     * getInstrumentHeaders() already strips CommentID / UserID /
     * Testdate / Examiner server-side; these four are what remain.
     */
    private const LINST_METADATA_COLUMNS = [
        'date_taken', 'candidate_age', 'gestational_age',
        'window_difference',
    ];

    /**
     * Clinical data directory, relative to the project mount.
     */
    private const CLINICAL_DIR = 'deidentified-raw/clinical';

    /**
     * BIDS phenotype directory, relative to the project mount.
     *
     * BIDS keeps tabular phenotype measures at the dataset root in
     * phenotype/. Phenotype data is clinical data, so the clinical
     * pipeline owns it: the .tsv files ingest as BIDS_TSV. The
     * instrument endpoint resolves participant_id / session_id itself —
     * see ClinicalClient::validateColumns(), which lists both as
     * structural columns — so nothing is remapped here.
     *
     * DATA ONLY. Data dictionaries are never read from here: every
     * dictionary, including a BIDS .json, lives in
     * documentation/data_dictionary/. A .json sitting next to a .tsv in
     * phenotype/ is ignored.
     *
     * Read-only, exactly like deidentified-raw/clinical.
     */
    private const PHENOTYPE_DIR = 'deidentified-raw/bids/phenotype';

    /**
     * Data-file extensions accepted from PHENOTYPE_DIR. Only .tsv.
     */
    private const PHENOTYPE_DATA_EXTENSIONS = ['tsv' => 'BIDS_TSV'];


    /** Key in a qis map supplying the QI list for files with no exact match. */
    private const QIS_DEFAULT_KEY = '_default';

    /**
     * Sentinel value for a per-file qis map entry meaning "use ALL of
     * this file's headers" (resolved via resolveAllHeaderQis(), so
     * exclude_qis still applies). Valid ONLY
     * as a per-filename value inside a qis map — not for '_default'
     * and not as a flat-list element.
     */
    private const QIS_ALL_HEADERS = '*';

    /**
     * Candidate-identifier column names, in priority order, matched
     * case-insensitively against the header. Drawn from the structural
     * columns ClinicalClient::validateColumns() treats as essential, so
     * the same names LORIS uses to locate a candidate are the ones row
     * tracking keys on. Overridable per project via
     * project.json -> row_tracking.identifier_columns.
     */
    private const ROW_ID_COLUMNS = [
        'PSCID', 'CandID', 'candid', 'participant_id', 'study_id', 'StudyID',
        // REDCap's default record identifier. Last in priority: when an
        // export carries both, study_id is what LORIS keys the candidate
        // on, so it wins. Present here so a stock REDCap export still
        // gets row-level tracking instead of falling back to whole-file.
        'record_id',
    ];

    /**
     * Session/visit column names, in priority order. A file with no
     * visit column still tracks rows — the key is then the identifier
     * alone.  Overridable via project.json -> row_tracking.visit_columns.
     */
    private const ROW_VISIT_COLUMNS = [
        'Visit_label', 'visit_label', 'session_id', 'redcap_event_name',
    ];

    /**
     * Extra columns folded into the row key when present. REDCap repeat
     * instruments put several rows on one candidate+event, which would
     * otherwise collapse to a single key.
     */
    private const ROW_QUALIFIER_COLUMNS = [
        'redcap_repeat_instrument', 'redcap_repeat_instance',
    ];

    /** Field separator used when hashing a row's cells. */
    private const ROW_HASH_SEPARATOR = "\x1F";

    private const DATE_COLUMN_NAMES = [
        'dob', 'date_of_birth', 'birth_date',
        'dod', 'date_of_death', 'death_date',
    ];

    /**
     * candidate_defaults keys consumed by the clinical pipeline, mapped to
     * the CSV column name they stamp. Extend later with e.g.
     * 'redcap_event_name' => 'redcap_event_name', 'visit_label' => 'Visit_label'.
     * Config is authoritative: every row gets the configured value.
     */
    private const CLINICAL_DEFAULT_COLUMNS = [
        'project' => 'Project',
        'cohort'  => 'Cohort',
        'site'    => 'Site',
    ];

    /**
     * Source sex encodings mapped to the three values LORIS accepts:
     * Male / Female / Other. Keys are lowercased; matching against the
     * file's cell value is case-insensitive.
     *
     * Distinct from CLINICAL_DEFAULT_COLUMNS: that map STAMPS a
     * configured value into a missing column, whereas this REWRITES an
     * existing value into the vocabulary LORIS requires. Both happen in
     * applyCandidateDefaults() so the processed copy is written once.
     *
     * A source encoding not covered here (e.g. numeric 1/2) is added
     * per project via project.json -> sex_mappings, which is merged
     * OVER this baseline.
     */
    private const SEX_VALUE_MAP = [
        'm' => 'Male',   'male'   => 'Male',
        'f' => 'Female', 'female' => 'Female',
        'o' => 'Other',  'other'  => 'Other',
    ];

    /**
     * Column names that hold the candidate's sex, matched
     * case-insensitively against the header. First match wins.
     */
    private const SEX_COLUMN_NAMES = ['sex', 'gender'];

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

        // api.timeout_seconds is per host: a data upload is not a quick
        // API call. LORIS writes a candidate, a session and an instrument
        // record per row synchronously inside the request, so an 84-row
        // file takes minutes, not seconds.
        $this->client = new ClinicalClient(
            $config['api']['base_url'],
            $config['api']['username'],
            $config['api']['password'],
            $config['api']['token_expiry_minutes'] ?? 55,
            $this->logger,
            $config['api']['api_version'] ?? 'v0.0.4-dev',
            (int)($config['api']['timeout_seconds'] ?? 600)
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
            $this->logger->info("║  - No data will be ingested into ARCHIMEDES              ║");
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

        $ddDir    = "{$mountPath}/documentation/data_dictionary";
        $dataDir  = "{$mountPath}/" . self::CLINICAL_DIR;
        $phenoDir = "{$mountPath}/" . self::PHENOTYPE_DIR;

        $this->lorisProjectName = trim((string)($project['candidate_defaults']['project'] ?? '')) ?: null;

        $this->logDir = "{$mountPath}/logs/clinical";
        $this->openRunLog();

        $this->log("========================================");
        $this->log("Project: {$name}");
        $this->log("Run: {$this->runTimestamp}");
        $this->log("DD dir    (read-only): {$ddDir}");
        $this->log("Data dir  (read-only): {$dataDir}");
        $this->log("Phenotype (read-only): {$phenoDir}"
            . (is_dir($phenoDir) ? "" : "  [absent - skipped]"));
        $this->log("========================================");
        $this->log("  ✓ Data accessible: {$mountPath}");
        if ($this->lorisProjectName !== null) {
            $this->log("  LORIS project (instrument scope): {$this->lorisProjectName}");
        } else {
            $this->log("  WARNING: candidate_defaults.project is not set — instrument"
                . " availability will be checked against whichever project the LORIS"
                . " API lists first, which may not be this one");
        }

        $this->installResults          = [];
        $this->dataResults             = [];
        $this->evidataLogDir           = null;
        $this->evidataNotificationSent = false;
        $this->evidataFailedFiles      = [];

        // Per-file privacy gate: each file is checked individually.
        // Files that pass are ingested; files that fail (bad verdict)
        // or error (no verdict) are skipped and retried next run. The
        // project is NOT aborted as a whole — passing files proceed.
        // One file list for the whole project: clinical CSV/TSV plus
        // BIDS phenotype TSV. The SAME list feeds the privacy gate and
        // the upload step, so the two can never drift apart.
        $dataFiles = $this->discoverDataFiles($dataDir, $phenoDir);

        $evidataOutcome = $this->runEvidataPreflight($project, $mountPath, $dataFiles);

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
        $this->uploadFromDirectory($project, $dataFiles);

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
                . " CandID(s) already in ARCHIMEDES");
        } catch (\Exception $e) {
            $this->log("  Pre-run candidate snapshot FAILED: " . $e->getMessage()
                . " - new-candidate count unavailable for this run");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  Small shared helpers
    // ══════════════════════════════════════════════════════════════════

    /**
     * Field delimiter for a data format.
     *
     * ONE definition, used by every reader and writer in this class.
     * Previously the delimiter was derived in two independent ways —
     * from $format in the upload path and from the file extension in the
     * EviData path — which agreed only because DATA_EXTENSIONS happens to
     * map tsv => BIDS_TSV. Two sources of truth for the same fact is a
     * latent bug: the privacy gate and the uploader could parse the same
     * file differently. delimiterForPath() now resolves the extension
     * THROUGH the same map, so the two can no longer drift.
     */
    private function delimiterForFormat(string $format): string
    {
        return ($format === 'BIDS_TSV') ? "\t" : ',';
    }

    /**
     * Field delimiter for a path, resolved via DATA_EXTENSIONS so it
     * always agrees with delimiterForFormat(). An unknown extension
     * falls back to comma, matching the previous behaviour.
     */
    private function delimiterForPath(string $path): string
    {
        $ext    = strtolower(pathinfo($path, PATHINFO_EXTENSION));
        $format = self::DATA_EXTENSIONS[$ext] ?? 'LORIS_CSV';
        return $this->delimiterForFormat($format);
    }

    /**
     * Read ONLY the header row of a delimited file and close it again.
     *
     * Returns null when the file cannot be opened or has no usable
     * header — callers decide whether that is fatal for them. Replaces
     * four identical open/fgetcsv/fclose blocks; the methods that need
     * to keep reading after the header (applyCandidateDefaults,
     * normalizeDatesInFile, computeRowHashes) still manage their own
     * handle, since they consume the body in the same pass.
     *
     * @return array<int,string>|null
     */
    private function readHeaderRow(string $path, string $delimiter): ?array
    {
        $fh = @fopen($path, 'r');
        if ($fh === false) {
            return null;
        }
        $headers = fgetcsv($fh, 0, $delimiter);
        fclose($fh);

        if (!is_array($headers) || $headers === []) {
            return null;
        }
        return array_map(fn($h) => (string)$h, $headers);
    }

    /**
     * Group filenames by the reason recorded for them and render the
     * "a, b [reason]; c [reason]" fragment used in the notification
     * email. Replaces three near-identical inline loops.
     *
     * @param array<string>              $files   filenames to group
     * @param array<string,array>        $results result map keyed by filename
     * @param string                     $key     result key holding the reason
     */
    private function groupByReason(
        array $files,
        array $results,
        string $key,
        string $default
    ): string {
        $byReason = [];
        foreach ($files as $file) {
            $reason = $results[$file][$key] ?? $default;
            $byReason[$reason][] = $file;
        }

        $parts = [];
        foreach ($byReason as $reason => $group) {
            $parts[] = implode(', ', $group) . " [{$reason}]";
        }
        return implode('; ', $parts);
    }

    // ══════════════════════════════════════════════════════════════════
    //  EviData pre-flight
    // ══════════════════════════════════════════════════════════════════

    /**
     * Run the EviData pre-flight against every data file the project
     * offers, PER FILE. Records the basenames that did NOT pass in
     * $this->evidataFailedFiles so ingestion can skip them.
     *
     * The file list is supplied by discoverDataFiles() — clinical CSV/TSV
     * plus BIDS phenotype TSV — so phenotype data goes through exactly
     * the same privacy gate as any other clinical file.
     *
     * @param array<array{path:string,name:string,format:string,source:string}> $dataFiles
     *
     * Returns one of:
     *   'skipped'    — gate disabled / no data files
     *   'all_passed' — every file passed
     *   'partial'    — some passed, some failed/errored
     *   'all_failed' — no file passed
     */
    private function runEvidataPreflight(array $project, string $mountPath, array $dataFiles): string
    {
        $evi = $this->resolveEvidataConfig();
        if ($evi === null) {
            $this->log("  EviData: not enabled in evidata_config.json — skipping pre-flight");
            return 'skipped';
        }

        $csvFiles = array_column($dataFiles, 'path');
        sort($csvFiles);

        if (empty($csvFiles)) {
            $this->log("  EviData: no CSV/TSV data files for this project — nothing to check");
            return 'skipped';
        }

        $phenoCount = count(array_filter(
            $dataFiles,
            fn(array $f) => ($f['source'] ?? 'clinical') === 'phenotype'
        ));

        $this->log("");
        $this->log("──── EVIDATA PRE-FLIGHT" . ($this->dryRun ? " [DRY RUN]" : "") . " ────");
        $this->log("  Checking " . count($csvFiles) . " file(s) against EviData"
            . ($phenoCount > 0 ? " ({$phenoCount} from BIDS phenotype/)" : ""));
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
        $excludeSet = $this->resolveExcludeQis($project, $evi);
        [$qisByPath, $qiResolveErr] = $this->resolveQisByPath($csvFiles, $qisConfig, $excludeSet);

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
            $this->recordLocalQiFailures($qiErrors, $qisByPath);
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
        $this->recordRemoteEvidataResults($results);

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
     * Resolve the QI list for every file up front.
     *
     * In ALL-HEADERS mode ($qisConfig === null) each file's headers are
     * read and pruned against exclude_qis; otherwise the explicit config
     * is resolved and any configured QI absent from that file is dropped
     * with a report. A file whose QI list cannot be resolved at all is
     * returned in the error map rather than throwing, so one broken file
     * never stops the others being checked.
     *
     * @param array<string>       $csvFiles   absolute paths
     * @param array|null          $qisConfig  explicit config, or null for all-headers
     * @param array<string,true>  $excludeSet lowercased exclude set
     * @return array{0: array<string,array<string>>, 1: array<string,string>}
     *         [path => QI list, basename => error message]
     */
    private function resolveQisByPath(array $csvFiles, ?array $qisConfig, array $excludeSet): array
    {
        $qisByPath    = [];
        $qiResolveErr = [];

        foreach ($csvFiles as $path) {
            try {
                if ($qisConfig === null) {
                    // ALL-HEADERS mode (privacy-policy approved default)
                    $qisByPath[$path] = $this->resolveAllHeaderQis(
                        $path, $excludeSet, basename($path)
                    );
                    continue;
                }

                // Explicit qis config: resolve the configured list, then
                // drop any column that isn't actually in THIS file. A
                // configured QI absent from the file is reported and
                // skipped — the file is still checked against the QIs
                // that ARE present. Only an empty remainder is fatal.
                $resolved = $this->resolveQisForFile($qisConfig, basename($path));

                if ($resolved === [self::QIS_ALL_HEADERS]) {
                    // Per-file all-headers sentinel ("*"): this file uses
                    // every header (minus exclude_qis), exactly like
                    // project-wide all-headers mode, even though other
                    // files in the same project use explicit lists.
                    $qisByPath[$path] = $this->resolveAllHeaderQis(
                        $path, $excludeSet, basename($path)
                    );
                } else {
                    $qisByPath[$path] = $this->pruneMissingQis(
                        $path, $resolved, basename($path)
                    );
                }
            } catch (\RuntimeException $e) {
                $qiResolveErr[basename($path)] = $e->getMessage();
            }
        }

        return [$qisByPath, $qiResolveErr];
    }

    /**
     * Record files that failed LOCAL QI validation (unreadable, missing
     * configured QI columns, or a resolve error) as failed, and remove
     * them from the remote-check set so they are never sent to EviData.
     *
     * @param array<string,array<string>> $qiErrors  basename => missing/marker
     * @param array<string,array<string>> $qisByPath modified in place
     */
    private function recordLocalQiFailures(array $qiErrors, array &$qisByPath): void
    {
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

    /**
     * Tally each file's remote verdict, persist its artifacts, and record
     * the failures so ingestion skips them.
     *
     * A file is INGESTED only if overall_passed=true. A false verdict OR
     * an error (no verdict) marks it failed.
     *
     * @param array<string,array> $results basename => EviData result
     */
    private function recordRemoteEvidataResults(array $results): void
    {
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

            foreach ($this->persistEvidataArtifacts($name, $r) as $path) {
                $this->log("    artifact: " . basename($path));
            }
        }
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
        // use every CSV header as a QI, minus exclude_qis.
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
     * minus the configured exclude_qis. Nothing else is pruned — the file
     * is assessed as it stands, and what counts as a quasi-identifier is
     * a configuration decision (evidata.qis to include, exclude_qis to
     * drop), not something inferred from the values. Logs exactly what
     * was used and excluded so every all-headers run is auditable.
     *
     * @param array<string,true> $excludeSet  lowercased exclude set.
     * @return array  QI column names (original casing) to send.
     * @throws \RuntimeException if unreadable or nothing remains.
     */
    private function resolveAllHeaderQis(string $path, array $excludeSet, string $basename): array
    {
        $headers = $this->readHeaderRow($path, $this->delimiterForPath($path));
        if ($headers === null) {
            throw new \RuntimeException("CSV unreadable: {$basename}");
        }

        $kept = [];
        $excludedByConfig = [];
        foreach ($headers as $h) {
            $key = strtolower(trim($h));
            if ($key === '') {
                continue;
            }
            if (isset($excludeSet[$key])) {
                $excludedByConfig[] = $h;
            } else {
                $kept[] = $h;
            }
        }

        $this->log(sprintf(
            "    %s — ALL HEADERS: %d of %d columns used (excluded %d by config)",
            $basename, count($kept), count($headers), count($excludedByConfig)
        ));
        if ($excludedByConfig) {
            $this->log("      excluded (config): " . implode(', ', $excludedByConfig));
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
        $headers = $this->readHeaderRow($path, $this->delimiterForPath($path));
        if ($headers === null) {
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
            $headers = $this->readHeaderRow($path, $this->delimiterForPath($path));
            if ($headers === null) {
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

    /**
     * Filesystem-safe stem for a file's EviData artifacts.
     *
     * The EXTENSION IS KEPT. Two data files may share a stem but differ
     * in extension — moca.csv in deidentified-raw/clinical and moca.tsv
     * in bids/phenotype are distinct files with distinct privacy
     * results. Stripping the extension would make both write
     * moca.results.json / moca.report.zip into the same run directory,
     * silently overwriting one verdict with the other and attaching the
     * wrong PDF to the failure email.
     *
     * Must be the single source of this value: persistEvidataArtifacts()
     * writes the files and collectEvidataPdfs() reads them back, so the
     * two have to agree exactly.
     */
    private function evidataArtifactStem(string $sourceName): string
    {
        return preg_replace('/[^A-Za-z0-9._-]/', '_', $sourceName) ?: 'artifact';
    }

    private function persistEvidataArtifacts(string $sourceName, array $result): array
    {
        if ($this->evidataLogDir === null) {
            return [];
        }

        $stem    = $this->evidataArtifactStem($sourceName);
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
            $stem = $this->evidataArtifactStem($name);
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

        [$attachments, $totalBytes, $attachTier] =
            ($clientErrorOverride === null && $this->evidataLogDir !== null)
                ? $this->selectEvidataAttachments()
                : [[], 0, 'none'];

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

    /**
     * Choose which PDFs to attach to the failure email.
     *
     * Graceful fallback ladder. The MTA size limit applies to the WHOLE
     * message, so the decision is made on each batch's TOTAL size, not
     * per file:
     *   1. full report.pdf set — attach as-is if the total fits; if it
     *      exceeds the cap, compress the whole set and use it if the
     *      compressed total now fits.
     *   2. summary_letter.pdf set — same rule.
     *   3. nothing — the caller tells the recipient to contact the team.
     *
     * EviData's report.zip bundles both report.pdf (full) and
     * summary_letter.pdf (summary) per file.
     *
     * @return array{0: array<array{path:string,name:string}>, 1: int, 2: string}
     *         [attachments, totalBytes, tier] where tier is
     *         'full' | 'summary' | 'none'.
     */
    private function selectEvidataAttachments(): array
    {
        [$fullAtt,    $fullBytes]    = $this->collectEvidataPdfs('report.pdf', '_report.pdf', '_evidata_report.pdf');
        [$summaryAtt, $summaryBytes] = $this->collectEvidataPdfs('summary_letter.pdf', '_summary.pdf', '_evidata_summary.pdf');

        // Per-host attachment ceiling, derived from the MTA limit in
        // config (post-base64 headroom already applied).
        $maxBytes = $this->maxAttachBytes();
        $capMb    = $maxBytes / 1024 / 1024;

        // ── Tier 1: full reports ────────────────────────────────────
        if (!empty($fullAtt)) {
            if ($fullBytes <= $maxBytes) {
                return [$fullAtt, $fullBytes, 'full'];   // fits as-is
            }
            $this->log(sprintf(
                "  EviData attachments: full report batch is %.1f MB, "
                . "exceeds the %.1f MB email cap — compressing the batch",
                $fullBytes / 1024 / 1024, $capMb
            ));
            [$fullC, $fullCBytes] = $this->compressAttachmentBatch($fullAtt);
            if ($fullCBytes <= $maxBytes) {
                return [$fullC, $fullCBytes, 'full'];
            }
        }

        // ── Tier 2: summaries ───────────────────────────────────────
        if (!empty($summaryAtt)) {
            if ($summaryBytes <= $maxBytes) {
                return [$summaryAtt, $summaryBytes, 'summary'];
            }
            $this->log(sprintf(
                "  EviData attachments: summary batch is %.1f MB, "
                . "exceeds the %.1f MB email cap — compressing the batch",
                $summaryBytes / 1024 / 1024, $capMb
            ));
            [$sumC, $sumCBytes] = $this->compressAttachmentBatch($summaryAtt);
            if ($sumCBytes <= $maxBytes) {
                return [$sumC, $sumCBytes, 'summary'];
            }
        }

        // ── Tier 3: nothing fits ────────────────────────────────────
        return [[], 0, 'none'];
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
            $body .= "Status    : PREVIEW — no ARCHIMEDES writes attempted (dry run)\n\n";
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
                $this->verifyRedcapDictionary($filePath, $filename, $type);
                return;
            }

            if (stripos($msg, '409') !== false || stripos($msg, 'already') !== false) {
                $this->log("    Already installed ({$elapsed}s)");
                $this->installResults[$filename] = ['status' => 'exists', 'type' => $type, 'time' => $elapsed];
                $this->stats['dd_already_existed']++;
                $this->verifyRedcapDictionary($filePath, $filename, $type);
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
                $this->verifyRedcapDictionary($filePath, $filename, $type);
                return;
            }
            $this->log("    EXCEPTION: {$emsg}");
            $this->writeError($filename, "Install exception: {$emsg}");
            $this->installResults[$filename] = ['status' => 'failed', 'type' => $type, 'error' => $emsg];
            $this->stats['dd_failed']++;
        }
    }

    /**
     * Confirm that a REDCap dictionary reported as installed actually
     * produced instruments this project can use.
     *
     * "Already installed" is normally the right answer and the pipeline
     * treats it as success. It is NOT always true. An instrument has to
     * exist in three places, and the install can report success while
     * only the first is satisfied:
     *
     *   1. the instruments directory  — the generated instrument file
     *   2. test_names                 — the registry row LORIS looks up
     *   3. test_battery               — active for this project + visit
     *
     * A stale or empty file in the instruments directory is enough for
     * LORIS to answer "already exists" and skip the work, leaving no
     * test_names row behind it. Every subsequent run then reports a
     * clean install, skips again, and the data files that target those
     * instruments fail much later with a message that says nothing about
     * the dictionary.
     *
     * This closes that loop: read the dictionary's own Form Name column
     * and check each form against the project's instrument list. Costs
     * one header-and-column read of a file already on disk plus a set
     * comparison against a list the run fetches anyway.
     *
     * REDCap dictionaries only. A .linst declares its instrument name
     * inside the file in a form this method does not parse, and a BIDS
     * .json maps to instruments differently; both are skipped rather
     * than guessed at.
     *
     * Reported as a NOTE, never as a failure. The pipeline can only see
     * the project's instrument list, and an instrument can be correctly
     * installed yet absent from it because it has not been added to the
     * test battery. The genuine error - a data file targeting a form
     * that is not available - is reported by processOneDataFile() when
     * that file is processed, by name.
     */
    private function verifyRedcapDictionary(string $filePath, string $filename, string $type): void
    {
        if ($type !== 'redcap') {
            return;
        }

        $forms = $this->redcapDictionaryForms($filePath);
        if ($forms === []) {
            $this->log("    NOTE: could not read a 'Form Name' column from {$filename}"
                . " - skipping post-install verification");
            return;
        }

        // The install may have just created instruments; make sure the
        // list is fetched fresh rather than served from an earlier probe.
        $this->client->clearInstrumentCache();
        $available = $this->client->getInstalledInstruments($this->lorisProjectName);

        $missing = array_values(array_diff($forms, $available));

        if ($missing === []) {
            $this->log("    ✓ verified: all " . count($forms)
                . " form(s) available to this project");
            return;
        }

        $projectLabel = $this->lorisProjectName ?? '(project not configured)';
        $shown        = implode(', ', array_slice($missing, 0, 15));
        $suffix       = count($missing) > 15
            ? ' ... and ' . (count($missing) - 15) . ' more'
            : '';

        // NOT reported as a failure. The list the pipeline can see is the
        // project's instrument list; an instrument can be correctly
        // installed - present in test_names, file on disk - and still be
        // absent from it because nobody has added it to the test battery
        // yet. That is a separate, deliberate step, not a fault in the
        // dictionary or its installation.
        //
        // The genuine error surfaces later and precisely: if a data file
        // targets one of these forms, processOneDataFile() reports it by
        // name and fails that file. Failing the install here as well would
        // report the same fact twice, once wrongly.
        $this->log(sprintf(
            "    NOTE: %s installed. %d of %d form(s) it defines are not in"
            . " project %s's instrument list: %s%s",
            $filename, count($missing), count($forms), $projectLabel, $shown, $suffix
        ));
        $this->log("    That is expected if they have not been added to the test"
            . " battery yet - installing a dictionary registers its instruments,"
            . " it does not activate them for a project. Only an issue if a data"
            . " file targets one of these forms, which is reported separately"
            . " when that file is processed.");
    }

    /**
     * Distinct form names declared by a REDCap data dictionary.
     *
     * Matches the 'Form Name' column case-insensitively and tolerates
     * the underscored variant some exports use.
     *
     * @return array<string>
     */
    private function redcapDictionaryForms(string $filePath): array
    {
        $fh = @fopen($filePath, 'r');
        if ($fh === false) {
            return [];
        }

        $headers = fgetcsv($fh, 0, ',');
        if (!is_array($headers)) {
            fclose($fh);
            return [];
        }

        $idx = null;
        foreach ($headers as $i => $h) {
            $key = strtolower(trim((string)$h));
            if ($key === 'form name' || $key === 'form_name') {
                $idx = $i;
                break;
            }
        }
        if ($idx === null) {
            fclose($fh);
            return [];
        }

        $forms = [];
        while (($row = fgetcsv($fh, 0, ',')) !== false) {
            $name = isset($row[$idx]) ? trim((string)$row[$idx]) : '';
            if ($name !== '' && !in_array($name, $forms, true)) {
                $forms[] = $name;
            }
        }
        fclose($fh);

        // Forms the pipeline never ingests carry no signal here either.
        return array_values(array_diff($forms, self::DEFAULT_EXCLUDE_FORMS));
    }

    // ══════════════════════════════════════════════════════════════════
    //  STEP 2: Upload data, file by file
    // ══════════════════════════════════════════════════════════════════

    /**
     * Build the project's data-file list: everything in
     * deidentified-raw/clinical (.csv, .tsv) plus every BIDS phenotype
     * .tsv in deidentified-raw/bids/phenotype.
     *
     * Called ONCE per project. The same list is handed to the EviData
     * gate and to the upload step, so a file can never be ingested
     * without having been privacy-checked, or checked without being
     * considered for ingestion.
     *
     * Basenames must be unique ACROSS both directories. Tracking
     * (.clinical_tracking.json), per-file results, EviData artifacts and
     * the processed copy are all keyed by basename, so a duplicate would
     * silently overwrite the other file's state. A colliding phenotype
     * file is therefore reported as a failure and excluded, rather than
     * ingested under a key that already belongs to a clinical file.
     *
     * @return array<array{path:string,name:string,format:string,source:string}>
     */
    private function discoverDataFiles(string $clinicalDir, string $phenoDir): array
    {
        $files = [];
        $seen  = [];   // basename => source dir label

        if (is_dir($clinicalDir)) {
            foreach (self::DATA_EXTENSIONS as $ext => $format) {
                foreach (glob("{$clinicalDir}/*.{$ext}") ?: [] as $path) {
                    $name         = basename($path);
                    $seen[$name]  = 'clinical';
                    $files[]      = [
                        'path'   => $path,
                        'name'   => $name,
                        'format' => $format,
                        'source' => 'clinical',
                    ];
                }
            }
        }

        if (is_dir($phenoDir)) {
            $phenoCount = 0;
            foreach (self::PHENOTYPE_DATA_EXTENSIONS as $ext => $format) {
                foreach (glob("{$phenoDir}/*.{$ext}") ?: [] as $path) {
                    $name = basename($path);

                    if (isset($seen[$name])) {
                        $msg = "phenotype/{$name} has the same filename as a file "
                            . "in {$seen[$name]}/ — not ingested. Tracking, privacy "
                            . "artifacts and the processed copy are keyed by "
                            . "filename, so rename one of them.";
                        $this->log("  [phenotype/{$name}] FAILED - duplicate filename");
                        $this->writeError("phenotype/{$name}", $msg);
                        $this->dataResults["phenotype/{$name}"] = [
                            'status' => 'failed',
                            'reason' => "duplicate filename (also in {$seen[$name]}/)",
                        ];
                        $this->stats['data_failed']++;
                        continue;
                    }

                    // Same stem, different extension: clinical/moca.csv
                    // and phenotype/moca.tsv are separate files (own
                    // tracking entry, own privacy verdict, own artifacts
                    // — evidataArtifactStem keeps the extension) but
                    // processOneDataFile derives the instrument from the
                    // stem, so BOTH upload to instrument "moca". Legal,
                    // and ARCHIMEDES dedupes the rows, but worth surfacing so
                    // a mistakenly duplicated export is not mistaken for
                    // two datasets.
                    $stem = pathinfo($name, PATHINFO_FILENAME);
                    foreach (array_keys($seen) as $seenName) {
                        if (pathinfo($seenName, PATHINFO_FILENAME) === $stem) {
                            $this->log("  [phenotype/{$name}] NOTE: targets the "
                                . "same instrument '{$stem}' as {$seenName} "
                                . "(different extension). Both will be ingested.");
                            break;
                        }
                    }

                    $seen[$name] = 'phenotype';
                    $files[]     = [
                        'path'   => $path,
                        'name'   => $name,
                        'format' => $format,
                        'source' => 'phenotype',
                    ];
                    $phenoCount++;
                }
            }
            if ($phenoCount > 0) {
                $this->log("  BIDS phenotype: {$phenoCount} .tsv file(s) found in {$phenoDir}");
            }
        }

        usort($files, fn(array $a, array $b) => strcmp($a['name'], $b['name']));

        return $files;
    }

    /**
     * @param array<array{path:string,name:string,format:string,source:string}> $files
     *        Prepared by discoverDataFiles(): clinical + BIDS phenotype.
     */
    private function uploadFromDirectory(array $project, array $files): void
    {
        $this->log("");
        $this->log("──── STEP 2: UPLOAD CLINICAL DATA ────");

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

            if (($f['source'] ?? 'clinical') === 'phenotype') {
                $this->log("  [{$filename}] source: BIDS phenotype/");
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

        // Row-level change detection, computed from the ORIGINAL before
        // any enrichment. --force skips it: the whole file goes up.
        // A null plan means "send everything" (first upload, no usable
        // identifier column, or no stored row map yet).
        $rowPlan = $this->force
            ? null
            : $this->planRowUpload($project, $filename, $filePath, $format);

        if ($rowPlan !== null && $rowPlan['skip']) {
            // The file changed — reordered columns, whitespace, a removed
            // row — but no row's content did. Nothing to send. Record the
            // new file hash so the next run takes the fast path.
            $this->log("    SKIPPED - file changed but no row content changed"
                . ($rowPlan['removed'] > 0
                    ? " ({$rowPlan['removed']} row(s) no longer present; LORIS is never asked to delete)"
                    : ""));
            $this->dataResults[$filename] = [
                'status'        => 'skipped',
                'reason'        => 'no row changes',
                'rows'          => $rows,
                'change_status' => $changeStatus,
            ];
            $this->stats['data_skipped']++;
            $this->updateTracking($filename, $filePath, [], $rowPlan['current']);
            return;
        }

        // Stamp project/cohort/site from project.json candidate_defaults
        // BEFORE anything touches the instrument endpoint. Original stays
        // read-only; ingestion reads the processed copy.
        $sourcePath = $this->applyCandidateDefaults($project, $filePath, $format);
        if ($sourcePath === null) {
            $this->writeError($filename, "candidate_defaults: processed copy could not be created");
            $this->dataResults[$filename] = [
                'status'        => 'failed',
                'reason'        => 'candidate_defaults processed copy failed',
                'rows'          => $rows,
                'change_status' => $changeStatus,
            ];
            $this->stats['data_failed']++;
            return;
        }

        // Ambiguous two-number dates (5/6/2024) are resolved with the
        // project's declared order; unambiguous ones (5/23/2024) are
        // resolved from the data regardless. Default 'mdy' matches the
        // date_mdy validation REDCap dictionaries normally declare.
        $dateOrder = strtolower((string)($project['date_input_format'] ?? 'mdy'));

        // Date normalisation writes into processed/clinical/ rather than a
        // temp file, so the processed copy IS the file that was uploaded -
        // defaults stamped, sex normalised, dates corrected. When a row is
        // rejected for a bad date or a missing site/cohort, the operator
        // can open that file and see the value LORIS actually received.
        $processedDir  = rtrim(
                $project['data_access']['mount_path'] ?? $project['_projectPath'],
                '/'
            ) . '/processed/clinical';
        $processedPath = "{$processedDir}/{$filename}";

        if (!is_dir($processedDir) && !@mkdir($processedDir, 0755, true)
            && !is_dir($processedDir)
        ) {
            $this->log("    WARNING: {$processedDir} is not writable - date"
                . " normalisation will use a temp file and the processed copy"
                . " will NOT reflect what was uploaded");
            $processedPath = null;
        }

        $uploadPath = $this->normalizeDatesInFile(
            $sourcePath, $format, $dateOrder, $processedPath
        );

        // The processed copy is a deliverable, not scratch - never unlinked.
        $usingTemp = ($uploadPath !== $sourcePath && $uploadPath !== $processedPath);

        if ($uploadPath === $processedPath) {
            $this->log("    Processed copy (as uploaded): processed/clinical/{$filename}");
        }

        // Narrow the upload to the new/changed rows. Done AFTER enrichment
        // and date normalisation, both of which rewrite row-for-row in
        // order, so the indices computed from the original still line up.
        if ($rowPlan !== null && !$rowPlan['all']) {
            $filtered = $this->filterRowsByIndex($uploadPath, $rowPlan['keep'], $format);
            if ($filtered !== null) {
                if ($usingTemp) {
                    @unlink($uploadPath);
                }
                $uploadPath = $filtered;
                $usingTemp  = true;
                $rows       = count($rowPlan['keep']);
                $this->log("    Uploading {$rows} of "
                    . ($rowPlan['new'] + $rowPlan['changed'] + $rowPlan['unchanged'])
                    . " row(s)");
            } else {
                $this->log("    Row filter failed - uploading the full file "
                    . "(LORIS will skip rows that already exist)");
            }
        }

        // Last gate before the API. The file has been enriched as far as
        // the pipeline can take it; anything still invalid would be
        // rejected by LORIS row by row, with no row numbers and no
        // context. Fail here instead, naming every problem at once.
        $problems = $this->preflightRows($uploadPath, $format, $project);
        if ($problems !== []) {
            $this->log("    FAILED validation - not sent to ARCHIMEDES:");
            foreach ($problems as $pb) {
                $this->log("      - {$pb}");
            }
            $this->writeError($filename,
                "Validation failed before upload, nothing was sent: "
                . implode(' | ', $problems)
            );
            $this->dataResults[$filename] = [
                'status'        => 'failed',
                'reason'        => 'failed validation before upload',
                'instruments'   => [],
                'rows'          => $rows,
                'change_status' => $changeStatus,
            ];
            $this->stats['data_failed']++;
            if ($usingTemp && file_exists($uploadPath)) {
                @unlink($uploadPath);
            }
            return;
        }

        try {
            // Shortcut: a file named after a single instrument uploads
            // straight to it. A miss here is expected for any file holding
            // more than one instrument's data, and is not reported.
            if ($this->client->instrumentExists($baseName, $this->lorisProjectName)) {
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
                    $this->updateTracking($filename, $filePath, $result, $rowPlan['current'] ?? null);
                    $this->archiveSnapshot($project, $filePath);
                } elseif (($result['reason'] ?? '') === 'ingestion not confirmed (rows may already exist)') {
                    // Nothing to archive - nothing was written. Record the hash
                    // anyway: LORIS returns this same empty response every time
                    // the rows already exist, so without tracking the file would
                    // be re-sent on every run for ever.
                    $this->updateTracking($filename, $filePath, [], $rowPlan['current'] ?? null);
                }
                return;
            }

            $detected    = $this->detectInstrumentsFromHeaders($uploadPath, $format);
            $instruments = $detected['available'];

            if ($instruments === [] && $detected['named'] !== []) {
                // The file DOES declare its instruments via _complete
                // columns — they are simply not available to this
                // project. Field matching cannot help: it iterates the
                // very same project instrument list that just rejected
                // them. Fail immediately, naming them, instead of
                // spending an HTTP call per installed instrument to
                // reach the same answer.
                $missing = implode(', ', $detected['named']);
                $this->log("    FAILED - the file targets " . count($detected['named'])
                    . " instrument(s) that are NOT available to this project: {$missing}");
                $this->log("    The data dictionary may be installed without the"
                    . " instruments having been added to the project's test battery."
                    . " Check the LORIS Test Battery module for this project.");
                $this->writeError($filename,
                    "Instruments named by _complete columns are not available to this "
                    . "project: {$missing}. Installing the data dictionary registers an "
                    . "instrument; it does not add it to the project's test battery. "
                    . "Add them to the battery for the visit label(s) being ingested."
                );
                $this->dataResults[$filename] = [
                    'status'        => 'failed',
                    'reason'        => 'instruments not in project battery',
                    'instruments'   => [],
                    'rows'          => $rows,
                    'change_status' => $changeStatus,
                ];
                $this->stats['data_failed']++;
                return;
            }

            if ($instruments === []) {
                // The file does not name its instruments via <form>_complete
                // columns. That is not a problem and not a REDCap
                // requirement: RedcapCSVParser never reads those columns,
                // and getEssentialHeaders() asks only for study_id and
                // redcap_event_name. They are simply a convenient shortcut
                // when present. Resolve the target instrument(s) by matching
                // each available instrument's field list against the file's
                // columns instead.
                $this->log("    Matching instruments by field name");
                $instruments = $this->detectInstrumentsFromFields($uploadPath, $format);
            }

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
                $this->updateTracking($filename, $filePath, $result, $rowPlan['current'] ?? null);
                $this->archiveSnapshot($project, $filePath);
            } elseif (($result['reason'] ?? '') === 'ingestion not confirmed (rows may already exist)') {
                // See above: record the hash so an unchanged file is not re-sent.
                $this->updateTracking($filename, $filePath, [], $rowPlan['current'] ?? null);
            }
        } finally {
            if ($usingTemp && file_exists($uploadPath)) {
                @unlink($uploadPath);
            }
        }
    }

    /**
     * Stamp project/cohort/site from project.json -> candidate_defaults
     * onto a data file BEFORE it reaches the instrument upload endpoint.
     * Same source of truth as the BIDS participants.tsv enrichment.
     *
     * Config is a FALLBACK, not an override. For each key in
     * CLINICAL_DEFAULT_COLUMNS present in candidate_defaults:
     *   - column absent from the header -> appended, stamped on every row
     *   - column present, cell blank    -> filled from config
     *   - column present, cell populated-> left alone (the file wins)
     * A key absent from candidate_defaults never stamps.
     *
     * ALSO normalises the sex column, when the file has one, into the
     * Male/Female/Other vocabulary LORIS requires - see SEX_VALUE_MAP.
     * This is a REWRITE of existing values, not a stamp of a configured
     * one: a blank cell stays blank (candidate_defaults.sex is not
     * applied here, since stamping an unknown sex would fabricate data)
     * and an unrecognised encoding is left untouched and logged, so it
     * surfaces as a LORIS rejection rather than a silent wrong answer.
     * Both operations share this one pass so the processed copy is
     * written once.
     *
     * A file that needs nothing gets no processed copy: the original path
     * is returned and ingestion reads it directly. In practice only the
     * REDCap exports, which omit these columns, reach processed/clinical/.
     *
     * The original in deidentified-raw/ is NEVER modified (read-only
     * contract). Returns the processed path; the ORIGINAL path when
     * nothing needed a default; or null on read/write failure - the
     * caller fails the file rather than ingesting without the defaults.
     */
    private function applyCandidateDefaults(array $project, string $srcPath, string $format): ?string
    {
        $defaults = $project['candidate_defaults'] ?? [];

        // Only the keys this pipeline consumes, and only when present.
        // REDCap-format files use lowercase structural column names
        // (project/site/cohort) - stamp with matching casing so the
        // server-side header check recognises the appended columns.
        $isRedcap = $this->client->isRedcapCSV($srcPath);

        $apply = [];   // CSV column name => value
        foreach (self::CLINICAL_DEFAULT_COLUMNS as $key => $column) {
            if (isset($defaults[$key]) && $defaults[$key] !== '') {
                $col         = $isRedcap ? strtolower($column) : $column;
                $apply[$col] = (string)$defaults[$key];
            }
        }
        // NOTE: no early return on an empty $apply. A file may need no
        // stamped defaults but still carry sex values in a form LORIS
        // rejects (M/F), so the header has to be read before deciding
        // whether a processed copy is needed at all. The decision is
        // made below, once both $apply and $sexIdx are known.

        // Per-project overrides merged over the baseline vocabulary.
        $sexMap = array_change_key_case(
            array_merge(self::SEX_VALUE_MAP, $project['sex_mappings'] ?? []),
            CASE_LOWER
        );

        $delimiter = $this->delimiterForFormat($format);
        $basename  = basename($srcPath);

        $in = fopen($srcPath, 'r');
        if ($in === false) {
            $this->log("    candidate_defaults: cannot read {$basename}");
            return null;
        }
        $headers = fgetcsv($in, 0, $delimiter);
        if (!is_array($headers) || $headers === []) {
            fclose($in);
            $this->log("    candidate_defaults: no header row in {$basename}");
            return null;
        }

        // Match case-insensitively: an existing column keeps the file's
        // casing and is only filled where blank; a missing one is
        // appended and stamped on every row.
        $headersLower = array_map(fn($h) => strtolower(trim((string)$h)), $headers);
        $origCount    = count($headers);
        $fillIndex    = [];   // column => index of the existing column
        $appended     = [];   // columns added to the header
        foreach (array_keys($apply) as $column) {
            $idx = array_search(strtolower($column), $headersLower, true);
            if ($idx !== false) {
                $fillIndex[$column] = $idx;
            } else {
                $appended[] = $column;
                $headers[]  = $column;
            }
        }

        // Locate the sex column, if the file has one. First name in
        // SEX_COLUMN_NAMES that appears in the header wins.
        $sexIdx = null;
        foreach (self::SEX_COLUMN_NAMES as $sexCol) {
            $i = array_search($sexCol, $headersLower, true);
            if ($i !== false) {
                $sexIdx = $i;
                break;
            }
        }

        // Nothing to stamp AND no sex column to normalise - the copy
        // would be byte-identical to the original, so skip it entirely.
        if ($apply === [] && $sexIdx === null) {
            fclose($in);
            return $srcPath;
        }

        $outDir = rtrim($project['data_access']['mount_path'] ?? $project['_projectPath'], '/')
            . '/processed/clinical';
        if (!is_dir($outDir) && !mkdir($outDir, 0755, true)) {
            fclose($in);
            $this->log("    candidate_defaults: cannot create {$outDir}");
            return null;
        }
        $outPath = "{$outDir}/{$basename}";

        $out = fopen($outPath, 'w');
        if ($out === false) {
            fclose($in);
            $this->log("    candidate_defaults: cannot write {$outPath}");
            return null;
        }
        fputcsv($out, $headers, $delimiter);

        $filled         = array_fill_keys(array_keys($fillIndex), 0);
        $rows           = 0;
        $sexNormalized  = 0;
        $sexUnmapped    = [];   // raw value => occurrences
        while (($row = fgetcsv($in, 0, $delimiter)) !== false) {
            $rows++;
            $row = array_pad($row, $origCount, '');
            foreach ($fillIndex as $column => $idx) {
                if (trim((string)$row[$idx]) === '') {
                    $row[$idx] = $apply[$column];
                    $filled[$column]++;
                }
            }

            // Rewrite sex into the vocabulary LORIS accepts. A blank is
            // left blank (candidate_defaults.sex is NOT applied here -
            // stamping a sex we do not know would fabricate data). An
            // unrecognised value is also left untouched and reported:
            // it surfaces as a LORIS rejection to investigate rather
            // than being silently coerced into a wrong answer.
            if ($sexIdx !== null && isset($row[$sexIdx])) {
                $raw = trim((string)$row[$sexIdx]);
                $key = strtolower($raw);
                if ($raw !== '') {
                    if (isset($sexMap[$key])) {
                        if ($sexMap[$key] !== $raw) {
                            $row[$sexIdx] = $sexMap[$key];
                            $sexNormalized++;
                        }
                    } else {
                        $sexUnmapped[$raw] = ($sexUnmapped[$raw] ?? 0) + 1;
                    }
                }
            }

            foreach ($appended as $column) {
                $row[] = $apply[$column];
            }
            fputcsv($out, $row, $delimiter);
        }
        fclose($in);
        fclose($out);

        // Unrecognised sex values are reported whether or not a copy is
        // kept - they will be rejected by LORIS either way, and the
        // operator needs to know which value to add to sex_mappings.
        foreach ($sexUnmapped as $value => $n) {
            $this->log("    WARNING: unrecognised sex value '{$value}' in {$n} row(s) of "
                . "{$basename} - left as-is and LORIS will reject it. Add it to "
                . "project.json -> sex_mappings, or correct the source export.");
        }

        // Nothing was missing and no sex value changed - the copy matches
        // the original, so discard it and ingest the original. Only files
        // that actually needed a change reach processed/clinical/.
        if ($appended === [] && array_sum($filled) === 0 && $sexNormalized === 0) {
            @unlink($outPath);
            $this->log("    candidate_defaults: {$basename} already carries every target column - using file as-is");
            return $srcPath;
        }

        $parts = [];
        foreach ($appended as $column) {
            $parts[] = "{$column}={$apply[$column]} (column added)";
        }
        foreach ($filled as $column => $n) {
            if ($n > 0) {
                $parts[] = "{$column}={$apply[$column]} ({$n}/{$rows} blank cell(s) filled)";
            }
        }
        if ($sexNormalized > 0) {
            $parts[] = "sex normalised to Male/Female/Other ({$sexNormalized}/{$rows} value(s) rewritten)";
        }
        $this->log("    candidate_defaults applied from project.json: " . implode(', ', $parts));
        $this->log("    Processed copy: processed/clinical/{$basename}");

        return $outPath;
    }


    // ══════════════════════════════════════════════════════════════════
    //  Row-level change detection
    // ══════════════════════════════════════════════════════════════════

    /**
     * Locate the columns that identify a row, from the file's header.
     *
     * Returns null when no identifier column can be found — the file then
     * falls back to whole-file re-ingestion, exactly as before. Row
     * tracking is a narrowing optimisation, never a gate: if we cannot
     * identify rows with confidence we send the whole file and let LORIS
     * dedupe, rather than risk withholding a row.
     *
     * @return array{id:int, visit:?int, quals:array<int>, headers:array}|null
     */
    private function resolveRowKeyColumns(array $project, array $headers, string $basename): ?array
    {
        $cfg = $project['row_tracking'] ?? [];

        if (($cfg['enabled'] ?? true) === false) {
            $this->log("    Row tracking: disabled in project.json for this project");
            return null;
        }

        $lower = [];
        foreach ($headers as $i => $h) {
            $lower[strtolower(trim((string)$h))] = $i;
        }

        $find = function (array $candidates) use ($lower): ?int {
            foreach ($candidates as $name) {
                $key = strtolower(trim($name));
                if (isset($lower[$key])) {
                    return $lower[$key];
                }
            }
            return null;
        };

        // candidate_defaults appends columns to the PROCESSED copy that
        // the original may not carry. Row keys are read from the
        // ORIGINAL, so a stamped column is invisible here. Today that is
        // harmless: only Project/Cohort/Site are stamped, and none of
        // them is a key column. But CLINICAL_DEFAULT_COLUMNS is
        // documented as extensible to visit_label / redcap_event_name —
        // and the moment a key column is stamped rather than supplied,
        // keying on the original would silently lose the visit, collapse
        // a candidate's rows onto one key, and leave them separated only
        // by positional occurrence suffixes that reordering would break.
        // Detect that and fall back to whole-file upload instead.
        $stamped = [];
        foreach (self::CLINICAL_DEFAULT_COLUMNS as $key => $column) {
            if (isset($project['candidate_defaults'][$key])
                && $project['candidate_defaults'][$key] !== ''
            ) {
                $stamped[] = strtolower($column);
            }
        }
        $keyColumns = array_map('strtolower', array_merge(
            $cfg['identifier_columns'] ?? self::ROW_ID_COLUMNS,
            $cfg['visit_columns']      ?? self::ROW_VISIT_COLUMNS,
            self::ROW_QUALIFIER_COLUMNS
        ));
        foreach ($stamped as $col) {
            if (in_array($col, $keyColumns, true) && !isset($lower[$col])) {
                $this->log("    Row tracking: '{$col}' is supplied by "
                    . "candidate_defaults and absent from {$basename} - row keys "
                    . "cannot be derived from the original, falling back to "
                    . "whole-file upload");
                return null;
            }
        }

        $idIdx = $find($cfg['identifier_columns'] ?? self::ROW_ID_COLUMNS);
        if ($idIdx === null) {
            $this->log("    Row tracking: no identifier column in {$basename} "
                . "(looked for: " . implode(', ', $cfg['identifier_columns'] ?? self::ROW_ID_COLUMNS)
                . ") - falling back to whole-file upload");
            return null;
        }

        $visitIdx = $find($cfg['visit_columns'] ?? self::ROW_VISIT_COLUMNS);

        $quals = [];
        foreach (self::ROW_QUALIFIER_COLUMNS as $name) {
            $i = $lower[strtolower($name)] ?? null;
            if ($i !== null) {
                $quals[] = $i;
            }
        }

        $this->log(sprintf(
            "    Row tracking: id=%s%s%s",
            (string)$headers[$idIdx],
            $visitIdx !== null ? ", visit=" . (string)$headers[$visitIdx] : ", visit=(none)",
            $quals ? ", qualifiers=" . implode('+', array_map(fn($i) => (string)$headers[$i], $quals)) : ""
        ));

        return ['id' => $idIdx, 'visit' => $visitIdx, 'quals' => $quals, 'headers' => $headers];
    }

    /**
     * Read the ORIGINAL file and compute, per data row, a stable key and
     * a content hash.
     *
     * Both are taken from the file as delivered — before
     * applyCandidateDefaults() stamps Project/Cohort/Site and before
     * normalizeDatesInFile() rewrites dates — so pipeline enrichment can
     * never make a row look changed.
     *
     * A key that repeats within one file (same candidate, visit and
     * qualifiers) gets an occurrence suffix so the rows stay distinct
     * rather than collapsing onto one entry.
     *
     * @return array{keys:array<int,string>, hashes:array<string,string>, labels:array<string,string>}
     */
    private function computeRowHashes(string $path, string $format, array $cols): array
    {
        $delimiter = $this->delimiterForFormat($format);

        $keys   = [];   // row index (0-based, data rows only) => key
        $hashes = [];   // key => md5 of the row
        $labels = [];   // key => human-readable "PSCID / visit"
        $seen   = [];   // key => occurrences so far

        $fh = @fopen($path, 'r');
        if ($fh === false) {
            return ['keys' => [], 'hashes' => [], 'labels' => []];
        }
        fgetcsv($fh, 0, $delimiter);   // discard header

        $i = 0;
        while (($row = fgetcsv($fh, 0, $delimiter)) !== false) {
            $cell = fn(?int $idx) => ($idx !== null && isset($row[$idx]))
                ? trim((string)$row[$idx]) : '';

            $parts = [$cell($cols['id'])];
            if ($cols['visit'] !== null) {
                $parts[] = $cell($cols['visit']);
            }
            foreach ($cols['quals'] as $q) {
                $parts[] = $cell($q);
            }
            $label = implode(' / ', array_filter($parts, fn($p) => $p !== ''));
            $base  = implode('|', $parts);

            $seen[$base] = ($seen[$base] ?? 0) + 1;
            $key = md5($seen[$base] > 1 ? "{$base}#{$seen[$base]}" : $base);

            $keys[$i]     = $key;
            $hashes[$key] = md5(implode(self::ROW_HASH_SEPARATOR, array_map(
                fn($v) => trim((string)$v),
                $row
            )));
            $labels[$key] = $label !== '' ? $label : "(row " . ($i + 1) . ")";
            $i++;
        }
        fclose($fh);

        return ['keys' => $keys, 'hashes' => $hashes, 'labels' => $labels];
    }

    /**
     * Decide which rows of a changed file actually need uploading.
     *
     * Returns null to mean "send the whole file" — first upload, --force,
     * no usable identifier column, or no stored row map yet. Otherwise
     * returns the row indices to keep plus the current hash map for
     * tracking.
     *
     * @return array{all:bool, skip:bool, keep:array<int>, current:array<string,string>,
     *               new:int, changed:int, unchanged:int, removed:int}|null
     */
    private function planRowUpload(
        array $project,
        string $filename,
        string $filePath,
        string $format
    ): ?array {
        $headers = $this->readHeaderRow($filePath, $this->delimiterForFormat($format));
        if ($headers === null) {
            return null;
        }

        $cols = $this->resolveRowKeyColumns($project, $headers, $filename);
        if ($cols === null) {
            return null;
        }

        $now    = $this->computeRowHashes($filePath, $format, $cols);
        $stored = $this->trackingData[$filename]['rows'] ?? null;

        if (!is_array($stored) || $stored === []) {
            // No row map yet (first upload, or tracking written by an
            // older build). Send everything, and record the map so the
            // NEXT run can narrow.
            $this->log("    Row tracking: no stored row map - uploading all "
                . count($now['keys']) . " row(s), map recorded for next run");
            return [
                'all' => true, 'skip' => false, 'keep' => array_keys($now['keys']),
                'current' => $now['hashes'], 'new' => count($now['keys']),
                'changed' => 0, 'unchanged' => 0, 'removed' => 0,
            ];
        }

        $keep = $newRows = $changedRows = [];
        $unchanged = 0;
        foreach ($now['keys'] as $idx => $key) {
            if (!isset($stored[$key])) {
                $keep[]    = $idx;
                $newRows[] = $now['labels'][$key];
            } elseif ($stored[$key] !== $now['hashes'][$key]) {
                $keep[]        = $idx;
                $changedRows[] = $now['labels'][$key];
            } else {
                $unchanged++;
            }
        }

        $removed = count(array_diff_key($stored, $now['hashes']));

        $summary = sprintf(
            "    Row tracking: %d new, %d changed, %d unchanged%s",
            count($newRows), count($changedRows), $unchanged,
            $removed > 0 ? ", {$removed} no longer in file" : ""
        );
        $this->log($summary);

        foreach (array_slice($newRows, 0, 10) as $l) {
            $this->log("      + {$l}");
        }
        if (count($newRows) > 10) {
            $this->log("      + ... and " . (count($newRows) - 10) . " more new");
        }
        foreach (array_slice($changedRows, 0, 10) as $l) {
            $this->log("      ~ {$l}");
        }
        if (count($changedRows) > 10) {
            $this->log("      ~ ... and " . (count($changedRows) - 10) . " more changed");
        }

        return [
            'all'       => count($keep) === count($now['keys']),
            'skip'      => $keep === [],
            'keep'      => $keep,
            'current'   => $now['hashes'],
            'new'       => count($newRows),
            'changed'   => count($changedRows),
            'unchanged' => $unchanged,
            'removed'   => $removed,
        ];
    }

    /**
     * Write header + the given data-row indices to a temp file.
     *
     * Indices are positions in the ORIGINAL file. applyCandidateDefaults()
     * and normalizeDatesInFile() both rewrite row-for-row in order, so
     * position N here is still the same record after enrichment — that
     * invariant is what lets the row plan be computed from the original
     * and applied to the enriched copy.
     *
     * Returns the temp path, or null on any failure (caller then uploads
     * the unfiltered file rather than dropping rows).
     */
    private function filterRowsByIndex(string $path, array $keepIndices, string $format): ?string
    {
        $delimiter = $this->delimiterForFormat($format);
        $keep      = array_flip($keepIndices);

        $in = @fopen($path, 'r');
        if ($in === false) {
            return null;
        }
        $headers = fgetcsv($in, 0, $delimiter);
        if (!is_array($headers)) {
            fclose($in);
            return null;
        }

        $tmp = tempnam(sys_get_temp_dir(), 'clinical_rows_') . '_' . basename($path);
        $out = @fopen($tmp, 'w');
        if ($out === false) {
            fclose($in);
            return null;
        }
        fputcsv($out, $headers, $delimiter);

        $i = $written = 0;
        while (($row = fgetcsv($in, 0, $delimiter)) !== false) {
            if (isset($keep[$i])) {
                fputcsv($out, $row, $delimiter);
                $written++;
            }
            $i++;
        }
        fclose($in);
        fclose($out);

        if ($written !== count($keepIndices)) {
            @unlink($tmp);
            return null;   // row count drifted - do not risk a partial upload
        }
        return $tmp;
    }

    /**
     * Instruments named by the file's REDCap <form>_complete columns.
     *
     * Returns BOTH sets, because "no instruments" has two very different
     * causes and the caller must distinguish them:
     *
     *   'named'     every instrument the file declares via a _complete
     *               column, excluding DEFAULT_EXCLUDE_FORMS. Non-empty
     *               means the file IS a REDCap export and we already
     *               know what it targets.
     *   'available' the subset LORIS will accept — i.e. present in the
     *               project's instrument list.
     *
     * Previously only 'available' was returned, so a file naming eight
     * instruments that are installed but absent from the project's test
     * battery was indistinguishable from a file with no _complete
     * columns at all. The caller then logged "No *_complete column(s)"
     * and ran a field-match that could not possibly succeed.
     *
     * @return array{named: array<string>, available: array<string>}
     */
    private function detectInstrumentsFromHeaders(string $filePath, string $format): array
    {
        $headers = $this->readHeaderRow($filePath, $this->delimiterForFormat($format));
        if ($headers === null) {
            return ['named' => [], 'available' => []];
        }

        $named = $available = [];

        foreach ($headers as $col) {
            if (!preg_match('/^(.+)_complete$/', trim($col), $m)) {
                continue;
            }
            $inst = $m[1];
            if (in_array($inst, self::DEFAULT_EXCLUDE_FORMS, true)) {
                continue;
            }
            $named[] = $inst;
            if ($this->client->instrumentExists($inst, $this->lorisProjectName)) {
                $available[] = $inst;
            }
        }

        return [
            'named'     => array_values(array_unique($named)),
            'available' => array_values(array_unique($available)),
        ];
    }

    /**
     * Fallback instrument detection for files with NO *_complete columns.
     *
     * detectInstrumentsFromHeaders() relies on REDCap's per-form
     * <form>_complete column. That column is a convenient marker, NOT a
     * LORIS requirement: RedcapCSVParser::getEssentialHeaders() lists
     * only study_id / dob / sex / project / site / redcap_event_name /
     * cohort as structural, and the instrument name is passed to the
     * upload endpoint explicitly rather than inferred. So a file with no
     * _complete columns is perfectly ingestible — the pipeline just has
     * to work out which instrument(s) it targets.
     *
     * Each installed instrument's expected header line is fetched from
     * LORIS (getInstrumentDataHeaders returns RedcapCSVParser::
     * getCSVHeaders() output) and its DATA fields — structural columns,
     * LINST metadata and _complete removed — are compared against the
     * file's columns. An instrument matches when the file carries ANY of
     * its fields.
     *
     * "Any" rather than "all" or a threshold, because that is LORIS's
     * own rule: the pipeline uploads with strict=false, so
     * InstrumentDataParser::parseData() validates against
     * getEssentialHeaders() alone and every instrument field absent from
     * the file is simply stored as NULL. Requiring more than LORIS does
     * would reject files LORIS would happily ingest. Missing fields are
     * logged as a warning so a partially-populated instrument is
     * visible, not silent.
     *
     * Templates are cached per run — one HTTP call per instrument, not
     * per file.
     *
     * @return array<string> Matching instrument names, or [] if none.
     */
    private function detectInstrumentsFromFields(string $filePath, string $format): array
    {
        $delimiter = $this->delimiterForFormat($format);

        $fh = @fopen($filePath, 'r');
        if ($fh === false) {
            return [];
        }
        $headerLine = fgets($fh);
        fclose($fh);
        if ($headerLine === false) {
            return [];
        }

        $fileCols = array_flip(array_map(
            fn($h) => strtolower(trim((string)$h)),
            str_getcsv(trim($headerLine), $delimiter)
        ));
        $structural = array_flip(self::STRUCTURAL_COLUMNS);

        $matched = [];
        foreach ($this->client->getInstalledInstruments($this->lorisProjectName) as $inst) {
            if (in_array($inst, self::DEFAULT_EXCLUDE_FORMS, true)) {
                continue;
            }

            $dataFields = $this->instrumentDataFields($inst, $format, $delimiter, $structural);
            if ($dataFields === []) {
                continue;   // template unavailable, or no data fields
            }

            $present = array_values(array_filter(
                $dataFields,
                fn($c) => isset($fileCols[$c])
            ));
            $missing = array_values(array_filter(
                $dataFields,
                fn($c) => !isset($fileCols[$c])
            ));

            // LORIS's own rule, not a pipeline-invented threshold: the
            // upload runs with strict=false, so InstrumentDataParser
            // validates against getEssentialHeaders() only and any
            // instrument field absent from the file is stored as null.
            // A file therefore targets an instrument as soon as it
            // carries ANY of that instrument's fields; the missing ones
            // are reported, never used to veto the match.
            if ($present !== []) {
                $matched[] = $inst;
                $this->log(sprintf(
                    "      ✓ %s — %d of %d field(s) present",
                    $inst, count($present), count($dataFields)
                ));
                if ($missing !== []) {
                    $shown = array_slice($missing, 0, 10);
                    $this->log(sprintf(
                        "        WARNING: %d field(s) absent from the file, will "
                        . "be stored as NULL: %s%s",
                        count($missing),
                        implode(', ', $shown),
                        count($missing) > 10
                            ? ' ... and ' . (count($missing) - 10) . ' more'
                            : ''
                    ));
                }
            }
        }

        return $matched;
    }

    /**
     * An instrument's own data-field names, lowercased, with structural
     * and _complete columns removed. Cached per run: the template is a
     * property of the instrument, not of the file being matched.
     *
     * @param array<string,int> $structural Flipped STRUCTURAL_COLUMNS.
     * @return array<string>
     */
    private function instrumentDataFields(
        string $instrument,
        string $format,
        string $delimiter,
        array $structural
    ): array {
        if (isset($this->instrumentFieldCache[$instrument])) {
            return $this->instrumentFieldCache[$instrument];
        }

        $template = $this->client->getInstrumentDataHeaders(
            $instrument,
            'CREATE_SESSIONS',
            $format
        );
        if ($template === null || trim($template) === '') {
            return $this->instrumentFieldCache[$instrument] = [];
        }

        $fields = array_values(array_filter(
            array_map(
                fn($h) => strtolower(trim((string)$h)),
                str_getcsv(trim($template), $delimiter)
            ),
            fn($c) => $c !== ''
                && !isset($structural[$c])
                && !in_array($c, self::LINST_METADATA_COLUMNS, true)
                && !str_ends_with($c, '_complete')
        ));

        return $this->instrumentFieldCache[$instrument] = $fields;
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

                if ($this->uploadUnconfirmed($ui)) {
                    return $this->unconfirmedUploadResult($instrument, $filePath, $elapsed, $ui);
                }

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

                if ($this->uploadUnconfirmed($ui)) {
                    return $this->unconfirmedUploadResult('multi-instrument', $filePath, $elapsed, $ui);
                }

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

    /**
     * Did LORIS report any evidence that the upload ingested anything?
     *
     * A successful response normally carries "Saved X out of Y" and/or an
     * idMapping. Neither present means we cannot CONFIRM anything was
     * written — it does NOT prove nothing was. LORIS can complete real
     * work and return a bare OK, so this is deliberately named as a lack
     * of confirmation rather than a lack of ingestion, and the message
     * tells the operator to check rather than asserting an outcome.
     *
     * Treated as a failure so the run does not report success, and so
     * tracking is not written: the file is retried next run, which is
     * harmless because LORIS skips rows that already exist.
     */
    private function uploadUnconfirmed(array $ui): bool
    {
        return ($ui['rows_total'] ?? null) === null
            && ($ui['pairs'] ?? 0) === 0;
    }

    /**
     * The result recorded when LORIS accepts an upload but returns no row
     * counts and no idMapping.
     *
     * This is NOT proof that nothing was written. A re-upload of rows
     * that already exist produces exactly the same response: LORIS has
     * nothing to insert, so there is nothing to report. Treating that as
     * a failure marks a correct no-op as an error.
     *
     * Recorded as a SKIP with its own reason, so it is visible in the
     * summary and the email without failing the run. Tracking is still
     * written, so an unchanged file is not re-sent on every subsequent
     * run.
     *
     * Note on <form>_complete: it plays no part in this. The REDCap CSV
     * parser never reads those columns — getEssentialHeaders() requires
     * only study_id and redcap_event_name — so their presence or absence
     * is irrelevant to whether rows are ingested.
     */
    private function unconfirmedUploadResult(
        string $context,
        string $filePath,
        float $elapsed,
        array $ui
    ): array {
        $this->log(sprintf(
            "    UNCONFIRMED (%.2fs) - LORIS accepted the upload but returned no row"
            . " counts and no candidate/session mapping",
            $elapsed
        ));
        $this->log("    This is the expected response when every row already exists."
            . " If these rows are NEW, verify in ARCHIMEDES: check the visit label,"
            . " site, project and cohort values against those configured on the"
            . " platform, and that the instrument is in this project's test battery.");

        $this->stats['data_skipped']++;

        return [
            'status'  => 'skipped',
            'reason'  => 'ingestion not confirmed (rows may already exist)',
            'elapsed' => $elapsed,
        ];
    }

    /**
     * Validate the enriched file BEFORE anything is sent to LORIS.
     *
     * Runs after candidate_defaults stamping, sex mapping and date
     * normalisation, so it sees the values LORIS would actually receive -
     * not what the user wrote. Anything still wrong at this point is
     * something the pipeline could not fix, and LORIS will reject it.
     *
     * Reporting it here instead of after upload matters for three
     * reasons: the operator gets the row number and the offending value
     * rather than a per-row rejection with no context; nothing is written
     * to ARCHIMEDES from a file with known-bad rows; and a large file
     * fails in milliseconds rather than after minutes of server work.
     *
     * Checks only what the pipeline cannot repair. A M/D/YYYY date or an
     * "F" for sex is not reported, because both are corrected upstream.
     *
     * @return array<string> Human-readable problems, empty when clean.
     */
    private function preflightRows(string $path, string $format, array $project): array
    {
        $delimiter = $this->delimiterForFormat($format);

        $fh = @fopen($path, 'r');
        if ($fh === false) {
            return ["file could not be opened for validation: {$path}"];
        }
        $headers = fgetcsv($fh, 0, $delimiter);
        if (!is_array($headers)) {
            fclose($fh);
            return ['file has no header row'];
        }

        $idx = [];
        foreach ($headers as $i => $h) {
            $idx[strtolower(trim((string)$h))] = $i;
        }

        $dobIdx  = $idx['dob'] ?? $idx['date_of_birth'] ?? $idx['birth_date'] ?? null;
        $sexIdx  = null;
        foreach (self::SEX_COLUMN_NAMES as $c) {
            if (isset($idx[$c])) { $sexIdx = $idx[$c]; break; }
        }

        // Structural columns LORIS needs to place the candidate. Absent
        // entirely is a different failure (caught by validateColumns);
        // here we look for cells that are still blank after stamping.
        $structural = [];
        foreach (['project', 'site', 'cohort'] as $c) {
            if (isset($idx[$c])) {
                $structural[$c] = $idx[$c];
            }
        }

        $badDob = $badSex = [];
        $blank  = [];
        $line   = 1;

        while (($row = fgetcsv($fh, 0, $delimiter)) !== false) {
            $line++;
            if ($row === [null] || $row === []) {
                continue;
            }

            if ($dobIdx !== null) {
                $v = trim((string)($row[$dobIdx] ?? ''));
                // Blank is allowed here - LORIS decides whether DoB is
                // mandatory. Only a populated value in the wrong shape is
                // reported, because that one is certain to be rejected.
                if ($v !== '' && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $v)) {
                    $badDob[] = "line {$line}: '{$v}'";
                }
            }

            if ($sexIdx !== null) {
                $v = trim((string)($row[$sexIdx] ?? ''));
                if ($v !== '' && !in_array($v, ['Male', 'Female', 'Other'], true)) {
                    $badSex[] = "line {$line}: '{$v}'";
                }
            }

            foreach ($structural as $name => $i) {
                if (trim((string)($row[$i] ?? '')) === '') {
                    $blank[$name][] = "line {$line}";
                }
            }
        }
        fclose($fh);

        $problems = [];

        if ($badDob !== []) {
            $problems[] = sprintf(
                "%d row(s) have a date of birth the pipeline could not convert to "
                . "YYYY-MM-01: %s%s. Accepted at source: YYYY-MM-DD, YYYY-MM, YYYY, "
                . "M/D/YYYY, D/M/YYYY. Correct these at source, or set "
                . "date_input_format in project.json if the day/month order is "
                . "being read the wrong way round.",
                count($badDob),
                implode(', ', array_slice($badDob, 0, 10)),
                count($badDob) > 10 ? ' ... and ' . (count($badDob) - 10) . ' more' : ''
            );
        }

        if ($badSex !== []) {
            $problems[] = sprintf(
                "%d row(s) have a sex value the pipeline could not map to "
                . "Male/Female/Other: %s%s. Add the encoding to sex_mappings in "
                . "project.json, or correct it at source.",
                count($badSex),
                implode(', ', array_slice($badSex, 0, 10)),
                count($badSex) > 10 ? ' ... and ' . (count($badSex) - 10) . ' more' : ''
            );
        }

        foreach ($blank as $name => $lines) {
            $problems[] = sprintf(
                "%d row(s) have an empty '%s' after candidate_defaults were "
                . "applied: %s%s. Set candidate_defaults.%s in project.json, or "
                . "populate the column at source.",
                count($lines), $name,
                implode(', ', array_slice($lines, 0, 10)),
                count($lines) > 10 ? ' ... and ' . (count($lines) - 10) . ' more' : '',
                $name
            );
        }

        return $problems;
    }

    private function logUploadSuccess(float $elapsed, array $ui, ?int $instCount = null): void
    {
        $parts = ["SUCCESS ({$elapsed}s)"];
        if ($instCount !== null) {
            $parts[] = "{$instCount} instruments";
        }

        if ($ui['rows_saved'] !== null) {
            if ($ui['rows_saved'] === 0 && $ui['rows_existed'] > 0) {
                $parts[] = "0 new rows - {$ui['rows_existed']} already existed in ARCHIMEDES";
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

    /**
     * Persist a file's tracking entry after a successful upload.
     *
     * The stored 'hash' is always the MD5 of the ORIGINAL under
     * deidentified-raw/, never of the enriched copy.
     *
     * $rowHashes, when supplied, replaces the per-row map wholesale with
     * the rows present in the file NOW. Rows deleted upstream therefore
     * drop out of tracking; LORIS is never asked to delete anything, and
     * if such a row reappears it is treated as new and re-sent (harmless
     * — LORIS skips rows that already exist). Null leaves any existing
     * map untouched, which is what a --force or whole-file upload wants.
     *
     * An empty $uploadResult means nothing was sent (file changed, no row
     * content changed); the previous row counts are preserved rather than
     * overwritten with nulls, so the "last run" detail stays truthful.
     */
    private function updateTracking(
        string $filename,
        string $filePath,
        array $uploadResult,
        ?array $rowHashes = null
    ): void {
        $existing = $this->trackingData[$filename] ?? null;
        $now      = date('Y-m-d\TH:i:s');
        $sent     = $uploadResult !== [];

        $entry = [
            'hash'              => md5_file($filePath),
            'first_uploaded_at' => $existing['first_uploaded_at'] ?? $now,
            'last_uploaded_at'  => $sent ? $now : ($existing['last_uploaded_at'] ?? $now),
            'run_timestamp'     => $this->runTimestamp,
            'upload_count'      => ($existing['upload_count'] ?? 0) + ($sent ? 1 : 0),
            'rows_total'        => $uploadResult['rows_total']   ?? $existing['rows_total']   ?? null,
            'rows_saved'        => $uploadResult['rows_saved']   ?? $existing['rows_saved']   ?? null,
            'rows_existed'      => $uploadResult['rows_existed'] ?? $existing['rows_existed'] ?? 0,
        ];

        $rows = $rowHashes ?? ($existing['rows'] ?? null);
        if ($rows !== null) {
            $entry['rows'] = $rows;
        }

        $this->trackingData[$filename] = $entry;

        $this->saveTrackingFile();
    }

    /**
     * Keep a dated copy of the file that was just ingested.
     *
     * Called ONLY after a successful upload. A failure here does not
     * fail the file — the data is already in ARCHIMEDES — but it is
     * reported rather than swallowed: a missing dated directory with a
     * successful tracking entry is otherwise impossible to explain
     * after the fact.
     */
    private function archiveSnapshot(array $project, string $src): void
    {
        $dest = rtrim($project['data_access']['mount_path'] ?? '', '/')
            . '/processed/clinical/' . date('Y-m-d');

        if (!is_dir($dest) && !@mkdir($dest, 0755, true) && !is_dir($dest)) {
            $this->log("    WARNING: snapshot directory could not be created: {$dest}"
                . " - the ingested file was NOT archived");
            return;
        }

        $target = "{$dest}/" . basename($src);
        if (file_exists($target)) {
            $target = "{$dest}/" . time() . '_' . basename($src);
        }

        if (@copy($src, $target)) {
            $this->log("    Snapshot archived -> processed/clinical/"
                . date('Y-m-d') . "/" . basename($target));
            return;
        }

        $this->log("    WARNING: snapshot copy failed: {$src} -> {$target}"
            . " - the ingested file was NOT archived (check permissions"
            . " and free space on the mount)");
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
            [$firstUpload, $reingested, $failed, $skipped] = $this->partitionDataResults();

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
                $this->log("    Newly created in ARCHIMEDES: {$nc['new_count']}");
                if (!empty($nc['new_candids'])) {
                    $this->log("      CandIDs: " . implode(', ', $nc['new_candids']));
                }
                if ($nc['existing_count'] > 0) {
                    $existingIds = array_values(array_diff($nc['total_candids'], $nc['new_candids']));
                    $this->log("    Existing, data refreshed: {$nc['existing_count']}");
                    $this->log("      CandIDs: " . implode(', ', $existingIds));
                }
            } else {
                $this->log("    (classification unavailable - pre-run ARCHIMEDES snapshot failed)");
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
        $this->log("    Rows existed:        {$s['rows_existed']} (already in ARCHIMEDES - includes last-known from skipped files)");
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

    /**
     * Send the clinical-channel outcome email.
     *
     * Orchestration only: status, recipients, dispatch. The body is
     * assembled by buildClinicalNotificationBody() and its section
     * helpers, so this method stays readable and each section can be
     * changed without scrolling through the others.
     */
    private function sendNotification(array $project): void
    {
        $name   = $project['project_common_name'] ?? 'Unknown';
        $status = $this->notificationStatus();

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
        $body    = $this->buildClinicalNotificationBody($name, $status);

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

    /**
     * SUCCESS / SUMMARY / FAILURE for the run as a whole.
     *
     * Counts EviData skips (file did not pass the privacy check) and
     * install/ingest errors as errors. Hash-unchanged skips and
     * config-excluded files are NOT errors, so steady-state runs stay
     * SUCCESS.
     */
    private function notificationStatus(): string
    {
        $s = $this->stats;

        $successCount = ($s['evidata_files_passed'] ?? 0) + ($s['data_uploaded'] ?? 0);
        $errorCount   = ($s['evidata_files_failed'] ?? 0)
            + ($s['data_failed'] ?? 0)
            + ($s['dd_failed'] ?? 0);

        if ($errorCount === 0) {
            return 'SUCCESS';
        }
        if ($successCount === 0) {
            return 'FAILURE';
        }
        return 'SUMMARY';
    }

    /** Assemble the notification body from its sections. */
    private function buildClinicalNotificationBody(string $name, string $status): string
    {
        $body  = "Project: {$name}\n";
        $body .= "Modality: clinical\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n";
        $body .= "Run: {$this->runTimestamp}\n";
        if ($this->force) {
            $body .= "Mode: FORCE (hash check bypassed)\n";
        }
        $body .= "\n";

        $body .= $this->notificationEvidataSection();
        $body .= $this->notificationInstallSection();
        $body .= $this->notificationDataSection();
        $body .= $this->notificationTotalsSection();
        $body .= $this->notificationCandidateSection();
        $body .= $this->notificationOutcomeLine($status);

        $body .= "\n";
        if ($this->runLogPath) {
            $body .= "Run log: {$this->runLogPath}\n";
        }
        if ($this->errorLogPath) {
            $body .= "Error log: {$this->errorLogPath}\n";
        }

        return $body;
    }

    private function notificationEvidataSection(): string
    {
        $s = $this->stats;
        if (($s['evidata_files_checked'] ?? 0) === 0) {
            return '';
        }

        $body  = "EviData Pre-flight:\n";
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
        return $body . "\n";
    }

    private function notificationInstallSection(): string
    {
        $body = "Instrument Installation:\n";

        $byStatus = ['installed' => [], 'exists' => [], 'failed' => [], 'dry_run' => []];
        foreach ($this->installResults as $file => $r) {
            $byStatus[$r['status']][] = $file;
        }

        if (count($this->installResults) === 0) {
            return $body . "  (no DD files found)\n\n";
        }

        if (!empty($byStatus['installed'])) {
            $count = count($byStatus['installed']);
            $body .= "  ✔ Installed: {$count} (" . implode(', ', $byStatus['installed']) . ")\n";
        }
        if (!empty($byStatus['exists'])) {
            $count = count($byStatus['exists']);
            $body .= "  ● Already existed: {$count} (" . implode(', ', $byStatus['exists']) . ")\n";
        }
        if (!empty($byStatus['failed'])) {
            $count = count($byStatus['failed']);
            $body .= "  ✗ Failed: {$count} ("
                . $this->groupByReason(
                    $byStatus['failed'], $this->installResults, 'error', 'unknown'
                ) . ")\n";
        }

        return $body . "\n";
    }

    private function notificationDataSection(): string
    {
        $body = "Data Ingestion:\n";

        [$firstUpload, $reingested, $failed, $skipped] = $this->partitionDataResults();

        if (count($this->dataResults) === 0) {
            return $body . "  (no data files found)\n\n";
        }

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
            $count = count($failed);
            $body .= "  ✗ Failed: {$count} ("
                . $this->groupByReason($failed, $this->dataResults, 'reason', 'error')
                . ")\n";
        }
        if (!empty($skipped)) {
            $count = count($skipped);
            $body .= "  ⚠ Skipped: {$count} ("
                . $this->groupByReason($skipped, $this->dataResults, 'reason', 'unknown')
                . ")\n";
        }

        return $body . "\n";
    }

    private function notificationTotalsSection(): string
    {
        $s = $this->stats;

        $body  = str_repeat('-', 50) . "\n";
        $body .= "Totals:\n";
        $body .= "  DD files: {$s['dd_files_found']} found, {$s['dd_installed']} installed, "
            . "{$s['dd_already_existed']} existed, {$s['dd_failed']} failed\n";
        $body .= "  Data files: {$s['data_files_found']} found, {$s['data_uploaded']} processed, "
            . "{$s['data_failed']} failed, {$s['data_skipped']} skipped\n";

        if ($s['rows_existed'] > 0) {
            $body .= "  Rows existed (skipped ARCHIMEDES): {$s['rows_existed']}\n";
        }
        if ($s['pairs_processed'] > 0) {
            $body .= "  Candidate-session pairs touched: {$s['pairs_processed']}\n";
        }

        return $body . "\n";
    }

    private function notificationCandidateSection(): string
    {
        $nc = $this->computeNewCandidates();
        if (empty($nc['total_candids'])) {
            return '';
        }

        $body = "Candidates:\n";
        if ($nc['available']) {
            $body .= "  New candidates created:        {$nc['new_count']}\n";
            if ($nc['existing_count'] > 0) {
                $body .= "  Existing candidates refreshed: {$nc['existing_count']}\n";
            }
        } else {
            $body .= "  Total candidates touched: " . count($nc['total_candids'])
                . " (new/existing split unavailable)\n";
        }
        return $body . "\n";
    }

    private function notificationOutcomeLine(string $status): string
    {
        $s = $this->stats;

        if ($status === 'SUMMARY') {
            return "⚠ Partial ingestion. Files that passed EviData were ingested; "
                . "files that failed or errored were skipped (not ingested) and "
                . "will be retried next run.\n"
                . "See the EviData section above for the skipped files, and the "
                . "attached/linked report(s) for why they failed.\n";
        }
        if ($status === 'FAILURE'
            && ($s['evidata_files_checked'] ?? 0) > 0
            && ($s['evidata_files_passed'] ?? 0) === 0
        ) {
            return "✗ No files passed the EviData privacy check — nothing was ingested.\n"
                . "See the attached/linked report(s) for details.\n";
        }
        if ($status === 'FAILURE') {
            return "⚠ Some instruments failed to install or ingest.\n"
                . "Check logs for details.\n";
        }
        if ($s['data_uploaded'] > 0) {
            return "✔ Ingestion completed successfully.\n";
        }
        if ($s['data_skipped'] > 0 && $s['data_uploaded'] === 0) {
            return "✔ Ingestion completed. All files skipped - no content changes detected (hash match).\n";
        }
        return "✔ Ingestion completed. No data files to process.\n";
    }

    /**
     * Split dataResults into the four buckets both the run summary and
     * the notification email report on, so the two can never disagree
     * about which file went where.
     *
     * @return array{0:array<string>,1:array<string>,2:array<string>,3:array<string>}
     */
    private function partitionDataResults(): array
    {
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

        return [$firstUpload, $reingested, $failed, $skipped];
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

    /**
     * @param string $outPath Where the normalised copy is written. Pass
     *        the processed/clinical/ path so that what LORIS receives is
     *        the file an operator can open and inspect, rather than a
     *        temp file deleted before anyone can look at it. Dates are
     *        the field most often wrong at source, so the processed copy
     *        showing the corrected value is what makes a rejection
     *        traceable afterwards.
     */
    private function normalizeDatesInFile(
        string $srcPath,
        string $format,
        string $dateOrder = 'mdy',
        ?string $outPath = null
    ): string {
        $delimiter = $this->delimiterForFormat($format);

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

        // Write beside the enriched copy when a destination is given, so
        // processed/clinical/ holds exactly what was uploaded. Fall back
        // to a temp file only when no destination is available.
        //
        // CRITICAL: never open $srcPath for writing. applyCandidateDefaults()
        // usually returns the processed copy, which is the very path passed
        // in as $outPath - opening it with 'w' would truncate the file this
        // loop is still reading. Write to a sibling and rename over the
        // destination once the handle is closed. The rename is atomic on the
        // same filesystem, so a reader never sees a half-written file.
        $finalPath = $outPath;
        $writePath = ($outPath === null)
            ? tempnam(sys_get_temp_dir(), 'clinical_dates_') . '_' . basename($srcPath)
            : $outPath . '.writing';

        $out = @fopen($writePath, 'w');
        if ($out === false) {
            fclose($in);
            $this->log("    WARNING: cannot write {$writePath} - dates left unnormalised");
            return $srcPath;
        }
        fputcsv($out, $headers, $delimiter);

        $changedPerCol = array_fill_keys(array_keys($dateCols), 0);
        $total         = 0;

        while (($row = fgetcsv($in, 0, $delimiter)) !== false) {
            $total++;
            foreach ($dateCols as $idx => $_label) {
                if (array_key_exists($idx, $row)) {
                    $orig = (string)$row[$idx];
                    $norm = $this->normalizeDateValue($orig, $dateOrder);
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

        // Both handles are closed - now it is safe to replace the source.
        $resultPath = $writePath;
        if ($finalPath !== null) {
            if (!@rename($writePath, $finalPath)) {
                @unlink($writePath);
                $this->log("    WARNING: cannot replace {$finalPath}"
                    . " - dates left unnormalised");
                return $srcPath;
            }
            $resultPath = $finalPath;
        }

        $parts = [];
        foreach ($dateCols as $idx => $label) {
            $parts[] = "{$label} {$changedPerCol[$idx]}/{$total}";
        }
        $this->log("    Date columns normalized: " . implode(', ', $parts)
            . " row(s) rewritten to YYYY-MM-01");

        return $resultPath;
    }

    /**
     * Normalise one date cell to YYYY-MM-01.
     *
     * The day is deliberately discarded (privacy: DoB is stored to
     * month precision) — so only the YEAR and MONTH have to be
     * recovered correctly from whatever the source supplied.
     *
     * Accepted inputs:
     *   YYYY-MM-DD / YYYY-MM / YYYY        unambiguous, taken as-is
     *   YYYY/MM/DD                         leading 4-digit year
     *   M/D/YYYY or D/M/YYYY               resolved as described below
     *   separators - / .                   all treated alike
     *
     * Resolving the two-number forms:
     *   - one part > 12  -> that part MUST be the day, the other the
     *                       month. Order is determined by the data, not
     *                       by configuration.
     *   - both parts <=12 -> genuinely ambiguous (5/6/2024 is May 6th or
     *                       June 5th). $order decides: 'mdy' (default,
     *                       matching REDCap's date_mdy) or 'dmy', set
     *                       per project via project.json ->
     *                       date_input_format.
     *
     * Anything unrecognised is returned UNCHANGED, so a malformed value
     * reaches LORIS and is rejected there rather than being silently
     * turned into a plausible-looking wrong date.
     *
     * @param string $order 'mdy' or 'dmy' — used only for the ambiguous
     *                      case.
     */
    private function normalizeDateValue(string $value, string $order = 'mdy'): string
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

        // Year-first with / or . separators: YYYY/M/D, YYYY.MM.DD
        if (preg_match('/^(\d{4})[\/.](\d{1,2})(?:[\/.](\d{1,2}))?$/', $value, $m)) {
            $month = (int)$m[2];
            return ($month >= 1 && $month <= 12)
                ? sprintf('%s-%02d-01', $m[1], $month)
                : $value;
        }

        // Two numbers then a 4-digit year: M/D/YYYY or D/M/YYYY,
        // separated by / - or .
        if (preg_match('/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})$/', $value, $m)) {
            $a    = (int)$m[1];
            $b    = (int)$m[2];
            $year = $m[3];

            if ($a > 12 && $b >= 1 && $b <= 12) {
                $month = $b;              // first part must be the day
            } elseif ($b > 12 && $a >= 1 && $a <= 12) {
                $month = $a;              // second part must be the day
            } elseif ($a >= 1 && $a <= 12 && $b >= 1 && $b <= 12) {
                $month = ($order === 'dmy') ? $b : $a;   // ambiguous
            } else {
                return $value;            // neither part is a valid month
            }

            return sprintf('%s-%02d-01', $year, $month);
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