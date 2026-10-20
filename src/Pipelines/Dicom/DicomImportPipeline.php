<?php
declare(strict_types=1);

/**
 * ARCHIMEDES DICOM Import Pipeline
 *
 * Scans <projectDir>/deidentified-raw/imaging/dicoms/ for study directories
 * and imports each via POST /cbigr_api/script/importdicomstudy endpoint.
 *
 * Execution model:
 *   - Imports fired as async=true → SPM forks script → returns job_id immediately
 *   - Pipeline polls GET /cbigr_api/script/job/{id} every 30s
 *   - On ERROR the full Python/Perl stdout+stderr is returned (not a cURL timeout)
 *
 * Flag selection:
 *   - Normal run          → --insert (script errors if already in DB → auto-retry with --update --overwrite)
 *   - --force / --retry   → --update --overwrite (known to be in DB from prior run)
 *   - --force-study=NAME  → --update --overwrite (explicit safe reinsertion)
 *
 * Tracking:
 *   Per-project tracking file is stored at
 *     <projectDir>/processed/imaging/.dicom_import_tracking.json
 *     (read from the old name .dicom_import_processed.json until the next write)
 *   so pipeline state lives with other processed outputs, not inside the raw
 *   deidentified-raw input dir. The directory is created on first run.
 *
 * @package LORIS\Pipelines
 */

namespace LORIS\Pipelines\Dicom;

use LORIS\Pipelines\Shared\FingerprintTracker;
use LORIS\Pipelines\Shared\StudyFingerprint;
use LORIS\Pipelines\Shared\TrackingFile;

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

    /** @var array<string, array{uid: ?string, since: string}> Imported, removed from delivery. */
    private array $stillInLoris = [];

    /** @var array<string, string> Old folder name => new name, same study (relinked). */
    private array $renamed = [];
    private bool $dryRun;
    private bool $verbose;
    private string $token = '';

    /**
     * Set when the run aborts before examining any study — failed auth, fatal
     * exception. Distinct from studies_failed, which counts only studies the
     * run actually reached: an abort leaves that at 0, so without this a run
     * that never started reports SUCCESS.
     */
    private ?string $abortReason = null;

    private string $runTimestamp;

    /** @var resource|null */
    private $errorFh = null;
    private ?string $errorLogPath = null;

    /** @var resource|null */
    private $runLogFh = null;
    private ?string $runLogPath = null;

    private ?string $logDir = null;

    private const TRACK_FILE   = '.dicom_import_tracking.json';
    private const LEGACY_TRACK_FILE = '.dicom_import_processed.json';

    /** Shared tracking-file handler, built on first use. */
    private ?FingerprintTracker $fingerprintTracker = null;

    /** Path the cached tracker was built for. */
    private ?string $fingerprintTrackerPath = null;
    private const TRACK_SUBDIR = 'processed/imaging';

    /** Seconds between SPM job status poll requests */
    private const POLL_INTERVAL_SECONDS = 30;

    /** Maximum seconds to wait for a single study before declaring timeout */
    private const POLL_TIMEOUT_SECONDS = 14400; // 4 hours

    /**
     * Python script error message indicating the study is already in the DB.
     * Matched against computeExitText() output from ScriptServerProcess.
     */
    // "since it is already inserted in LORIS" - but NOT the opposite message,
    // "since it is not already inserted in LORIS", which the old pattern also
    // matched (so --update on a study not yet in LORIS retried --update forever).
    private const ALREADY_INSERTED_PATTERN = '/(?<!not )already inserted in LORIS/i';

    /** --update on a study LORIS does not have yet (e.g. --force on a new study). */
    private const NOT_INSERTED_PATTERN = '/not already inserted in LORIS/i';

    private array $importResults = [];

    private array $stats = [
        'studies_found'         => 0,
        'studies_processed'     => 0,
        'studies_skipped'       => 0,
        'studies_already_exist' => 0,
        'studies_failed'        => 0,
        'studies_no_session'    => 0,
    ];

    /** @var array<string, ?bool> Session exists in LORIS, per "CandID/Visit" (this run). */
    private array $sessionInLoris = [];

    // ──────────────────────────────────────────────────────────────────
    // Constructor
    // ──────────────────────────────────────────────────────────────────

    /** @var bool Add --session to linked studies only (see _sessionFlags). */
    private bool $sessionForLinked = false;

    public function setSessionForLinked(bool $v): void
    {
        $this->sessionForLinked = $v;
    }

    /** @var array<string, true> StudyInstanceUIDs the operator left out. */
    private array $excludedUids = [];

    /** @var string[] What was excluded, by folder, for the email. */
    private array $excludedLabels = [];

    public function setExcluded(array $studyUids, array $labels = []): void
    {
        $this->excludedUids   = array_flip(array_map('strval', $studyUids));
        $this->excludedLabels = $labels;
    }

    /** @var string Collection name, for the command printed in reports. */
    private string $collection = '<collection>';

    public function setCollection(string $c): void
    {
        $this->collection = $c;
    }

    public function __construct(array $config, bool $dryRun = false, bool $verbose = false)
    {
        $this->config       = $config;
        $this->dryRun       = $dryRun;
        $this->verbose      = $verbose;
        $this->runTimestamp = date('Y-m-d_H-i-s');

        $logLevel = $verbose ? Logger::DEBUG : Logger::INFO;
        $this->logger = new Logger('dicom_import');
        $formatter    = new CleanLogFormatter();

        $console = new StreamHandler('php://stdout', $logLevel);
        $console->setFormatter($formatter);
        $this->logger->pushHandler($console);

        $this->http = new HttpClient([
            'base_uri'    => rtrim($config['api']['base_url'] ?? '', '/') . '/',
            'timeout'     => 30,        // handshake only — scripts run async via SPM
            'verify'      => $config['verify_ssl'] ?? true,
            'http_errors' => false,
        ]);

        $this->notification = new Notification();
    }

    // ──────────────────────────────────────────────────────────────────
    // Main entry
    // ──────────────────────────────────────────────────────────────────

    /**
     * @param string   $projectDir    Full path to project directory
     * @param bool     $force         Reprocess ALL studies (use --update --overwrite)
     * @param bool     $retryFailed   Auto-retry previously failed studies (default true)
     * @param array    $flags         Base script flags (insert/update/overwrite/session/verbose)
     * @param string   $profile       Python config profile name
     * @param string[] $forceStudies  Specific study names to force-reinsert with
     *                                --update --overwrite regardless of tracking status
     */
    public function run(
        string $projectDir,
        bool   $force        = false,
        bool   $retryFailed  = true,
        array  $flags        = ['insert', 'verbose'],
        string $profile      = 'database_config.py',
        array  $forceStudies = [],
        string $sourceSubdir = 'deidentified-raw/imaging/dicoms'
    ): array {
        $this->importResults = [];
        $this->abortReason   = null;
        $this->forceRun      = $force;
        $this->stats = [
            'studies_found'         => 0,
            'studies_processed'     => 0,
            'studies_skipped'       => 0,
            'studies_already_exist' => 0,
            'studies_failed'        => 0,
            'studies_running'       => 0,
            'studies_no_session'    => 0,
        ];

        $this->logDir = rtrim($projectDir, '/') . '/logs/dicom';
        $this->_openRunLog();

        // Ensure tracking directory exists up front so it's visible and any
        // permission issues surface before we start hitting the API.
        $this->_ensureTrackingDir($projectDir);

        $this->_log("=== DICOM IMPORT PIPELINE ===");
        $this->_log("Project: {$projectDir}");
        $this->_log("Run: {$this->runTimestamp}");
        if ($this->dryRun) {
            $this->_log("MODE: DRY RUN");
        }
        $this->_log("Flags: " . implode(', ', $flags));
        $this->_log("Profile: {$profile}");
        if ($force) {
            $this->_log("Mode: FORCE — all studies reprocessed with --update --overwrite");
        } elseif ($retryFailed) {
            $this->_log("Mode: RETRY FAILED — auto-retrying previously failed studies");
        } else {
            $this->_log("Mode: SKIP FAILED — skipping previously failed studies");
        }
        if (!empty($forceStudies)) {
            $this->_log("Force-study targets: " . implode(', ', $forceStudies));
        }
        $this->_log("========================================");

        // One import per project at a time: two concurrent runs would both see
        // a study as new and race to insert it.
        $lock = null;
        if (!$this->dryRun) {
            $lock = @fopen(rtrim($projectDir, '/') . '/.dicom_import.lock', 'c');
            if ($lock === false || !flock($lock, LOCK_EX | LOCK_NB)) {
                $this->abortReason = 'another DICOM import is already running for this project';
                $this->_writeError('LOCK', ucfirst($this->abortReason) . ' - skipped, nothing ingested.');
                $this->_closeAllLogs();
                return $this->_finalStats();
            }
        }

        try {
            if (!$this->dryRun) {
                if (!$this->_authenticate()) {
                    $this->abortReason = 'Authentication failed';
                    $this->_writeError("AUTH", "Authentication failed — aborting.");
                    $this->_sendNotification($projectDir);
                    $this->_closeAllLogs();
                    return $this->_finalStats();
                }
            }

            $this->_processProject(
                $projectDir, $force, $retryFailed, $flags, $profile,
                $forceStudies, $sourceSubdir
            );
            $this->_writeProjectSummary(basename($projectDir));
            $this->_sendNotification($projectDir);
            $this->_closeAllLogs();

            return $this->_finalStats();

        } catch (\Exception $e) {
            $this->abortReason = 'Fatal: ' . $e->getMessage();
            $this->_writeError("FATAL", $e->getMessage());
            $this->logger->debug($e->getTraceAsString());
            $this->_sendNotification($projectDir);
            $this->_closeAllLogs();
            return $this->_finalStats();
        }
    }

    /**
     * Stats plus the abort flag, for the caller's exit code.
     *
     * @return array
     */
    private function _finalStats(): array
    {
        $stats = $this->stats;

        $stats['aborted']      = $this->abortReason !== null;
        $stats['abort_reason'] = $this->abortReason;

        return $stats;
    }

    // ──────────────────────────────────────────────────────────────────
    // Process project
    // ──────────────────────────────────────────────────────────────────

    private function _processProject(
        string $projectDir,
        bool   $force,
        bool   $retryFailed,
        array  $flags,
        string $profile,
        array  $forceStudies,
        string $sourceSubdir = 'deidentified-raw/imaging/dicoms'
    ): void {
        $this->_log("");
        $this->_log("──── STEP 1: SCAN DICOM DIRECTORIES ────");

        $studies = $this->_findStudyDirectories($projectDir, $sourceSubdir);
        $this->stats['studies_found'] = count($studies);

        if (empty($studies)) {
            $this->_log("  No DICOM study directories found.");
            return;
        }

        $this->_log("  Found {$this->stats['studies_found']} stud"
            . ($this->stats['studies_found'] === 1 ? 'y' : 'ies'));
        $this->_log("");

        $processed = $this->_loadProcessed($projectDir);

        $this->_log("──── STEP 2: IMPORT STUDIES ────");
        $this->_log("");

        foreach ($studies as $studyPath) {
            $name       = basename($studyPath);
            $isForced   = in_array($name, $forceStudies, true);
            $prev       = $processed[$name] ?? null;
            $prevStatus     = $prev['status'] ?? null;
            $contentChanged = false;

            // Set by the skip block when it ran; reused at the success exits so
            // a changed study is never walked or hashed twice.
            $comparison = null;

            // ── A job an earlier run stopped watching ───────────────────────────
            // Ingestion runs for hours under the server process manager. If an
            // earlier run timed out or lost contact, that job may still be
            // writing this study. Never start a second import of it: ask the
            // job first, even with --force.
            if ($prevStatus === 'running' && !empty($prev['job_id']) && !$this->dryRun) {
                $jobState = $this->_jobState((int) $prev['job_id']);
                $since    = $prev['timestamp'] ?? '?';
                $ageHours = (time() - (strtotime((string) $since) ?: time())) / 3600;

                if ($jobState === 'RUNNING'
                    || ($jobState === null && $ageHours < self::LOST_JOB_HOURS)
                ) {
                    $this->_log("  [{$name}] STILL RUNNING in LORIS - job {$prev['job_id']} since {$since};"
                        . ' not started again');
                    $this->importResults[$name] = [
                        'status' => 'running',
                        'reason' => "job {$prev['job_id']} still running (since {$since})",
                        'job_id' => $prev['job_id'],
                    ];
                    $this->stats['studies_running']++;
                    continue;
                }

                if ($jobState === 'SUCCESS') {
                    $this->_log("  [{$name}] job {$prev['job_id']} finished successfully after this pipeline"
                        . ' stopped watching - recorded as imported');
                    $this->_markProcessed(
                        $projectDir, $processed, $name, 'success', '',
                        $this->_fingerprintFor($projectDir, $studyPath) + ['job_id' => $prev['job_id']]
                    );
                    $this->importResults[$name] = ['status' => 'success', 'job_id' => $prev['job_id']];
                    $this->stats['studies_processed']++;
                    $this->importResults[$name] += $this->_identity($studyPath);
                    continue;
                }

                // ERROR, or not found for LOST_JOB_HOURS: a failure, retried below.
                $this->_log("  [{$name}] earlier job {$prev['job_id']} "
                    . ($jobState === 'ERROR' ? 'ended in error' : 'cannot be found')
                    . ' - retrying');
                $prevStatus = 'failed';
                $prev['status'] = 'failed';
            }

            // ── Excluded by the operator (issue reported, good studies only) ──
            $uidHere = $this->_identity($studyPath)['uid'];
            if ($uidHere !== null && isset($this->excludedUids[$uidHere])) {
                $this->_log("  [{$name}] EXCLUDED - issue reported, not ingested this run");
                $this->importResults[$name] = ['status' => 'excluded', 'reason' => 'issue reported'];
                continue;
            }

            // ── Unlinked: never ingested (no participant, no session) ──────────
            // Current runs never copy them here; a copy left by an older version
            // is still not ingested.
            if (($this->_identity($studyPath)['link_status'] ?? '') === 'unlinked') {
                $this->_log("  [{$name}] UNLINKED - no participant, not ingested");
                $this->importResults[$name] = ['status' => 'held', 'reason' => 'unlinked'];
                continue;
            }

            // ── Skip / retry logic ─────────────────────────────────────────────
            if (!$force && !$isForced && $prev !== null) {
                if ($prevStatus === 'failed' && $retryFailed) {
                    $this->_log("  [{$name}] RETRYING — previously failed"
                        . " ({$prev['timestamp']}): "
                        . $this->_truncateForLog($prev['detail'] ?? 'unknown'));
                    // fall through to import
                } else {
                    // Name-based tracking alone answers only "has this name
                    // succeeded before", so a study re-delivered with a new
                    // series or corrected files was skipped forever. The
                    // manifest hash is stat-only and runs every time; the bytes
                    // are read only when it differs, which in steady state is
                    // never.
                    $comparison = $this->_tracker($projectDir)->check($name, $studyPath);

                    if ($comparison['reprocess']) {
                        $contentChanged = true;
                        $this->_log("  [{$name}] CHANGED — {$comparison['reason']}"
                            . " — re-importing with --update --overwrite");
                        // fall through to import
                    } else {
                        $this->_log("  [{$name}] SKIPPED — {$prevStatus}"
                            . " ({$prev['timestamp']}), {$comparison['reason']}");

                        // Refresh the stored hashes even when skipping, so a
                        // study that was renamed or re-copied does not force a
                        // full byte read on every future run.
                        $this->_tracker($projectDir)->skipped(
                            $name, $studyPath, $comparison
                        );
                        $processed = $this->_tracker($projectDir)->all();

                        $this->importResults[$name] = [
                            'status' => 'skipped',
                            'reason' => "already {$prevStatus}",
                        ];
                        if ($prevStatus === 'already_exists') {
                            $this->stats['studies_already_exist']++;
                        } else {
                            $this->stats['studies_skipped']++;
                        }
                        continue;
                    }
                }
            }

            // ── No session: warning, not ingested ───────────────────────────────
            // Like BIDS (sub-/ses-), every DICOM study goes in attached to its
            // LORIS session. A study without one - not reidentified, or its
            // session missing in LORIS - is reported and left out. It is not
            // recorded, so the next run picks it up once the session exists.
            $noSession = $this->_noSessionReason($studyPath);
            if ($noSession !== null) {
                $this->_log("  [{$name}] WARNING NO SESSION - {$noSession} - not ingested");
                $this->importResults[$name] = ['status' => 'no_session', 'reason' => $noSession]
                    + $this->_identity($studyPath);
                $this->stats['studies_no_session']++;
                continue;
            }

            $this->_log("  [{$name}]");

            if ($this->dryRun) {
                $effectiveFlags = $this->_sessionFlags(
                    $this->_resolveFlags($flags, $force, $isForced, $prevStatus, $contentChanged),
                    $studyPath
                );
                $this->_log("    DRY RUN — would POST importdicomstudy (async via SPM)");
                $this->_log("    source={$studyPath}");
                $this->_log("    flags: " . implode(', ', $effectiveFlags));
                $this->importResults[$name] = ['status' => 'dry_run'];
                $this->stats['studies_processed']++;
                continue;
            }

            // ── Resolve flags ───────────────────────────────────────────────────
            // --force / --force-study on a study with a prior tracking entry means
            // we know it reached the DB (or partially did). Use --update --overwrite
            // to safely overwrite without creating a duplicate tarchive row.
            // Normal first-run studies use --insert; if the script replies "already
            // inserted", _importOneStudy() automatically retries with --update --overwrite.
            $effectiveFlags = $this->_sessionFlags(
                $this->_resolveFlags($flags, $force, $isForced, $prevStatus, $contentChanged),
                $studyPath
            );

            // The file list and comparison are handed down when the skip block
            // already built them, so a changed study is not walked twice.
            $this->_importOneStudy(
                $name, $studyPath, $projectDir, $processed, $effectiveFlags,
                $profile, false, $comparison
            );
            $this->importResults[$name] = ($this->importResults[$name] ?? ['status' => 'failed'])
                + $this->_identity($studyPath);
        }

        $this->_reconcileWithDelivery($projectDir, $studies, $sourceSubdir);
    }

    /** @var bool --force given: the operator takes ownership of conflicts. */
    private bool $forceRun = false;

    /**
     * Did this pipeline import this study for this project before?
     *
     * True for the same folder name already tracked as imported, or the same
     * StudyInstanceUID under another tracked name (a relinked folder), or
     * --force. Only then may "already inserted" become --update --overwrite.
     */
    private function _ownsStudy(string $projectDir, string $name, string $studyPath): bool
    {
        if ($this->forceRun) {
            return true;
        }

        $uid       = $this->_identity($studyPath)['uid'];
        $processed = $this->_loadProcessed($projectDir);

        foreach ($processed as $key => $entry) {
            $imported = !empty($entry['in_loris'])
                || in_array($entry['status'] ?? null, ['success', 'already_exists'], true);

            if (!$imported) {
                continue;
            }
            if ($key === $name || ($uid !== null && ($entry['study_uid'] ?? null) === $uid)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Why a study has no session to be imported into, or null when it has one.
     *
     * Locally: the reidentifier's sidecar must say "linked" with a CandID and a
     * visit. In a real run LORIS is also asked (GET candidates/{CandID}/{Visit},
     * once per session per run): a 404 means the session is not there. Any
     * other answer (LORIS slow, 5xx) does not block - the import reports its
     * own error if the session really is missing. A dry run does not log in,
     * so it checks the sidecar only.
     */
    private function _noSessionReason(string $studyPath): ?string
    {
        $id = $this->_identity($studyPath);

        if ($id['session'] === null) {
            return $id['link_status'] === 'not_reidentified'
                ? 'not reidentified (no ' . basename($studyPath) . '.provenance.json); run run_dicom_reidentifier.php'
                : "no CandID/visit recorded (link status {$id['link_status']})";
        }

        if ($this->dryRun || $this->token === '') {
            return null;
        }

        if (!array_key_exists($id['session'], $this->sessionInLoris)) {
            [$candId, $visit] = explode('/', $id['session'], 2);
            $apiVersion = $this->config['api']['api_version']
                ?? $this->config['api_version']
                ?? 'v0.0.4-dev';
            try {
                $resp = $this->http->get(
                    "api/{$apiVersion}/candidates/{$candId}/" . rawurlencode($visit),
                    ['headers' => ['Authorization' => 'Bearer ' . $this->token], 'timeout' => 30]
                );
                $code = $resp->getStatusCode();
                $this->sessionInLoris[$id['session']] = $code === 200 ? true : ($code === 404 ? false : null);
            } catch (\Exception $e) {
                $this->sessionInLoris[$id['session']] = null;
            }
            if ($this->sessionInLoris[$id['session']] === null) {
                $this->_log("    session {$id['session']}: LORIS did not answer clearly - importing, the import reports it if missing");
            }
        }

        return $this->sessionInLoris[$id['session']] === false
            ? "session {$id['session']} not found in LORIS (create the visit in LORIS, or run run_dicom_reidentifier.php --force, which recreates it)"
            : null;
    }

    /**
     * Who a study is, from the reidentifier's <study>.provenance.json.
     *
     * @return array{uid: ?string, link_status: string, link_reason: ?string, session: ?string}
     */
    private function _identity(string $studyPath): array
    {
        $sidecar = rtrim($studyPath, '/') . '.provenance.json';
        $p = is_file($sidecar)
            ? (json_decode((string) file_get_contents($sidecar), true) ?: [])
            : [];

        $status = $p['link_status'] ?? 'not_reidentified';

        return [
            'uid'         => $p['study_instance_uid'] ?? null,
            'link_status' => $status,
            'link_reason' => $p['link_reason'] ?? null,
            'session'     => $status === 'linked' && !empty($p['cand_id'])
                ? "{$p['cand_id']}/{$p['visit_label']}"
                : null,
        ];
    }

    /**
     * Compare what has been imported with what is in the source now.
     *
     * Tracking is keyed by folder name. A name that has gone is either
     * (a) the same StudyInstanceUID under a new folder - relinked, e.g.
     *     unlinked-ARCHI0003_V01_MR -> ARCHI0003_300003_V01_MR - which is
     *     already updated in LORIS under the new name: the old key is dropped;
     * (b) a study removed from the delivery. It is STILL IN LORIS and is never
     *     deleted automatically (that cannot be undone). It is reported, and
     *     its tracking entry kept, so a re-deposit is matched, not re-imported.
     * Only entries recorded from this same source directory are compared.
     */
    private function _reconcileWithDelivery(string $projectDir, array $studies, string $sourceSubdir): void
    {
        if ($this->dryRun) {
            return;
        }

        $processed = $this->_loadProcessed($projectDir);
        $current   = [];
        $uidToName = [];

        foreach ($studies as $studyPath) {
            $name = basename($studyPath);
            $id   = $this->_identity($studyPath);
            $current[$name] = true;

            if (isset($processed[$name])) {
                $processed[$name]['source_subdir'] = $sourceSubdir;
                unset($processed[$name]['removed_from_delivery']);
                if ($id['uid'] !== null) {
                    $processed[$name]['study_uid'] = $id['uid'];
                    $uidToName[$id['uid']] = $name;
                }
            }
        }

        foreach ($processed as $name => $entry) {
            if (isset($current[$name])
                || ($entry['source_subdir'] ?? null) !== $sourceSubdir
                || !in_array($entry['status'] ?? null, ['success', 'already_exists'], true)
            ) {
                continue;
            }

            $uid = $entry['study_uid'] ?? null;

            if ($uid !== null && isset($uidToName[$uid])) {
                $this->_log("  [{$name}] now {$uidToName[$uid]} (same study) - tracking entry moved");
                $this->renamed[$name] = $uidToName[$uid];
                unset($processed[$name]);
                continue;
            }

            if (empty($entry['removed_from_delivery'])) {
                $processed[$name]['removed_from_delivery'] = date('c');
            }

            $this->stillInLoris[$name] = [
                'uid'   => $uid,
                'since' => $processed[$name]['removed_from_delivery'],
            ];
            $this->_writeError(
                'REMOVED',
                "{$name}: no longer in the delivery but STILL IN LORIS"
                . ($uid ? " (StudyInstanceUID {$uid})" : '')
                . ' - not deleted. Remove it with LORIS-MRI tools/delete_imaging_upload.pl if intended.'
            );
        }

        $this->_saveProcessed($projectDir, $processed);
    }

    // ──────────────────────────────────────────────────────────────────
    // Flag resolution
    // ──────────────────────────────────────────────────────────────────

    /**
     * Attach studies to their LORIS session according to their link status.
     *
     * Participant studies reidentified to PSCID_CandID_Visit belong to a visit
     * and are imported with --session. Unlinked studies are never imported. The status comes from
     * the <study>.provenance.json sidecar written by the reidentifier; a study
     * without one (organized but not reidentified) is imported without --session.
     *
     * @param string[] $flags     Flags resolved for this study.
     * @param string   $studyPath Study directory.
     *
     * @return string[]
     */
    private function _sessionFlags(array $flags, string $studyPath): array
    {
        if (!$this->sessionForLinked) {
            return $flags;
        }

        $sidecar    = rtrim($studyPath, '/') . '.provenance.json';
        $provenance = is_file($sidecar)
            ? (json_decode((string) file_get_contents($sidecar), true) ?: [])
            : [];
        $linked     = ($provenance['link_status'] ?? null) === 'linked';

        $flags = array_values(array_filter($flags, fn($f) => $f !== 'session'));
        if ($linked) {
            $flags[] = 'session';
        }

        $this->logger->debug(sprintf(
            '    link_status=%s -> %s',
            $provenance['link_status'] ?? 'none',
            $linked ? 'with --session' : 'without --session'
        ));

        return $flags;
    }

    /**
     * Resolve the effective flags for a single study.
     *
     * Rule:
     *   - force=true OR isForced=true AND prevStatus !== null
     *     → --update --overwrite  (known to be in DB, safe overwrite)
     *   - everything else
     *     → caller-supplied flags (normally --insert)
     *     If the script returns ALREADY_EXISTS, _importOneStudy() will
     *     automatically retry with --update --overwrite (two-pass fallback).
     *
     * @param array   $baseFlags   Flags from CLI / run() caller
     * @param bool    $force       Global --force flag
     * @param bool    $isForced    Study is in --force-study list
     * @param ?string $prevStatus  Previous tracking status or null
     *
     * @return array Resolved flags
     */
    private function _resolveFlags(
        array   $baseFlags,
        bool    $force,
        bool    $isForced,
        ?string $prevStatus,
        bool    $contentChanged = false
    ): array {
        // A study whose bytes changed is already in the DB, exactly like a
        // forced one. Without this it would go out with --insert, fail with
        // "already inserted", and take the auto-retry — which works, but costs
        // a wasted round trip per study and writes a misleading error into the
        // log and the notification email.
        $useOverwrite = $force
            || ($isForced && $prevStatus !== null)
            || $contentChanged;

        if ($useOverwrite) {
            // Strip insert, ensure update + overwrite
            $flags = array_filter($baseFlags, fn($f) => $f !== 'insert');
            if (!in_array('update', $flags, true)) {
                $flags[] = 'update';
            }
            if (!in_array('overwrite', $flags, true)) {
                $flags[] = 'overwrite';
            }
            return array_values($flags);
        }

        return $baseFlags;
    }

    /**
     * Build flags for automatic retry after ALREADY_EXISTS error.
     * Switches insert → update + overwrite.
     *
     * @param array $flags Current flags
     *
     * @return array
     */
    private function _buildRetryFlags(array $flags): array
    {
        $flags = array_filter($flags, fn($f) => $f !== 'insert');
        if (!in_array('update', $flags, true)) {
            $flags[] = 'update';
        }
        if (!in_array('overwrite', $flags, true)) {
            $flags[] = 'overwrite';
        }
        return array_values($flags);
    }

    // ──────────────────────────────────────────────────────────────────
    // Import one study — fire async, poll, auto-retry on ALREADY_EXISTS
    // ──────────────────────────────────────────────────────────────────

    private function _importOneStudy(
        string  $name,
        string  $studyPath,
        string  $projectDir,
        array  &$processed,
        array   $flags,
        string  $profile,
        bool    $isRetry = false, // true when auto-retrying after ALREADY_EXISTS
        ?array  $comparison = null
    ): void {
        // Fingerprint computed once per study and reused at every success-shaped
        // exit. Deferred until here so a launch failure costs nothing, and the
        // file list is reused from the skip block when it already walked.
        $fingerprint = null;

        try {
            $t0     = microtime(true);
            $launch = $this->_launchAsync($studyPath, $flags, $profile);

            if (($launch['http_status'] ?? 0) !== 202) {
                $elapsed = round(microtime(true) - $t0, 2);
                $errMsg  = $this->_extractErrorMessage($launch);
                $this->_log("    FAILED to launch ({$elapsed}s): {$errMsg}");
                $this->_writeError($name, "Launch failed: {$errMsg}");
                $this->_writeRequestDetail($studyPath, $flags, $launch);
                $this->_markProcessed($projectDir, $processed, $name, 'failed', $errMsg);
                $this->importResults[$name] = ['status' => 'failed', 'reason' => $errMsg, 'elapsed' => $elapsed];
                $this->stats['studies_failed']++;
                return;
            }

            $jobId = $launch['job_id'] ?? null;
            $pid   = $launch['pid']    ?? null;
            if (!$jobId) {
                $elapsed = round(microtime(true) - $t0, 2);
                $errMsg  = "No job_id returned from async launch";
                $this->_log("    FAILED ({$elapsed}s): {$errMsg}");
                $this->_writeError($name, $errMsg);
                $this->_writeRequestDetail($studyPath, $flags, $launch);
                $this->_markProcessed($projectDir, $processed, $name, 'failed', $errMsg);
                $this->importResults[$name] = ['status' => 'failed', 'reason' => $errMsg, 'elapsed' => $elapsed];
                $this->stats['studies_failed']++;
                return;
            }

            $retryLabel = $isRetry ? ' [retry with --' . implode(' --', $flags) . ']' : '';
            $this->_log("    Launched — job_id={$jobId} pid={$pid}{$retryLabel}, polling every "
                . self::POLL_INTERVAL_SECONDS . "s ...");

            // ── Poll ───────────────────────────────────────────────────────────
            $pollStart        = microtime(true);
            $pollFailures     = 0;
            $wasRunning       = false;

            while (true) {
                sleep(self::POLL_INTERVAL_SECONDS);

                $elapsed = round(microtime(true) - $t0, 2);

                if ((microtime(true) - $pollStart) > self::POLL_TIMEOUT_SECONDS) {
                    $errMsg = "Job {$jobId} timed out after "
                        . self::POLL_TIMEOUT_SECONDS . "s — "
                        . "check server_processes id={$jobId}";
                    $this->_log("    TIMEOUT ({$elapsed}s) - job may still be running in LORIS");
                    $this->_markStillRunning($projectDir, $processed, $name, $jobId, $errMsg, $elapsed);
                    return;
                }

                $status = $this->_pollJob($jobId);

                if ($status === null) {
                    $pollFailures++;
                    $this->_log("    WARNING: poll failed for job {$jobId} at {$elapsed}s"
                        . " — attempt {$pollFailures}/3");
                    if ($pollFailures >= 3) {
                        $errMsg = "Job {$jobId} poll failed 3 consecutive times"
                            . " — check GET cbigr_api/script/job/{$jobId}";
                        $this->_log("    LOST CONTACT ({$elapsed}s): {$errMsg} - job may still be running");
                        $this->_markStillRunning($projectDir, $processed, $name, $jobId, $errMsg, $elapsed);
                        return;
                    }
                    continue;
                }

                $pollFailures = 0;
                $state        = $status['state'] ?? 'UNKNOWN';
                $this->_log("    [{$elapsed}s] job {$jobId} → {$state}");

                if ($state === 'RUNNING') {
                    $wasRunning = true;
                    continue;
                }

                // UNKNOWN means SPM couldn't read the exit code file — either it was
                // deleted before the monitor synced (exit 0, deleteProcessFiles ran)
                // or the process died unexpectedly.
                // If we saw RUNNING previously, the process completed and the file was
                // cleaned up after a successful exit — treat as SUCCESS.
                if ($state === 'UNKNOWN') {
                    if ($wasRunning) {
                        $this->_log("    SUCCESS (exit code file cleaned up after successful run)"
                            . " ({$elapsed}s)");
                        $fingerprint ??= $this->_fingerprintFor(
                            $projectDir, $studyPath, $comparison
                        );
                        $this->_markProcessed(
                            $projectDir, $processed, $name, 'success', '', $fingerprint
                        );
                        $this->importResults[$name] = [
                            'status'  => 'success',
                            'elapsed' => $elapsed,
                            'job_id'  => $jobId,
                        ];
                        $this->stats['studies_processed']++;
                        return;
                    }
                    // Never saw RUNNING — process likely died immediately
                    $errMsg = "Job {$jobId} ended in UNKNOWN state without running"
                        . " — check server_processes id={$jobId}";
                    $this->_log("    FAILED ({$elapsed}s): {$errMsg}");
                    $this->_writeError($name, $errMsg);
                    $this->_writeRequestDetail($studyPath, $flags, $status + ['job_id' => $jobId]);
                    $this->_markProcessed($projectDir, $processed, $name, 'failed', $errMsg);
                    $this->importResults[$name] = [
                        'status'  => 'failed',
                        'reason'  => $errMsg,
                        'elapsed' => $elapsed,
                        'job_id'  => $jobId,
                    ];
                    $this->stats['studies_failed']++;
                    return;
                }

                // ── Job finished ───────────────────────────────────────────────
                $elapsed = round(microtime(true) - $t0, 2);

                if ($state === 'SUCCESS') {
                    $this->_log("    SUCCESS ({$elapsed}s)");
                    $fingerprint ??= $this->_fingerprintFor(
                        $projectDir, $studyPath, $comparison
                    );
                    $this->_markProcessed(
                        $projectDir, $processed, $name, 'success', '', $fingerprint
                    );
                    $this->importResults[$name] = [
                        'status'  => 'success',
                        'elapsed' => $elapsed,
                        'job_id'  => $jobId,
                    ];
                    $this->stats['studies_processed']++;
                    return;
                }

                // ── ERROR — read actual script output from SPM ─────────────────
                $errorDetail = $status['error_detail'] ?? $status['progress'] ?? 'Unknown error';
                $exitCode    = $status['exit_code'] ?? '?';

                // ALREADY_EXISTS: study is in DB but we tried --insert.
                // Auto-retry once with --update --overwrite.
                if (!$isRetry && preg_match(self::ALREADY_INSERTED_PATTERN, $errorDetail)
                    && !$this->_ownsStudy($projectDir, $name, $studyPath)
                ) {
                    // In LORIS, but never imported by this pipeline for this
                    // project: another project, a manual import, or a site that
                    // re-used a study. Updating it would overwrite that archive
                    // (and its PatientName) with this delivery. Refuse.
                    $uid = $this->_identity($studyPath)['uid'] ?? 'unknown';
                    $errorDetail = "StudyInstanceUID {$uid} is already in LORIS but was not imported "
                        . 'by this pipeline for this project - NOT overwritten. Check where it came '
                        . 'from; re-run with --force only if this project should own it.';
                    $this->_log("    REFUSED ({$elapsed}s): {$errorDetail}");
                    $this->_writeError($name, $errorDetail);
                    $this->_writeRequestDetail($studyPath, $flags, $status + ['job_id' => $jobId]);
                    $this->_markProcessed($projectDir, $processed, $name, 'failed', $errorDetail);
                    $this->importResults[$name] = [
                        'status' => 'failed', 'reason' => $errorDetail,
                        'elapsed' => $elapsed, 'job_id' => $jobId,
                    ];
                    $this->stats['studies_failed']++;
                    return;
                }

                if (!$isRetry && (preg_match(self::NOT_INSERTED_PATTERN, $errorDetail)
                    || preg_match(self::ALREADY_INSERTED_PATTERN, $errorDetail))
                ) {
                    // Keep the first attempt's script output in the run log, so a
                    // retry that then fails still shows both attempts.
                    if ($this->runLogFh) {
                        fwrite($this->runLogFh, "  FIRST-ATTEMPT job_id={$jobId} exit_code={$exitCode} "
                            . 'flags=' . implode(',', $flags) . " output:\n    "
                            . str_replace("\n", "\n    ", rtrim($errorDetail)) . "\n");
                    }
                }

                if (!$isRetry && preg_match(self::NOT_INSERTED_PATTERN, $errorDetail)) {
                    $this->_log("    Not in LORIS yet — retrying with --insert ({$elapsed}s)");
                    $insertFlags = array_values(array_unique(array_merge(
                        ['insert'],
                        array_diff($flags, ['update'])
                    )));
                    $this->_importOneStudy(
                        $name, $studyPath, $projectDir, $processed, $insertFlags,
                        $profile, true, $comparison
                    );
                    return;
                }

                if (!$isRetry && preg_match(self::ALREADY_INSERTED_PATTERN, $errorDetail)) {
                    $this->_log("    Already inserted — retrying with --update --overwrite ({$elapsed}s)");
                    $retryFlags = $this->_buildRetryFlags($flags);
                    $this->_importOneStudy(
                        $name, $studyPath, $projectDir, $processed, $retryFlags,
                        $profile, true, $comparison
                    );
                    return;
                }

                // On retry ALREADY_EXISTS means a genuine conflict — mark as already_exists
                if ($isRetry && preg_match(self::ALREADY_INSERTED_PATTERN, $errorDetail)) {
                    $this->_log("    Already inserted (confirmed) — marking done ({$elapsed}s)");
                    $fingerprint ??= $this->_fingerprintFor(
                        $projectDir, $studyPath, $comparison
                    );
                    $this->_markProcessed(
                        $projectDir, $processed, $name, 'already_exists', '', $fingerprint
                    );
                    $this->importResults[$name] = [
                        'status'  => 'already_exists',
                        'elapsed' => $elapsed,
                        'job_id'  => $jobId,
                    ];
                    $this->stats['studies_already_exist']++;
                    return;
                }

                // Real failure — the full Python/Perl error is in errorDetail
                $this->_log("    FAILED ({$elapsed}s) exit_code={$exitCode}: "
                    . $this->_truncateForLog($errorDetail));
                $this->_writeError($name, "Script failed (exit {$exitCode}): "
                    . $this->_truncateForLog($errorDetail, 300));
                $this->_writeRequestDetail($studyPath, $flags, $status + ['job_id' => $jobId]);
                $this->_markProcessed($projectDir, $processed, $name, 'failed', $errorDetail);
                $this->importResults[$name] = [
                    'status'    => 'failed',
                    'reason'    => $errorDetail,
                    'elapsed'   => $elapsed,
                    'job_id'    => $jobId,
                    'exit_code' => $exitCode,
                ];
                $this->stats['studies_failed']++;
                return;
            }

        } catch (\Exception $e) {
            $this->_log("    EXCEPTION: " . $e->getMessage());
            $this->_writeError($name, "Import exception: " . $e->getMessage());
            $this->_writeErrorDetail("source: {$studyPath}");
            $this->_writeErrorDetail("trace:\n" . $e->getTraceAsString());
            $this->importResults[$name] = ['status' => 'failed', 'reason' => $e->getMessage()];
            $this->stats['studies_failed']++;
        }
    }

    /**
     * Shared tracking-file handler.
     *
     * FingerprintTracker owns the tracking JSON and the rules around it -
     * refresh on skip, store only after success, walk each directory once.
     * The same instance serves BidsImportPipeline; only the key differs.
     *
     * @param string $projectDir Project root.
     *
     * @return FingerprintTracker
     */
    private function _tracker(string $projectDir): FingerprintTracker
    {
        $path = $this->_trackingFilePath($projectDir);

        if ($this->fingerprintTracker === null
            || $this->fingerprintTrackerPath !== $path
        ) {
            $this->fingerprintTracker     = new FingerprintTracker(
                $path,
                $this->logger,
                TrackingFile::legacy($path, self::LEGACY_TRACK_FILE)
            );
            $this->fingerprintTrackerPath = $path;
        }

        return $this->fingerprintTracker;
    }

    /**
     * Fingerprint to store for a study that reached LORIS.
     *
     * Computed at most once per study. The file list comes from the tracker's
     * cache, so the directory is walked once per run however many times this is
     * called, and a comparison from the skip block is reused rather than the
     * bytes being read a second time.
     *
     * @param string     $projectDir Project root, to reach the tracker.
     * @param string     $studyPath  Study directory.
     * @param array|null $comparison compare() result, if already computed.
     *
     * @return array manifest_hash, content_hash, hashed_at.
     */
    private function _fingerprintFor(
        string $projectDir,
        string $studyPath,
        ?array $comparison = null
    ): array {
        $tracker = $this->_tracker($projectDir);
        $files   = $tracker->filesIn($studyPath);

        $comparison ??= StudyFingerprint::compare(null, $files, $studyPath);

        return StudyFingerprint::toTracking($comparison, $files, $studyPath);
    }

    // ──────────────────────────────────────────────────────────────────
    // Auth
    // ──────────────────────────────────────────────────────────────────

    private function _authenticate(): bool
    {
        $apiVersion = $this->config['api']['api_version']
            ?? $this->config['api_version']
            ?? 'v0.0.4-dev';
        $username = $this->config['api']['username'] ?? '';
        $password = $this->config['api']['password'] ?? '';

        $this->_log("Authenticating ({$apiVersion})...");

        try {
            $resp = $this->http->post("api/{$apiVersion}/login", [
                'json' => compact('username', 'password'),
            ]);

            if ($resp->getStatusCode() !== 200) {
                $this->_log("  Auth failed: HTTP " . $resp->getStatusCode());
                return false;
            }

            $body        = json_decode($resp->getBody()->getContents(), true);
            $this->token = $body['token'] ?? '';

            if (empty($this->token)) {
                $this->_log("  Auth failed: no token returned");
                return false;
            }

            $this->_log("  Authenticated");
            return true;
        } catch (\Exception $e) {
            $this->_log("  Auth exception: " . $e->getMessage());
            return false;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // HTTP helpers
    // ──────────────────────────────────────────────────────────────────

    /**
     * POST importdicomstudy with async=true.
     * Returns immediately with { job_id, poll_url }.
     */
    private function _launchAsync(
        string $sourceDir,
        array  $flags   = ['insert'],
        string $profile = 'database_config.py'
    ): array {
        $endpoint = 'cbigr_api/script/importdicomstudy';

        $payload = [
            'args'  => ['source' => $sourceDir, 'profile' => $profile],
            'flags' => $flags,
            'async' => true,
        ];

        $this->logger->debug("  POST {$endpoint} async=true flags=" . implode(',', $flags));

        try {
            $resp = $this->http->post($endpoint, [
                'json'    => $payload,
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
                'timeout' => 30,
            ]);
            $raw  = (string) $resp->getBody();
            $body = json_decode($raw, true);
            if (!is_array($body)) {
                // Not JSON (proxy/HTML error page, PHP fatal): keep the raw text
                // so the error log shows what the server actually sent.
                $body = ['raw_body' => $raw];
            }
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

    /**
     * Full, untruncated description of a failed request/job for the error log:
     * what was sent and everything the endpoint returned.
     *
     * @param array<string,mixed> $response Launch or job-status body.
     */
    private function _writeRequestDetail(string $studyPath, array $flags, array $response): void
    {
        $this->_writeErrorDetail("source: {$studyPath}");
        $this->_writeErrorDetail('flags: ' . implode(',', $flags));
        foreach (['http_status', 'job_id', 'pid', 'state', 'exit_code', 'end_time'] as $k) {
            if (isset($response[$k]) && $response[$k] !== '') {
                $this->_writeErrorDetail("{$k}: " . (is_scalar($response[$k])
                    ? (string) $response[$k] : json_encode($response[$k])));
            }
        }
        if (!empty($response['errors'])) {
            $this->_writeErrorDetail('errors: ' . json_encode(
                $response['errors'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES
            ));
        }
        $seen = [];
        foreach (['error_detail', 'output', 'stdout', 'stderr', 'progress', 'raw_body'] as $k) {
            if (empty($response[$k]) || !is_string($response[$k])) {
                continue;
            }
            $text = rtrim($response[$k]);
            if (isset($seen[$text])) {
                continue;   // same text as an earlier field - print once
            }
            $seen[$text] = true;
            $this->_writeErrorDetail("{$k}:\n    " . str_replace("\n", "\n    ", $text));
        }
    }

    /**
     * GET /cbigr_api/script/job/{id}.
     * Returns null on transient failure — caller retries next poll cycle.
     */
    /** An unanswered job older than this is treated as lost (failed). */
    private const LOST_JOB_HOURS = 48;

    /**
     * Record a study whose job this run stopped watching (timeout, or the
     * poll endpoint stopped answering). Not "failed": the job may still be
     * writing it. The next run asks the job before doing anything.
     */
    private function _markStillRunning(
        string $projectDir,
        array &$processed,
        string $name,
        int $jobId,
        string $detail,
        float $elapsed
    ): void {
        $this->_writeError($name, "{$detail} - recorded as still running; the next run checks job {$jobId}"
            . ' before importing this study again');
        $this->_markProcessed($projectDir, $processed, $name, 'running', $detail, ['job_id' => $jobId]);
        $this->importResults[$name] = [
            'status'  => 'running',
            'reason'  => $detail,
            'elapsed' => $elapsed,
            'job_id'  => $jobId,
        ];
        $this->stats['studies_running']++;
    }

    /**
     * RUNNING | SUCCESS | ERROR from the job endpoint, NOT_FOUND for a 404,
     * or null when LORIS cannot be reached.
     */
    private function _jobState(int $jobId): ?string
    {
        try {
            $resp = $this->http->get("cbigr_api/script/job/{$jobId}", [
                'headers'     => ['Authorization' => 'Bearer ' . $this->token],
                'timeout'     => 15,
                'http_errors' => false,
            ]);
            if ($resp->getStatusCode() === 404) {
                return 'NOT_FOUND';
            }
            $body = json_decode($resp->getBody()->getContents(), true);
            return is_array($body) ? ($body['state'] ?? null) : null;
        } catch (\Exception $e) {
            return null;
        }
    }

    private function _pollJob(int $jobId): ?array
    {
        try {
            $resp = $this->http->get("cbigr_api/script/job/{$jobId}", [
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
                'timeout' => 15,
            ]);
            $raw  = (string) $resp->getBody();
            $body = json_decode($raw, true);
            if (!is_array($body)) {
                $this->_log("    poll job {$jobId}: HTTP {$resp->getStatusCode()}, not JSON: "
                    . $this->_truncateForLog($raw, 300));
                return null;
            }
            return $body;
        } catch (\Exception $e) {
            $this->_log("    poll job {$jobId}: " . $e->getMessage());
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Tracking file — stored under {projectDir}/processed/imaging/
    // ──────────────────────────────────────────────────────────────────

    /**
     * Return the absolute path to the tracking JSON for this project.
     * Does NOT create the directory by default (used for read-only checks).
     */
    private function _trackingFilePath(string $projectDir): string
    {
        return rtrim($projectDir, '/') . '/' . self::TRACK_SUBDIR . '/' . self::TRACK_FILE;
    }

    /**
     * Ensure the tracking directory exists. Idempotent and race-safe
     * (handles the case where another concurrent run created it first).
     */
    private function _ensureTrackingDir(string $projectDir): bool
    {
        $dir = rtrim($projectDir, '/') . '/' . self::TRACK_SUBDIR;
        if (is_dir($dir)) {
            return true;
        }
        // The is_dir() recheck handles the race where another process created
        // the dir between our is_dir() check above and mkdir() below.
        if (!@mkdir($dir, 0755, true) && !is_dir($dir)) {
            $this->_writeError('TRACKING', "Cannot create tracking directory: {$dir}");
            return false;
        }
        $this->_log("Tracking directory: {$dir}");
        return true;
    }

    private function _loadProcessed(string $projectDir): array
    {
        $path = $this->_trackingFilePath($projectDir);
        $path = TrackingFile::readPath($path, TrackingFile::legacy($path, self::LEGACY_TRACK_FILE));
        if (!file_exists($path)) {
            return [];
        }
        $contents = @file_get_contents($path);
        if ($contents === false) {
            $this->_writeError('TRACKING', "Cannot read: {$path}");
            return [];
        }
        $decoded = json_decode($contents, true);
        if (!is_array($decoded)) {
            $this->_writeError('TRACKING', "Malformed tracking file (not JSON object): {$path}");
            return [];
        }
        return $decoded;
    }

    private function _saveProcessed(string $projectDir, array $processed): void
    {
        // Guarantee directory exists every save (covers the case where it was
        // deleted out from under us mid-run).
        if (!$this->_ensureTrackingDir($projectDir)) {
            return;
        }
        $path = $this->_trackingFilePath($projectDir);
        $json = json_encode($processed, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        if (@file_put_contents($path, $json) === false) {
            $this->_writeError('TRACKING', "Cannot write: {$path}");
            return;
        }
        TrackingFile::retireLegacy($path, TrackingFile::legacy($path, self::LEGACY_TRACK_FILE));
    }

    private function _markProcessed(
        string  $projectDir,
        array  &$processed,
        string  $studyName,
        string  $status,
        string  $detail = '',
        array   $extra  = []
    ): void {
        // $extra carries the fingerprint, and is passed only at success-shaped
        // exits. A stored fingerprint must always mean "this reached LORIS",
        // never "we looked at it" — so failure paths deliberately omit it and
        // the study is retried in full next run.
        // Identity and "has been in LORIS" survive a later failure (hashes do
        // not - see above): they decide whether an "already inserted" may be
        // updated, and a study that once reached LORIS still belongs here.
        $previous = $processed[$studyName] ?? [];
        $sticky   = array_intersect_key($previous, array_flip(['study_uid', 'source_subdir', 'in_loris']));
        if (in_array($status, ['success', 'already_exists'], true)) {
            $sticky['in_loris'] = true;
        }

        $processed[$studyName] = array_merge(
            $sticky,
            [
                'status'    => $status,
                'detail'    => $detail,
                'timestamp' => date('c'),
            ],
            $extra
        );
        $this->_saveProcessed($projectDir, $processed);
    }

    // ──────────────────────────────────────────────────────────────────
    // Find study dirs
    // ──────────────────────────────────────────────────────────────────

    private function _findStudyDirectories(
        string $projectDir,
        string $sourceSubdir = 'deidentified-raw/imaging/dicoms'
    ): array {
        $dicomRoot = rtrim($projectDir, '/') . '/' . trim($sourceSubdir, '/');

        // With three possible sources — the raw delivery, the organized output
        // and the reidentified output — "no studies found" is ambiguous unless
        // the directory actually scanned is in the log.
        $this->_log("  Source: {$dicomRoot}");

        if (!is_dir($dicomRoot)) {
            $this->_log("  Directory not found: {$dicomRoot}");
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

    // ──────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────

    private function _truncateForLog(string $text, int $limit = 120): string
    {
        $text = preg_replace('/\s+/', ' ', trim($text));
        if (strlen($text) <= $limit) {
            return $text;
        }
        return substr($text, 0, $limit - 3) . '...';
    }

    private function _extractErrorMessage(array $result): string
    {
        $errors = $result['errors'] ?? [];
        if (!empty($errors)) {
            $msgs = array_map(fn($e) => $e['message'] ?? $e['type'] ?? 'unknown', $errors);
            return implode('; ', array_slice($msgs, 0, 3));
        }
        if (!empty($result['output'])) {
            $lines = array_filter(explode("\n", trim($result['output'])));
            return end($lines) ?: 'Unknown error';
        }
        return $result['error_type']
            ?? 'Unknown error (HTTP ' . ($result['http_status'] ?? '?') . ')';
    }

    // ══════════════════════════════════════════════════════════════════
    //  RUN LOG
    // ══════════════════════════════════════════════════════════════════

    private function _openRunLog(): void
    {
        if ($this->runLogFh !== null || $this->logDir === null) {
            return;
        }
        if (!is_dir($this->logDir)) {
            @mkdir($this->logDir, 0755, true);
        }
        $this->runLogPath = "{$this->logDir}/dicom_import_run_{$this->runTimestamp}.log";
        $this->runLogFh   = @fopen($this->runLogPath, 'a');
        if ($this->runLogFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->runLogFh,
                "{$sep}\n ARCHIMEDES DICOM Import Pipeline — Run Log\n"
                . " Started: " . date('Y-m-d H:i:s T') . "\n"
                . ($this->dryRun ? " Mode: DRY RUN\n" : "")
                . "{$sep}\n\n"
            );
        }
    }

    private function _log(string $msg): void
    {
        $this->logger->info($msg);
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[" . date('H:i:s') . "] {$msg}\n");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    //  ERROR LOG
    // ══════════════════════════════════════════════════════════════════

    private function _writeError(string $context, string $msg): void
    {
        $this->logger->error("[{$context}] {$msg}");

        if ($this->errorFh === null && $this->logDir !== null) {
            if (!is_dir($this->logDir)) {
                @mkdir($this->logDir, 0755, true);
            }
            $this->errorLogPath = "{$this->logDir}/dicom_import_errors_{$this->runTimestamp}.log";
            $this->errorFh      = @fopen($this->errorLogPath, 'a');
            if ($this->errorFh) {
                $sep = str_repeat('=', 72);
                fwrite($this->errorFh,
                    "{$sep}\n ARCHIMEDES DICOM Import Pipeline — Error Log\n"
                    . " Run: {$this->runTimestamp}\n{$sep}\n\n"
                );
            }
        }

        $ts = date('H:i:s');
        if ($this->errorFh) {
            fwrite($this->errorFh, "[{$ts}] [{$context}] {$msg}\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[{$ts}] ERROR [{$context}] {$msg}\n");
        }
    }

    private function _writeErrorDetail(string $text): void
    {
        if ($this->errorFh) {
            fwrite($this->errorFh, '  ' . str_replace("\n", "\n  ", $text) . "\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, '  ERROR-DETAIL: '
                . str_replace("\n", "\n  ERROR-DETAIL: ", $text) . "\n");
        }
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

    // ══════════════════════════════════════════════════════════════════
    //  PROJECT SUMMARY
    // ══════════════════════════════════════════════════════════════════

    private function _writeProjectSummary(string $projectName): void
    {
        $this->_log("");
        $this->_log("──── PROJECT SUMMARY: {$projectName} ────");
        $this->_log("  DICOM STUDY IMPORT:");

        if (empty($this->importResults)) {
            $this->_log("    (no studies found)");
        } else {
            $success = $failed = $skipped = $alreadyExist = $dryRun = $noSession = [];
            foreach ($this->importResults as $study => $r) {
                switch ($r['status']) {
                    case 'success':        $success[]      = $study; break;
                    case 'failed':         $failed[]       = $study; break;
                    case 'skipped':        $skipped[]      = $study; break;
                    case 'already_exists': $alreadyExist[] = $study; break;
                    case 'dry_run':        $dryRun[]       = $study; break;
                    case 'no_session':     $noSession[]    = $study; break;
                }
            }

            if (!empty($success)) {
                $this->_log("    Imported (" . count($success) . "):");
                foreach ($success as $s) {
                    $e = $this->importResults[$s]['elapsed'] ?? '';
                    $j = $this->importResults[$s]['job_id'] ?? null;
                    $this->_log("      + {$s}" . ($e ? " ({$e}s)" : "") . ($j ? " [job {$j}]" : ""));
                }
            }
            if (!empty($alreadyExist)) {
                $this->_log("    Already imported (" . count($alreadyExist) . "):");
                foreach ($alreadyExist as $s) {
                    $this->_log("      = {$s}");
                }
            }
            if (!empty($skipped)) {
                $this->_log("    Skipped (" . count($skipped) . "):");
                foreach ($skipped as $s) {
                    $this->_log("      - {$s} — " . ($this->importResults[$s]['reason'] ?? '?'));
                }
            }
            if (!empty($failed)) {
                $this->_log("    Failed (" . count($failed) . "):");
                foreach ($failed as $s) {
                    $j = $this->importResults[$s]['job_id'] ?? null;
                    $this->_log("      ! {$s} — "
                        . $this->_truncateForLog($this->importResults[$s]['reason'] ?? '?')
                        . ($j ? " [job {$j}]" : ""));
                }
            }
            if (!empty($noSession)) {
                $this->_log("    WARNING no session, NOT ingested (" . count($noSession) . "):");
                foreach ($noSession as $s) {
                    $this->_log("      ? {$s} — " . ($this->importResults[$s]['reason'] ?? '?'));
                }
            }
            if (!empty($dryRun)) {
                $this->_log("    Dry run (" . count($dryRun) . "):");
                foreach ($dryRun as $s) {
                    $this->_log("      ~ {$s}");
                }
            }
        }

        $s = $this->stats;
        $this->_log("");
        $this->_log("========================================");
        $this->_log("PIPELINE RUN SUMMARY");
        $this->_log("========================================");
        $this->_log("  Run: {$this->runTimestamp}");
        $this->_log("  Studies found:          {$s['studies_found']}");
        $this->_log("  Successfully imported:  {$s['studies_processed']}");
        $this->_log("  Already existed:        {$s['studies_already_exist']}");
        $this->_log("  Skipped:                {$s['studies_skipped']}");
        $this->_log("  Failed:                 {$s['studies_failed']}");
        if ($this->runLogPath)   $this->_log("  Run log:   {$this->runLogPath}");
        if ($this->errorLogPath) $this->_log("  Error log: {$this->errorLogPath}");
        $this->_log("  Result: " . (
            $this->abortReason !== null
                ? 'ABORTED — ' . $this->abortReason
                : ($s['studies_failed'] > 0
                    ? 'COMPLETED WITH ERRORS'
                    : 'COMPLETED SUCCESSFULLY')
        ));
        $this->_log("========================================");
    }

    // ══════════════════════════════════════════════════════════════════
    //  EMAIL NOTIFICATION
    // ══════════════════════════════════════════════════════════════════

    private function _evidataGlobal(): array
    {
        $path = __DIR__ . '/../../config/evidata_config.json';
        return is_file($path) ? (json_decode((string) file_get_contents($path), true) ?: []) : [];
    }

    private function _sendNotification(string $projectDir): void
    {
        if ($this->dryRun) {
            $this->_log("  Notification skipped (dry run)");
            return;
        }

        $projectName = basename($projectDir);
        $s           = $this->stats;
        $hasFailures = $s['studies_failed'] > 0;

        // An abort happens before any study is examined, so studies_failed is 0
        // and this would otherwise report SUCCESS to the on_success list. A run
        // that could not start is a failure, not an empty delivery.
        $hasFailures = $hasFailures || $this->abortReason !== null
            || !empty($this->stillInLoris) || !empty($this->renamed);

        $projectJson = rtrim($projectDir, '/') . '/project.json';
        $projectData = file_exists($projectJson)
            ? (json_decode(file_get_contents($projectJson), true) ?? [])
            : [];

        $notifConfig = $projectData['notification_emails']['dicom']
            ?? $this->config['notification_emails']['dicom']
            ?? $this->config['notification_defaults']
            ?? [];

        if (!($notifConfig['enabled'] ?? false)) {
            $this->_log("  Notifications disabled for dicom");
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
            $this->_log("  No notification emails configured for dicom");
            return;
        }

        // SUCCESS  everything that needed importing went in
        // PARTIAL  some studies imported, some failed - the failures are listed
        // FAILED   nothing imported: the run aborted, or every attempt failed
        $results  = $this->importResults;
        $imported = array_filter($results, fn($r) => in_array($r['status'], ['success', 'already_exists'], true));
        $failed   = array_filter($results, fn($r) => $r['status'] === 'failed');
        $noSessionR = array_filter($results, fn($r) => $r['status'] === 'no_session');
        $skipped  = array_filter($results, fn($r) => $r['status'] === 'skipped');
        $running  = array_filter($results, fn($r) => $r['status'] === 'running');
        $excludedCount = count($this->excludedUids);

        // Studies with no participant never reach this step: count them from
        // the organized manifest so the email still says they were left out.
        $orgManifest = rtrim($projectDir, '/') . '/processed/imaging/dicom_studies.json';
        $orgStudies  = is_file($orgManifest)
            ? ((json_decode((string) file_get_contents($orgManifest), true) ?: [])['studies'] ?? [])
            : [];
        $noParticipant = array_values(array_filter(
            $orgStudies,
            static fn ($st) => ($st['link_status'] ?? '') === 'unlinked'
        ));

        if ($this->abortReason !== null || (!empty($failed) && empty($imported))) {
            $status = 'FAILED';
        } elseif (!empty($failed)) {
            $status = 'PARTIAL';
        } else {
            $status = 'SUCCESS';
        }
        // Warnings recorded by organize and reidentify (orphan folders, rows
        // missing from participants.tsv, unconfigured visits, removals) - the
        // import is the last step, so its email is the run's one summary.
        $prep = [];
        foreach (['processed/imaging/dicom_studies.json',
                  'processed/imaging/dicom_participant_sync.json',
                  'deidentified-lorisid/imaging/dicom_studies_lorisid.json'] as $rel) {
            $m = rtrim($projectDir, '/') . '/' . $rel;
            $d = is_file($m) ? (json_decode((string) file_get_contents($m), true) ?: []) : [];
            foreach ($d['problems'] ?? [] as $pr) {
                $prep[($pr['level'] ?? 'warning') . ': ' . ($pr['message'] ?? '')] = true;
            }
        }
        $prep = array_keys($prep);

        $needsAttention = !empty($this->stillInLoris) || !empty($this->renamed) || !empty($prep)
            || !empty($running) || !empty($this->excludedUids) || !empty($noSessionR);
        $subject = "{$status}" . ($status === 'SUCCESS' && $needsAttention ? ' (warnings)' : '')
            . ": {$projectName} DICOM Import";

        // The names behind the email's counts, written to the run log.
        $this->_log('');
        $this->_log('──── RUN SUMMARY ────');
        foreach ($imported as $study => $r) {
            $this->_log(($r['link_status'] ?? '') === 'linked'
                ? "  ingested with session {$r['session']}: {$study}"
                : "  ingested WITHOUT session ({$r['link_status']}"
                    . (!empty($r['link_reason']) ? ", {$r['link_reason']}" : '') . "): {$study}");
        }
        foreach ($failed as $study => $r) {
            $this->_log("  FAILED, not ingested: {$study}: " . ($r['reason'] ?? 'unknown'));
        }
        foreach ($noSessionR as $study => $r) {
            $this->_log("  NO SESSION, not ingested: {$study}: " . ($r['reason'] ?? 'unknown'));
        }
        foreach ($noParticipant as $st) {
            $this->_log('  unlinked, not ingested: ' . ($st['directory_name'] ?? '?')
                . ' (' . ($st['link_reason'] ?? 'unlinked') . ')');
        }
        foreach ($running as $study => $r) {
            $this->_log("  STILL RUNNING in LORIS (job {$r['job_id']}): {$study} - checked again next run");
        }
        foreach ($this->stillInLoris as $study => $r) {
            $this->_log("  removed from delivery, STILL IN LORIS: {$study}" . ($r['uid'] ? " ({$r['uid']})" : ''));
        }
        foreach ($this->renamed as $old => $new) {
            $this->_log("  relinked: {$old} -> {$new} (old DCM_*_{$old}.tar in the LORIS archive dir is unreferenced)");
        }
        foreach ($prep as $w) {
            $this->_log("  {$w}");
        }

        // Short on purpose: status, counts, and only the failed studies by name.
        // Everything else (orphan names, warnings, removals) is in the run log.

        $body  = "Project: {$projectName} (dicom)\n";
        $body .= "Status:  {$status}\n";
        $body .= "Run:     {$this->runTimestamp}\n";
        if ($this->abortReason !== null) {
            $body .= "Aborted: {$this->abortReason}\n";
        }
        $body .= "\n";
        $body .= sprintf("Ingested:            %d  (all with session)\n", count($imported));
        $body .= sprintf("Failed:              %d\n", count($failed));
        $body .= sprintf("Unchanged, skipped:  %d\n", count($skipped));
        if (!empty($running)) {
            $body .= sprintf("Still running in LORIS: %d (checked again next run)\n", count($running));
        }
        if (!empty($noSessionR)) {
            $body .= sprintf("No session, NOT ingested: %d (ingested on a later run once the session exists)\n", count($noSessionR));
            foreach ($noSessionR as $study => $r) {
                $body .= "  {$study}: " . ($r['reason'] ?? 'unknown') . "\n";
            }
        }
        if ($excludedCount > 0) {
            $body .= sprintf("Excluded, NOT ingested: %d (data issues; continued with the good studies)\n", $excludedCount);
            foreach ($this->excludedLabels as $l) {
                $body .= "  {$l}\n";
            }
            $body .= "  Reasons: bundle log. Fix them and re-run to ingest them.\n";
        }
        if (!empty($noParticipant)) {
            $body .= sprintf("Unlinked, NOT ingested: %d\n", count($noParticipant));
            foreach ($noParticipant as $st) {
                $where = ($st['subject'] ?? null) !== null
                    ? sprintf('sub-%s/ses-%s', $st['subject'], $st['session'] ?? '?')
                    : ($st['directory_name'] ?? '?');
                $body .= "  {$where}: "
                    . DicomPreflight::unlinkedAdvice($st['link_reason'] ?? null, $st['subject'] ?? null) . "\n";
            }
        }
        if (!empty($this->stillInLoris)) {
            $body .= sprintf("Removed from delivery, still in LORIS: %d\n", count($this->stillInLoris));
        }
        // participants.tsv edits for participants already in LORIS are not
        // applied - name them, so someone corrects one side.
        $syncFile = rtrim($projectDir, '/') . '/processed/imaging/dicom_participant_sync.json';
        $differs  = is_file($syncFile)
            ? ((json_decode((string) file_get_contents($syncFile), true) ?: [])['problems'] ?? [])
            : [];
        if (!empty($differs)) {
            $body .= sprintf("participants.tsv differs from LORIS (LORIS not changed): %d\n", count($differs));
            foreach ($differs as $d) {
                $body .= '  ' . explode('. LORIS not changed', (string) ($d['message'] ?? ''))[0] . "\n";
            }
        }
        if (!empty($prep) || !empty($this->renamed)) {
            $body .= sprintf("Warnings:            %d (see run log)\n", count($prep) + count($this->renamed));
        }

        if (!empty($failed)) {
            $body .= "\nFailed studies (not ingested, retried next run):\n";
            foreach ($failed as $study => $r) {
                $body .= "  {$study}: " . ($r['reason'] ?? 'unknown') . "\n";
            }
        }

        // EviData is optional: one line, only when it is on for this
        // project's imaging.
        if (DicomEviDataExtract::isEnabled($projectData, $this->_evidataGlobal(), 'imaging')) {
            $extracts = glob(rtrim($projectDir, '/') . '/logs/evidata/evidata_imaging_*.csv') ?: [];
            sort($extracts);
            $body .= "\nEviData extract: " . ($extracts ? basename(end($extracts)) : 'none found') . "\n";
        }

        if ($this->runLogPath) {
            $body .= "\nRun log: {$this->runLogPath}\n";
        }

        $this->_log("  Sending notification to: " . implode(', ', $emailsToSend));
        foreach ($emailsToSend as $to) {
            $this->notification->send($to, $subject, $body);
        }
    }
}