<?php

declare(strict_types=1);

namespace LORIS\Pipelines\Bids;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use GuzzleHttp\Client as GuzzleClient;
use LORIS\Utils\CleanLogFormatter;
use LORIS\Pipelines\Shared\TrackingFile;

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
 * Per-subject tracking + incremental reidentification:
 *   {projectPath}/processed/bids/.bids_reidentify_tracking.json records
 *   every source sub-* folder that has been reidentified into the target.
 *   On each run:
 *     - If every source sub-* is already in tracking → skip the script entirely.
 *     - If new sub-* folders are present in source → stage just those folders
 *       (plus participants.tsv and other top-level files, via symlinks) into
 *       a temp directory and point the reidentifier at it. Previously
 *       reidentified subjects in the target are untouched.
 *   --force bypasses tracking, deletes the target, and reidentifies
 *   the full source from scratch.
 *
 * Log files written to {projectPath}/logs/bids/:
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

    /** Temp staging dir (symlink tree of new subjects). Cleaned in _closeLogs(). */
    private ?string $stagingDir = null;

    /**
     * Where the reidentifier writes this run (a hidden folder next to the
     * target, same disk). Moved into the target only after a successful run,
     * so an interrupted run never leaves half-written subjects in
     * deidentified-lorisid/; removed on any failure and at the next start.
     */
    private ?string $workDir = null;

    /**
     * Visits held back this run (BidsVisitCheck): sub-<ID> => [ses-<visit> => why].
     * Left out of the staged source; recorded on the subject, which is redone
     * on the next run so the visit goes in once fixed.
     */
    private array $held = [];

    /** Seconds between job status poll requests */
    private const POLL_INTERVAL_SECONDS = 30;

    /** Maximum seconds to wait for reidentification before timing out */
    private const POLL_TIMEOUT_SECONDS = 14400; // 4 hours

    /** Consecutive poll failures before giving up */
    private const MAX_POLL_FAILURES = 3;

    /** Pattern printed by bids_reidentifier.php for unmapped subjects */
    private const UNMAPPED_PATTERN = '/ERROR: no mapping found for (sub-\S+)/i';

    /** Tracking file name — stored in {projectPath}/processed/bids/ */
    private const TRACK_FILE        = '.bids_reidentify_tracking.json';
    private const LEGACY_TRACK_FILE = '.bids_reidentifier_processed.json';

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
        $logDir = "{$projectPath}/logs/bids";
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
        // Always tear down the temp staging dir before closing logs so its
        // cleanup line shows up in the run log and so an interrupted run
        // doesn't leave orphan symlink trees in /tmp.
        $this->_cleanupStaging();
        // A run that did not reach _promote() leaves nothing in the target.
        $this->_cleanupWork();

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
    //  PER-SUBJECT TRACKING
    //
    //  processed/bids/.bids_reidentify_tracking.json keeps a record of
    //  every source sub-* directory that has been successfully reidentified
    //  into the target. The structure:
    //
    //    {
    //      "subjects": {
    //        "sub-FDGP001": {
    //          "reidentified_at": "2026-04-30T12:00:00+00:00",
    //          "run_timestamp"  : "2026-04-30_12-00-00"
    //        },
    //        ...
    //      },
    //      "last_run": {
    //        "status"             : "success" | "failed",
    //        "timestamp"          : "...",
    //        "run_timestamp"      : "...",
    //        "subjects_processed" : N,
    //        "subjects_total"     : M,
    //        "detail"             : "..."
    //      }
    //    }
    //
    //  --force bypasses this entirely and rebuilds the target from scratch,
    //  marking every source subject as freshly reidentified at the end.
    // =========================================================================

    /**
     * Load the tracking dictionary from {projectPath}/processed/bids/.
     * Returns an empty skeleton (no subjects, no last_run) when no tracking
     * file exists yet.
     */
    private function _loadTracking(string $projectPath): array
    {
        $dir  = rtrim($projectPath, '/') . '/processed/bids/';
        $path = TrackingFile::readPath($dir . self::TRACK_FILE, $dir . self::LEGACY_TRACK_FILE);
        if (!file_exists($path)) {
            return ['subjects' => [], 'last_run' => null];
        }
        $data = json_decode(file_get_contents($path), true);
        if (!is_array($data)) {
            return ['subjects' => [], 'last_run' => null];
        }
        $data['subjects'] = $data['subjects'] ?? [];
        $data['last_run'] = $data['last_run'] ?? null;
        return $data;
    }

    /**
     * Persist the tracking dictionary. Creates processed/bids/ if missing.
     */
    private function _saveTracking(string $projectPath, array $tracking): void
    {
        $dir = rtrim($projectPath, '/') . '/processed/bids';
        if (!is_dir($dir)) {
            @mkdir($dir, 0755, true);
        }
        $path = "{$dir}/" . self::TRACK_FILE;
        $ok   = file_put_contents(
            $path,
            json_encode($tracking, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
        );
        if ($ok !== false) {
            TrackingFile::retireLegacy($path, "{$dir}/" . self::LEGACY_TRACK_FILE);
        }
    }

    /**
     * Record a successful reidentification: stamp every $subjects entry
     * with this run's timestamp, refresh last_run, and write to disk.
     */
    private function _markSuccess(
        string $projectPath,
        array $tracking,
        array $subjects,
        string $detail = ''
    ): void {
        $now = date('c');

        if (!isset($tracking['subjects']) || !is_array($tracking['subjects'])) {
            $tracking['subjects'] = [];
        }
        foreach ($subjects as $subj) {
            $tracking['subjects'][$subj] = [
                'reidentified_at' => $now,
                'run_timestamp'   => $this->runTimestamp,
            ] + (empty($this->held[$subj]) ? [] : ['held' => array_keys($this->held[$subj])]);
        }

        $tracking['last_run'] = [
            'status'             => 'success',
            'timestamp'          => $now,
            'run_timestamp'      => $this->runTimestamp,
            'subjects_processed' => count($subjects),
            'subjects_total'     => count($tracking['subjects']),
            'detail'             => $detail,
        ];

        $this->_saveTracking($projectPath, $tracking);
        $this->_log("  Tracking updated: " . count($subjects)
            . " subject(s) marked reidentified"
            . " (total tracked: " . count($tracking['subjects']) . ")");
    }

    /**
     * Record a failed run. Subject map is left intact — only last_run is
     * updated so the operator can see what went wrong without losing the
     * record of previously successful subjects.
     */
    private function _markFailed(
        string $projectPath,
        array $tracking,
        string $detail
    ): void {
        $tracking['last_run'] = [
            'status'             => 'failed',
            'timestamp'          => date('c'),
            'run_timestamp'      => $this->runTimestamp,
            'subjects_processed' => 0,
            'subjects_total'     => count($tracking['subjects'] ?? []),
            'detail'             => $detail,
        ];
        $this->_saveTracking($projectPath, $tracking);
    }

    // =========================================================================
    //  STAGING
    //
    //  When only some source sub-* folders are new, we stage just those
    //  folders into a temp directory using symlinks, then point the
    //  reidentifier at the temp dir. The script processes only the new
    //  subjects and writes them to the actual target. Previously
    //  reidentified subjects in the target remain untouched.
    //
    //  participants.tsv is symlinked too — the script reads it for
    //  ID resolution. Other top-level files (dataset_description.json,
    //  README, etc.) are also symlinked so the BIDS layout is intact.
    //
    //  Symlinks instead of copies: zero disk cost and the script reads
    //  through them transparently. The output written to target is regular
    //  files (the script copies content while renaming), so the symlinks
    //  themselves never end up in target.
    // =========================================================================

    /**
     * Build a temp BIDS tree containing only the requested sub-* folders.
     * Returns the staging path on success, null on failure (logs the error
     * and cleans up any partial directory).
     *
     * The created directory is recorded on $this->stagingDir so it gets
     * cleaned up automatically by _closeLogs() on every exit path.
     */
    private function _stageNewSubjects(string $sourceDir, array $newSubjects): ?string
    {
        $sourceDir = rtrim($sourceDir, '/');
        $stagePath = sys_get_temp_dir() . '/bids_reidentifier_stage_' . $this->runTimestamp;

        if (is_dir($stagePath)) {
            // Highly unlikely, but be defensive
            exec('rm -rf ' . escapeshellarg($stagePath));
        }
        if (!@mkdir($stagePath, 0755, true)) {
            $this->_error("STAGING", "Cannot create staging directory: {$stagePath}");
            return null;
        }

        $this->stagingDir = $stagePath;

        // Top-level files (participants.tsv, dataset_description.json, README, ...).
        // The script needs participants.tsv for ID mapping; the rest keep the BIDS
        // tree well-formed. Directories at top level are skipped except for sub-*
        // folders, which are linked individually below.
        foreach (glob("{$sourceDir}/*") as $item) {
            $name = basename($item);
            if (str_starts_with($name, 'sub-')) {
                continue; // handled below, only the new ones
            }
            if (!is_file($item)) {
                continue; // skip non-sub directories
            }
            if (!@symlink($item, "{$stagePath}/{$name}")) {
                $this->_error("STAGING",
                    "Failed to symlink top-level file: {$name}"
                    . " — falling back to no-staging full run"
                );
                return null;
            }
        }

        // Symlink each new sub-* directory; a subject with visits held back
        // gets a real folder holding links to everything but those visits.
        foreach ($newSubjects as $subj) {
            $src = "{$sourceDir}/{$subj}";
            $dst = "{$stagePath}/{$subj}";
            if (!is_dir($src)) {
                $this->_error("STAGING", "New subject directory missing: {$src}");
                return null;
            }
            if (!empty($this->held[$subj])) {
                if (!@mkdir($dst)) {
                    $this->_error("STAGING", "Cannot create {$dst}");
                    return null;
                }
                foreach (scandir($src) ?: [] as $entry) {
                    if ($entry === '.' || $entry === '..' || isset($this->held[$subj][$entry])) {
                        continue;
                    }
                    if (!@symlink("{$src}/{$entry}", "{$dst}/{$entry}")) {
                        $this->_error("STAGING", "Failed to symlink {$subj}/{$entry}");
                        return null;
                    }
                }
                continue;
            }
            if (!@symlink($src, $dst)) {
                $this->_error("STAGING", "Failed to symlink subject: {$subj}");
                return null;
            }
        }

        $this->_log("  Staging directory: {$stagePath}");
        $this->_log("    Staged " . count($newSubjects) . " sub-* folder(s) via symlink"
            . ($this->held !== [] ? ', held-back visits left out' : ''));
        return $stagePath;
    }

    /**
     * Tear down the staging directory if one was created. Idempotent.
     * Called from _closeLogs() so every exit path triggers cleanup.
     */
    /**
     * Move this run's output into the target, then record the subjects.
     * A subject already in the target is replaced as a whole (no stale files
     * from an earlier copy); with --force the whole target is replaced.
     * If the move fails, the run is recorded as failed (subjects not marked
     * done), so the next run redoes them.
     */
    private function _finishSuccess(
        string $projectPath,
        array $tracking,
        array $subjects,
        string $targetDir,
        bool $force,
        string $detail
    ): void {
        $problem = $this->_promote($targetDir, $force);
        if ($problem !== null) {
            $this->_error('PROMOTE', $problem . ' - the subjects of this run are not recorded; run again');
            $this->_markFailed($projectPath, $tracking, 'moving the output into place failed: ' . $problem);
            return;
        }
        $this->_markSuccess($projectPath, $tracking, $subjects, $detail);
    }

    /**
     * @return string|null Why the output could not be moved into place, or null.
     */
    private function _promote(string $targetDir, bool $force): ?string
    {
        $work   = (string) $this->workDir;
        $target = rtrim($targetDir, '/');
        if (!is_dir($work)) {
            return "the work folder {$work} is missing";
        }
        if ($force) {
            // Full rebuild: swap the whole tree.
            if (is_dir($target)) {
                exec('rm -rf ' . escapeshellarg($target), $o, $rc);
                if ($rc !== 0 || is_dir($target)) {
                    return "cannot remove the previous output {$target}";
                }
            }
            if (!@rename($work, $target)) {
                return "cannot move {$work} to {$target}";
            }
            $this->workDir = null;
            $this->_log("  Output in place (full rebuild): {$target}");
            return null;
        }
        if (!is_dir($target) && !@mkdir($target, 0777, true)) {
            return "cannot create {$target}";
        }
        $moved = 0;
        foreach (scandir($work) ?: [] as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }
            $from = "{$work}/{$entry}";
            $to   = "{$target}/{$entry}";
            if (file_exists($to) || is_link($to)) {
                exec('rm -rf ' . escapeshellarg($to), $o, $rc);
                if ($rc !== 0 || file_exists($to)) {
                    return "cannot replace {$to}";
                }
            }
            if (!@rename($from, $to)) {
                return "cannot move {$from} to {$to}";
            }
            $moved++;
        }
        exec('rm -rf ' . escapeshellarg($work));
        $this->workDir = null;
        $this->_log("  Output in place: {$moved} entr" . ($moved === 1 ? 'y' : 'ies') . " moved into {$target}");
        return null;
    }

    /** Remove this run's work folder when the run did not finish. Idempotent. */
    private function _cleanupWork(): void
    {
        if ($this->workDir !== null && is_dir($this->workDir)) {
            exec('rm -rf ' . escapeshellarg($this->workDir));
            $this->_log("  Unfinished output removed (nothing changed in the target): {$this->workDir}");
        }
        $this->workDir = null;
    }

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

        // Shared reader: header names in any case and their aliases
        // (Participant_ID, ExtStudyID ...), as the participant sync reads them.
        try {
            $parsed = \LORIS\Pipelines\Shared\ParticipantsTsv::readRows($tsvFile);
        } catch (\RuntimeException $e) {
            return [];
        }

        $participants = [];
        foreach ($parsed['rows'] as $entry) {
            $row = (array) ($entry['row'] ?? []);
            $pid = trim((string) ($row['participant_id'] ?? ''));
            if ($pid !== '') {
                $participants[] = [
                    'participant_id' => $pid,
                    'external_id'    => $this->_extractExternalID($row) ?? substr($pid, 4),
                ];
            }
        }
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
        $force      = $options['force']   ?? false;
        $cliProject = $options['project'] ?? null;

        $projectPath = dirname(dirname(rtrim($sourceDir, '/')));
        $this->_openLogs($projectPath);

        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  BIDS Reidentifier");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  Source  : {$sourceDir}");
        $this->_log("  Target  : {$targetDir}");
        $this->_log("  Dry run : " . ($dryRun ? 'YES' : 'NO'));
        $this->_log("  Force   : " . ($force  ? 'YES' : 'NO'));
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

        // ── Tracking + new-subject detection ──────────────────────────────
        // Decide what work is needed before touching the API: glob source
        // sub-* dirs, diff against tracked ones. Skip when nothing is new;
        // otherwise either stage just the new subjects (default) or run
        // the full source (--force).
        $tracking         = $this->_loadTracking($projectPath);
        $sourceSubjects   = array_map('basename',
            glob(rtrim($sourceDir, '/') . '/sub-*', GLOB_ONLYDIR) ?: []
        );
        sort($sourceSubjects);

        // ── Visits held back (flagged; the rest goes in) ──────────────────
        $this->held = BidsVisitCheck::held($sourceDir, $this->projectConfig ?? []);

        // A subject recorded with visits held back is redone once those
        // change (a folder fixed or added), so the fixed visit goes in.
        $trackedSubjects  = [];
        foreach ((array) ($tracking['subjects'] ?? []) as $sub => $entry) {
            $was = (array) ($entry['held'] ?? []);
            $now = array_keys($this->held[$sub] ?? []);
            sort($was);
            sort($now);
            if ($was === $now) {
                $trackedSubjects[] = $sub;
            }
        }
        $allHeld    = [];
        foreach ($this->held as $sub => $sessions) {
            foreach ($sessions as $ses => $why) {
                $this->_warn('VISIT MISMATCH', "{$sub}/{$ses}: {$why} - HELD BACK"
                    . ($dryRun ? ' (would be left out)' : ' (not reidentified, not imported; the rest goes in)'));
            }
            $sesDirs = glob(rtrim($sourceDir, '/') . "/{$sub}/ses-*", GLOB_ONLYDIR) ?: [];
            if (count($sesDirs) === count($sessions)) {
                $allHeld[] = $sub;   // nothing of this subject can go in yet
            }
        }

        $newSubjects      = array_values(array_diff($sourceSubjects, $trackedSubjects, $allHeld));
        if ($allHeld !== []) {
            $this->_log('  Every visit held back, not processed: ' . implode(', ', $allHeld));
        }
        $alreadyDone      = count($sourceSubjects) - count($newSubjects);
        $orphanInTracking = array_values(array_diff($trackedSubjects, $sourceSubjects));

        $lastRun = $tracking['last_run'] ?? null;
        $this->_log("  Tracking: "
            . ($lastRun
                ? "last run {$lastRun['timestamp']} — {$lastRun['status']}"
                . " (" . count($trackedSubjects) . " subject(s) on record)"
                : "no previous run"));
        $this->_log("  Source has " . count($sourceSubjects) . " sub-* dir(s):"
            . " {$alreadyDone} already reidentified, " . count($newSubjects) . " new");

        if (!empty($orphanInTracking)) {
            $this->_log("  NOTE: " . count($orphanInTracking)
                . " subject(s) in tracking are missing from source — orphans"
                . " may remain in target. Use --force for clean rebuild.");
        }

        // No new subjects → skip entirely.
        if (!$force && !$dryRun && empty($newSubjects)) {
            if ($this->held !== []) {
                $this->_log("  Nothing else to do: only held-back visits remain - fix them and re-run");
            }
            $this->_log("");
            $this->_log("  ✓ All source subjects already reidentified — skipping");
            $this->_log("    Use --force to re-run anyway.");
            $this->stats['subjects_mapped'] = $this->stats['subjects_found'];
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        // Show which subjects will be processed (new ones).
        if (!$force && !empty($newSubjects)) {
            $preview = array_slice($newSubjects, 0, 10);
            $suffix  = count($newSubjects) > 10
                ? ' +' . (count($newSubjects) - 10) . ' more'
                : '';
            $this->_log("  New subjects to reidentify: "
                . implode(', ', $preview) . $suffix);
        }
        if ($force) {
            $this->_log("  --force: bypassing tracking — full source rebuild");
        }

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
            $this->_log("  Would process: " . ($force
                    ? "full source (" . count($sourceSubjects) . " subjects, --force)"
                    : count($newSubjects) . " new subject(s), staged via symlinks")
                . ($this->held !== [] ? '; held-back visits left out (see VISIT MISMATCH above)' : ''));
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        // --force: the existing output is kept until the rebuild succeeded,
        // then replaced as a whole (_promote) - an interrupted rebuild leaves
        // the previous, complete output in place.

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

        // ── Work folder: a fresh one each run ─────────────────────────────
        // Output of an earlier run that did not finish (crash, kill, error)
        // is removed here, so it is never taken for finished work.
        // One folder per run: the LORIS-side job of a killed run may still be
        // writing into its own folder, never into this run's.
        $workBase = dirname(rtrim($targetDir, '/')) . '/.' . basename(rtrim($targetDir, '/')) . '.reidentify-tmp';
        foreach (glob($workBase . '*', GLOB_ONLYDIR) ?: [] as $stale) {
            exec('rm -rf ' . escapeshellarg($stale));
            $this->_log("  Removed the unfinished output of an earlier run: {$stale}");
        }
        $this->workDir = $workBase . '-' . $this->runTimestamp;
        if (!@mkdir($this->workDir, 0777, true)) {
            $this->_error("TARGET_DIR", "Cannot create the work folder {$this->workDir} - check the parent is writable");
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }
        chmod($this->workDir, 0777);   // written by the LORIS host (bids_reidentifier.php)

        // ── Decide effective source ───────────────────────────────────────
        // --force: run on the original source (full rebuild).
        // First run with no tracking and all subjects new: also run on
        //   original source — staging would be a 1:1 mirror, no benefit.
        // Otherwise: stage only the new subjects.
        $effectiveSourceDir   = $sourceDir;
        $toDo                 = $force ? array_values(array_diff($sourceSubjects, $allHeld)) : $newSubjects;
        $subjectsToReidentify = $sourceSubjects; // recorded on success
        $holding              = array_intersect($toDo, array_keys($this->held)) !== [];

        if (count($toDo) < count($sourceSubjects) || $holding) {
            $staged = $this->_stageNewSubjects($sourceDir, $toDo);
            if ($staged === null) {
                // _stageNewSubjects already logged the error and recorded it
                // in stats; the staging dir (if partially created) is on
                // $this->stagingDir and will be cleaned by _closeLogs.
                $this->_markFailed($projectPath, $tracking,
                    "staging failed for " . count($newSubjects) . " subject(s)");
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }
            $effectiveSourceDir   = $staged;
            $subjectsToReidentify = $toDo;
        }

        if (!$this->_authenticate()) {
            $this->_markFailed($projectPath, $tracking, "authentication failed");
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        // Launch async job
        $launch = $this->_launchAsync($effectiveSourceDir, $this->workDir, $idPattern, $projectList);

        $httpStatus = $launch['http_status'] ?? 0;

        if ($httpStatus !== 202) {
            $errMsg = $launch['error']
                ?? ($launch['error'] ?? json_encode($launch))
                ?? "HTTP {$httpStatus}";
            $this->_error("LAUNCH",
                "Failed to launch bidsreidentifier (HTTP {$httpStatus}): {$errMsg}"
                . " — check /cbigr_api/script/bidsreidentifier endpoint and Apache logs"
            );
            $this->_markFailed($projectPath, $tracking, "launch HTTP {$httpStatus}");
            $this->_printSummary();
            $this->_closeLogs();
            return $this->stats;
        }

        $jobId = $launch['job_id'] ?? null;
        $pid   = $launch['pid']    ?? null;

        if (!$jobId) {
            $this->_error("LAUNCH", "No job_id returned from async launch");
            $this->_markFailed($projectPath, $tracking, "no job_id from launch");
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
        $pollFailures    = 0;
        $wasRunning      = false;
        $firstPoll       = true;
        $firstPollRetry  = false; // retry once if DB row not yet written

        while (true) {
            if ($firstPoll) {
                $firstPoll = false;
                // Short first poll — catches fast-completing scripts
                // (all files already reidentified → script exits in ~1s)
                sleep(5);
            } else {
                sleep(self::POLL_INTERVAL_SECONDS);
            }

            $elapsed = round(microtime(true) - $t0, 2);

            if ((microtime(true) - $pollStart) > self::POLL_TIMEOUT_SECONDS) {
                $this->_error("TIMEOUT",
                    "Job {$jobId} timed out after " . self::POLL_TIMEOUT_SECONDS . "s"
                );
                $this->_markFailed($projectPath, $tracking,
                    "timeout after " . self::POLL_TIMEOUT_SECONDS . "s job_id={$jobId}");
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }

            $status = $this->_pollJob($jobId);

            if ($status === null) {
                // First poll null — SPM DB row may not be written yet.
                // Retry once after a short delay before counting as failure.
                if (!$firstPollRetry) {
                    $firstPollRetry = true;
                    $this->logger->debug("  First poll null — SPM row may not be ready, retrying in 3s");
                    sleep(3);
                    continue;
                }

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
                    $this->_markFailed($projectPath, $tracking,
                        "poll failed job_id={$jobId}");
                    $this->_printSummary();
                    $this->_closeLogs();
                    return $this->stats;
                }
                continue;
            }

            // Once we get a real response, disable the first-poll retry logic
            $firstPollRetry = true;

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
                    $this->_finishSuccess($projectPath, $tracking, $subjectsToReidentify, $targetDir, $force,
                        "job_id={$jobId} (UNKNOWN confirmed via wasRunning)");
                    $this->_printSummary();
                    $this->_closeLogs();
                    return $this->stats;
                }
                $this->_error("UNKNOWN_STATE",
                    "Job {$jobId} ended UNKNOWN without ever running"
                    . " — check server_processes id={$jobId} and Apache error log"
                );
                $this->_markFailed($projectPath, $tracking,
                    "UNKNOWN without RUNNING job_id={$jobId}");
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }

            if ($state === 'SUCCESS') {
                $this->_log("  ✓ SUCCESS ({$elapsed}s)");
                $output = $status['progress'] ?? '';
                $this->_parseScriptOutput($output);
                $this->_finishSuccess($projectPath, $tracking, $subjectsToReidentify, $targetDir, $force,
                    "job_id={$jobId}");
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
            //
            // This ONLY applies when exit code is genuinely unknown (not when
            // SPM has a real non-zero exit code). Confirm by checking:
            //   1. Error detail says "exit code unknown" (SPM race condition marker)
            //   2. Target has at least the expected number of sub-* dirs
            //
            // For the staging case we only expect the new subjects to land in
            // target *during this run* — but target may already contain prior
            // reidentified subjects from earlier runs, so the count check stays
            // against the source-total expectation.
            $isExitCodeUnknown = str_contains(strtolower($errorDetail), 'exit code unknown');
            $targetSubjects    = glob("{$this->workDir}/sub-*", GLOB_ONLYDIR) ?: [];
            $targetCount       = count($targetSubjects);
            $sourceCount       = count($subjectsToReidentify);

            if ($isExitCodeUnknown
                && $targetCount > 0
                && $targetCount >= $sourceCount
            ) {
                $this->_log(
                    "  ✓ SUCCESS (exit code race condition — script completed"
                    . " before first poll. Target has {$targetCount}/{$sourceCount}"
                    . " sub-* dirs) ({$elapsed}s)"
                );
                $this->stats['subjects_mapped'] = $sourceCount;
                $this->_finishSuccess($projectPath, $tracking, $subjectsToReidentify, $targetDir, $force,
                    "job_id={$jobId} (race condition recovery)");
                $this->_printSummary();
                $this->_closeLogs();
                return $this->stats;
            }

            // If exit code unknown but target is empty or partial → genuine failure
            if ($isExitCodeUnknown && $targetCount < $sourceCount) {
                $this->_error("SCRIPT_FAILED",
                    "bids_reidentifier.php failed — exit code unknown and only"
                    . " {$targetCount}/{$sourceCount} sub-* dirs in target."
                    . " job_id={$jobId}. Check Apache error log."
                );
                $this->_markFailed($projectPath, $tracking,
                    "exit unknown, partial target ({$targetCount}/{$sourceCount})"
                    . " job_id={$jobId}");
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
            $this->_markFailed($projectPath, $tracking,
                "exit {$exitCode} job_id={$jobId}");
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