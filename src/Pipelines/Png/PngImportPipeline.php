<?php

/**
 * PngImportPipeline
 *
 * Step 3 of the PNG pipeline: ingest the reidentified visits from
 * deidentified-lorisid/imaging/png/ through
 *
 *   POST /cbigr_api/script/importpngdataset   (import_png_dataset.py, async via SPM)
 *   GET  /cbigr_api/script/job/{id}           (poll until SUCCESS / ERROR)
 *
 * One job per visit (--subject sub-<PSCID> --visit ses-<Visit>), so a problem
 * in one visit never blocks the others - the LORIS script validates its whole
 * selection before inserting anything.
 *
 * How this differs from the DICOM import, on purpose:
 *   - no insert/update/overwrite modes: the LORIS script skips any image whose
 *     hash is already in the files table and gives every new image a free
 *     version number. Re-sending a visit is always safe: what is in LORIS is
 *     skipped, what is new goes in. So a changed visit is simply sent again,
 *     and a failed or partial one is retried as is.
 *   - LORIS output is read per image: "inserted as FileID N (path)" and
 *     "already registered ... (FileID N)". Both are kept in the tracking file,
 *     so every image the site sent can be traced to its LORIS FileID.
 *   - environment problems that fail every visit the same way (no 'png' row in
 *     ImagingFileTypes, data dir or pic/ not writable, endpoint missing,
 *     permission) stop the project after the first visit instead of failing
 *     each one.
 *
 * Shared with DICOM: dry run by default; ingestion only from
 * deidentified-lorisid/; a visit is sent only when its session exists in
 * LORIS; fingerprints decide what is redone (--force: everything); a job the
 * run stopped watching is recorded as running and asked about next run, never
 * started twice; full output of every failed job in the error log; one email
 * per project.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Png;

use GuzzleHttp\Client as HttpClient;
use LORIS\Pipelines\Shared\PipelineSetup;
use LORIS\Utils\Notification;

class PngImportPipeline
{
    public const ENDPOINT = 'cbigr_api/script/importpngdataset';

    /** Exit codes of import_png_dataset.py (lib/exitcode.py). */
    private const EXIT_INVALID_PATH   = 5;
    private const EXIT_INVALID_IMPORT = 7;
    private const EXIT_INSERT_FAILURE = 21;

    /** An unanswered job older than this is treated as lost (failed). */
    private const LOST_JOB_HOURS = 48;

    /** Output that means "every visit will fail the same way": stop the project. */
    private const ENVIRONMENT_PATTERNS = [
        "/ImagingFileTypes.*no 'png' row/i"                     => "LORIS has no 'png' row in ImagingFileTypes - run the SQL patch (docs/png.md, LORIS side)",
        '/is not a directory writable by/i'                     => 'the LORIS data directory (assembly_png/) is not writable by the user running the script',
        '/does not exist\. It is created by imaging_install/i'  => 'the LORIS pic/ directory is missing',
        "/Missing 'dataDirBasepath' configuration/i"            => 'LORIS Config dataDirBasepath is not set',
        '/ModuleNotFoundError|ImportError/i'                    => 'the Loris-MRI Python environment cannot import the PNG script (cbig package) - see docs/png.md',
        '/OperationalError|Access denied for user/i'            => 'the Loris-MRI script cannot connect to the database (profile / database_config.py)',
    ];

    private HttpClient $http;
    private string $token = '';
    private string $apiVersion;
    private array $stats = [
        'visits_found' => 0, 'visits_imported' => 0, 'visits_skipped' => 0, 'visits_failed' => 0,
        'visits_no_session' => 0, 'visits_running' => 0, 'visits_excluded' => 0,
        'images_inserted' => 0, 'images_already' => 0, 'images_failed' => 0,
    ];

    /** @var array<int, array{level: string, visit: ?string, message: string}> */
    private array $problems = [];

    /** @var array<string, array> result per lorisid key, for the summary */
    private array $results = [];

    private ?string $abortReason = null;

    /** @var resource|null */
    private $runLog = null;

    /** @var resource|null */
    private $errorLog = null;

    private ?string $runLogPath = null;
    private ?string $errorLogPath = null;

    /** @var callable */
    private $out;

    public function __construct(
        private array $config,
        private string $projectPath,
        private bool $dryRun = true,
        private bool $verbose = false,
        ?callable $out = null,
        ?HttpClient $http = null
    ) {
        $api = $config['loris'] ?? $config['api'] ?? [];
        $this->apiVersion = (string) ($api['api_version'] ?? $config['api_version'] ?? 'v0.0.4-dev');
        $this->http = $http ?? new HttpClient([
            'base_uri'    => rtrim((string) ($api['base_url'] ?? ''), '/') . '/',
            'timeout'     => 30,
            'verify'      => $config['verify_ssl'] ?? true,
            'http_errors' => false,
        ]);
        $this->out = $out ?? static function (string $line): void {
            fwrite(STDOUT, $line . "\n");
        };
    }

    public function getStats(): array
    {
        return $this->stats;
    }

    public function getProblems(): array
    {
        return $this->problems;
    }

    /**
     * @param array $opts force (bool), validate_only (bool), create_pic (bool),
     *                    profile (string), excluded_visits (raw keys),
     *                    excluded_subjects (raw subjects), poll_interval (s),
     *                    poll_timeout (s), notify (bool)
     *
     * @return array{status: string, stats: array, problems: array}
     */
    public function run(array $opts): array
    {
        $force        = (bool) ($opts['force'] ?? false);
        $validateOnly = (bool) ($opts['validate_only'] ?? false);
        $createPic    = (bool) ($opts['create_pic'] ?? true);
        $profile      = (string) ($opts['profile'] ?? 'database_config.py');
        $interval     = max(1, (int) ($opts['poll_interval'] ?? 5));
        $timeout      = max(30, (int) ($opts['poll_timeout'] ?? 3600));
        $exVisits     = array_flip((array) ($opts['excluded_visits'] ?? []));
        $exSubjects   = array_flip((array) ($opts['excluded_subjects'] ?? []));

        $this->_openLogs();
        $root     = PngPaths::of($this->projectPath, PngPaths::LORISID_SUBDIR);
        $manifest = PngPaths::readJson(PngPaths::of($this->projectPath, PngPaths::LORISID_MANIFEST));
        $visits   = (array) ($manifest['visits'] ?? []);
        usort($visits, static fn ($a, $b) => strcmp((string) $a['lorisid_key'], (string) $b['lorisid_key']));

        $trackPath = PngPaths::of($this->projectPath, PngPaths::IMPORT_TRACKING);
        $tracking  = (array) (PngPaths::readJson($trackPath)['visits'] ?? []);

        $this->_log("Source: {$root}");
        $this->_log(sprintf('Found %d reidentified visit(s)%s', count($visits),
            $validateOnly ? ' - VALIDATE ONLY: LORIS checks everything, nothing is inserted' : ''));

        if ($visits === [] && !is_dir($root)) {
            $this->_log('Nothing reidentified yet - run run_png_reidentifier.php (--confirm) first');
        }

        // The import sends the run's one email: carry the warnings of the
        // latest participant sync and reidentification into it (as DICOM).
        // Each file's warnings are carried once (by its generated_at), so an
        // import-only re-run does not repeat them.
        $carried = (array) (PngPaths::readJson($trackPath)['carried'] ?? []);
        if (!$this->dryRun && !$validateOnly) {
            foreach ([PngPaths::SYNC_WARNINGS => 'participant sync', PngPaths::LORISID_MANIFEST => 'reidentify'] as $file => $step) {
                $data = PngPaths::readJson(PngPaths::of($this->projectPath, $file));
                $when = (string) ($data['generated_at'] ?? '');
                if ($when === '' || ($carried[$file] ?? null) === $when) {
                    continue;
                }
                $carried[$file] = $when;
                foreach ((array) ($data['problems'] ?? []) as $p) {
                    if (($p['level'] ?? '') === 'warning' && !empty($p['message'])) {
                        $this->problems[] = ['level' => 'warning', 'visit' => $p['key'] ?? null,
                            'message' => "{$step}: {$p['message']}", 'carried' => true];
                    }
                }
            }
        }

        // What the site must still fix, from the latest scan: images set
        // aside and visits not matched to a participant (carried once), and
        // what the operator left out this run (bundle exclusion file).
        if (!$this->dryRun && !$validateOnly) {
            $scan = PngPaths::readJson(PngPaths::of($this->projectPath, PngPaths::SCAN_MANIFEST));
            $when = (string) ($scan['generated_at'] ?? '');
            if ($when !== '' && empty($scan['dry_run']) && ($carried[PngPaths::SCAN_MANIFEST] ?? null) !== $when) {
                $carried[PngPaths::SCAN_MANIFEST] = $when;
                foreach ((array) ($scan['visits'] ?? []) as $sv) {
                    foreach ((array) ($sv['set_aside'] ?? []) as $a) {
                        $this->problems[] = ['level' => 'warning', 'visit' => null, 'carried' => true, 'message' =>
                            "SET ASIDE sub-{$sv['subject']}/ses-{$sv['session']}/{$a['name']}: {$a['reason']}"];
                    }
                    if (($sv['link_status'] ?? '') === 'unlinked') {
                        $this->problems[] = ['level' => 'warning', 'visit' => null, 'carried' => true, 'message' =>
                            "UNLINKED sub-{$sv['subject']}/ses-{$sv['session']}: " . PngDelivery::unlinkedAdvice(
                                $sv['link_reason'] ?? null, (string) $sv['subject'], (string) $sv['session'])];
                    }
                }
            }
            foreach ((array) ($opts['excluded_issues'] ?? []) as $where => $reasons) {
                $this->problems[] = ['level' => 'warning', 'visit' => (string) $where, 'carried' => true, 'message' =>
                    "EXCLUDED {$where} (data issue, not processed this run): " . implode(' | ', (array) $reasons)];
            }
        }
        $this->carried = $carried;
        if (!$this->dryRun && !$validateOnly) {
            $this->_saveTracking($trackPath, $tracking);
        }

        $needsLoris = !$this->dryRun || $validateOnly;
        if ($needsLoris && $visits !== [] && !$this->_authenticate()) {
            $this->abortReason = 'LORIS authentication failed';
        }

        foreach ($visits as $v) {
            if ($this->abortReason !== null) {
                break;
            }
            $lk  = (string) $v['lorisid_key'];
            $tag = "sub-{$v['pscid']}/ses-{$v['visit_label']}";
            $this->stats['visits_found']++;

            if (isset($exVisits[$v['key']]) || isset($exSubjects[(string) $v['subject']])) {
                $this->_log("  {$tag}: excluded (issue reported) - not sent this run");
                $this->stats['visits_excluded']++;
                continue;
            }

            if (!is_dir((string) $v['directory'])) {
                $this->_fail($tracking, $lk, $tag, 'reidentified copy is missing at ' . $v['directory']
                    . ' - run run_png_reidentifier.php --force', null, false);
                continue;
            }

            // What is on disk is exactly what was reidentified (and verified).
            $integrity = $this->_integrity($v);
            if ($integrity !== null) {
                $this->_fail($tracking, $lk, $tag, $integrity, null, false);
                continue;
            }

            $fp  = (string) ($v['lorisid_fingerprint'] ?? $v['fingerprint'] ?? '');
            $was = $tracking[$lk] ?? null;

            // A job a previous run stopped watching: never start it twice.
            if (!$validateOnly && ($was['status'] ?? '') === 'running' && !empty($was['job_id'])) {
                if ($this->dryRun) {
                    $this->_log("  {$tag}: recorded as still running in LORIS (job {$was['job_id']}) - checked on a real run");
                    continue;
                }
                $resolved = $this->_resolveRunning($tracking, $lk, $tag, $was, $fp);
                if ($resolved) {
                    continue;
                }
                $was = $tracking[$lk] ?? null;
            }

            if (!$validateOnly && !$force && ($was['status'] ?? '') === 'success' && ($was['fingerprint'] ?? '') === $fp) {
                $this->_log("  {$tag}: SKIPPED - unchanged since " . ($was['timestamp'] ?? '?'));
                $this->stats['visits_skipped']++;
                $this->results[$lk] = ['status' => 'skipped'];
                continue;
            }

            if ($this->dryRun && !$validateOnly) {
                $why = $was === null ? 'new' : (($was['status'] ?? '') === 'success' ? 'changed' : 'retry: ' . ($was['status'] ?? '?'));
                $this->_log(sprintf('  %s: would import %d image(s) (%s)%s', $tag, count($v['images'] ?? []), $why,
                    $force ? ' [--force]' : ''));
                continue;
            }

            // No session, no import (as DICOM): the LORIS script needs it.
            $session = $this->_sessionExists((string) $v['cand_id'], (string) $v['visit_label']);
            if ($session === false) {
                $this->_problem('warning', $tag, "NO SESSION {$tag} - LORIS has no visit {$v['visit_label']} for "
                    . "CandID {$v['cand_id']} - not ingested. Create the visit, or run run_png_reidentifier.php "
                    . '--force; the next run imports it');
                $this->stats['visits_no_session']++;
                $this->results[$lk] = ['status' => 'no_session'];
                continue;
            }

            $this->_importVisit($tracking, $trackPath, $v, $lk, $tag, $fp, $profile, $createPic, $validateOnly,
                $interval, $timeout);
        }

        // Ingested before, no longer reidentified: LORIS is not changed.
        if (!$validateOnly) {
            $current = array_flip(array_column($visits, 'lorisid_key'));
            foreach ($tracking as $lk => $t) {
                if (!isset($current[$lk]) && ($t['status'] ?? '') === 'success') {
                    $this->_problem('warning', $lk, "REMOVED {$lk}: no longer in the delivery - its images are "
                        . 'STILL IN LORIS (FileIDs ' . implode(', ', array_slice($t['file_ids'] ?? [], 0, 10)) . ')');
                }
            }
        }

        if ($this->abortReason !== null) {
            $this->_problem('error', null, 'STOPPED: ' . $this->abortReason
                . ' - the remaining visits were not sent; fix it and re-run (nothing is sent twice)');
        }

        $status = $this->_status($validateOnly);
        $this->_summary($status, $validateOnly);
        $this->_closeLogs();

        if (!$this->dryRun && !$validateOnly && ($opts['notify'] ?? true)) {
            $this->_notify($status);
        }

        return ['status' => $status, 'stats' => $this->stats, 'problems' => $this->problems,
            'run_log' => $this->runLogPath, 'error_log' => $this->errorLogPath];
    }

    // =========================================================================
    //  One visit
    // =========================================================================

    private function _importVisit(
        array &$tracking,
        string $trackPath,
        array $v,
        string $lk,
        string $tag,
        string $fp,
        string $profile,
        bool $createPic,
        bool $validateOnly,
        int $interval,
        int $timeout
    ): void {
        $flags = ['verbose'];
        if ($validateOnly) {
            $flags[] = 'validate_only';
        } elseif ($createPic) {
            $flags[] = 'create_pic';
        }
        $args = [
            'directory' => PngPaths::of($this->projectPath, PngPaths::LORISID_SUBDIR),
            'subject'   => 'sub-' . $v['pscid'],
            'visit'     => 'ses-' . $v['visit_label'],
            'profile'   => $profile,
        ];

        $this->_log("  {$tag}: " . ($validateOnly ? 'validating' : 'importing') . ' ' . count($v['images'] ?? [])
            . ' image(s)');
        $launch = $this->_post(self::ENDPOINT, ['args' => $args, 'flags' => $flags, 'async' => true]);
        $http   = (int) ($launch['http_status'] ?? 0);

        if ($http !== 202 || empty($launch['job_id'])) {
            $reason = match (true) {
                $http === 0   => 'LORIS did not answer: ' . ($launch['errors'][0]['message'] ?? 'connection error'),
                $http === 401 => 'LORIS refused the token (401)',
                $http === 403 => 'the API user lacks the permission for /cbigr_api/script (api_script_execution)',
                $http === 404 => 'LORIS has no /cbigr_api/script/importpngdataset endpoint - deploy the CBIGR change (docs/png.md)',
                $http === 400 => 'the endpoint rejected the request: ' . $this->_text($launch),
                default       => "HTTP {$http} from the endpoint: " . $this->_text($launch),
            };
            $this->_writeDetail($tag, $args, $flags, $launch);
            if (in_array($http, [0, 401, 403, 404, 500], true)) {
                $this->abortReason = $reason;
            }
            $this->_fail($tracking, $lk, $tag, $reason, $trackPath, !$validateOnly);
            return;
        }

        $jobId = (int) $launch['job_id'];
        $this->_log("    launched - job_id={$jobId}");
        if (!$validateOnly) {
            $tracking[$lk] = array_merge($tracking[$lk] ?? [], ['status' => 'running', 'job_id' => $jobId,
                'started_at' => date('c'), 'timestamp' => date('c')]);
            $this->_saveTracking($trackPath, $tracking);
        }

        $status = $this->_waitForJob($jobId, $interval, $timeout);
        if ($status === null) {
            $msg = "job {$jobId} still running after {$timeout}s (or LORIS stopped answering) - recorded as "
                . 'running; the next run asks LORIS before doing anything with this visit';
            $this->_problem('warning', $tag, "STILL RUNNING {$tag}: {$msg}");
            $this->stats['visits_running']++;
            $this->results[$lk] = ['status' => 'running', 'job_id' => $jobId];
            return;
        }

        $this->_finish($tracking, $trackPath, $v, $lk, $tag, $fp, $jobId, $status, $args, $flags, $validateOnly);
    }

    /**
     * Record what a finished job did.
     */
    private function _finish(
        array &$tracking,
        string $trackPath,
        array $v,
        string $lk,
        string $tag,
        string $fp,
        int $jobId,
        array $status,
        array $args,
        array $flags,
        bool $validateOnly
    ): void {
        $state  = (string) ($status['state'] ?? 'UNKNOWN');
        $output = $this->_output($status);
        $exit   = isset($status['exit_code']) && $status['exit_code'] !== null ? (int) $status['exit_code'] : null;
        $parsed = self::parseOutput($output);

        // UNKNOWN: SPM could not read the exit code file. Seen RUNNING before,
        // or the script's own success summary is there: the file was cleaned
        // up after a successful exit (same rule as the DICOM import).
        if ($state === 'UNKNOWN' && $exit === null
            && (!empty($status['was_running'])
                || ($parsed['summary'] !== null && $parsed['summary']['failed'] === 0))
        ) {
            $state = 'SUCCESS';
        }

        if ($state === 'SUCCESS' && ($exit === null || $exit === 0)) {
            if ($validateOnly) {
                $this->_log("    VALID - LORIS found no error" . ($parsed['warnings'] ? ' (' . count($parsed['warnings'])
                    . ' warning(s))' : ''));
                foreach ($parsed['warnings'] as $w) {
                    $this->_problem('warning', $tag, "{$tag}: LORIS warning: {$w}");
                }
                $this->results[$lk] = ['status' => 'valid'];
                $this->stats['visits_imported']++;
                return;
            }
            $s = $parsed['summary'] ?? ['inserted' => count($parsed['inserted']), 'already' => count($parsed['already']),
                'failed' => 0, 'validated' => count($v['images'] ?? [])];
            $this->_log(sprintf('    SUCCESS - %d inserted, %d already in LORIS (skipped)', $s['inserted'], $s['already']));
            foreach ($parsed['already'] as $a) {
                $this->_log("      already in LORIS: {$a['image']} = FileID {$a['file_id']} ({$a['path']})");
            }
            $this->stats['images_inserted'] += $s['inserted'];
            $this->stats['images_already']  += $s['already'];
            $this->stats['visits_imported']++;
            $retired = $this->_retired($tag, $v, $tracking[$lk] ?? []);
            $tracking[$lk] = [
                'status'      => 'success',
                'fingerprint' => $fp,
                'job_id'      => $jobId,
                'timestamp'   => date('c'),
                'inserted'    => $s['inserted'],
                'already'     => $s['already'],
                'images'      => $this->_imageIds($v, $parsed, $tracking[$lk]['images'] ?? []),
                'file_ids'    => array_values(array_unique(array_merge(
                    array_column($parsed['inserted'], 'file_id'),
                    array_column($parsed['already'], 'file_id')
                ))),
                'retired'     => $retired,
            ];
            $this->_saveTracking($trackPath, $tracking);
            $this->results[$lk] = ['status' => 'success', 'inserted' => $s['inserted'], 'already' => $s['already']];
            return;
        }

        // --- ERROR ------------------------------------------------------------------
        foreach (self::ENVIRONMENT_PATTERNS as $pattern => $meaning) {
            if (preg_match($pattern, $output)) {
                $this->abortReason = $meaning;
                break;
            }
        }
        // A crash (Python traceback) outside the script's own checks: the
        // same thing will happen to every visit - stop after this one.
        if ($this->abortReason === null && str_contains($output, 'Traceback (most recent call last)')
            && !in_array($exit, [self::EXIT_INVALID_IMPORT, self::EXIT_INSERT_FAILURE], true)
        ) {
            $this->abortReason = 'import_png_dataset.py crashed on the LORIS host (' . $this->_lastLine($output)
                . ') - see the error log';
        }

        $reason = match (true) {
            $exit === self::EXIT_INVALID_IMPORT => 'LORIS validation failed, nothing inserted: '
                . ($parsed['errors'] !== [] ? implode(' | ', $parsed['errors']) : $this->_firstLine($output)),
            $exit === self::EXIT_INSERT_FAILURE => sprintf('%d image(s) inserted, %d failed - retried next run '
                . '(inserted ones are skipped)', $parsed['summary']['inserted'] ?? count($parsed['inserted']),
                $parsed['summary']['failed'] ?? 0),
            $exit === self::EXIT_INVALID_PATH   => 'LORIS cannot see the folder ' . $args['directory']
                . ' - it must be readable by the web server user at the same path on the LORIS host',
            default => "job {$jobId} ended {$state}" . ($exit !== null ? " (exit {$exit})" : '') . ': '
                . (str_contains($output, 'Traceback (most recent call last)')
                    ? $this->_lastLine($output) : $this->_firstLine($output)),
        };
        $reason .= $this->_advice($output);

        if ($exit === self::EXIT_INSERT_FAILURE) {
            $this->stats['images_inserted'] += (int) ($parsed['summary']['inserted'] ?? count($parsed['inserted']));
            $this->stats['images_failed']   += (int) ($parsed['summary']['failed'] ?? 0);
        }

        $this->_writeDetail($tag, $args, $flags, $status + ['job_id' => $jobId]);
        $this->_fail($tracking, $lk, $tag, $reason, $validateOnly ? null : $trackPath, !$validateOnly, [
            'job_id'   => $jobId,
            'images'   => $this->_imageIds($v, $parsed, $tracking[$lk]['images'] ?? []),
        ]);
    }

    /**
     * A job recorded as running by an earlier run. True when this visit is
     * settled for this run (still running, or now recorded as success).
     */
    private function _resolveRunning(array &$tracking, string $lk, string $tag, array $was, string $fp): bool
    {
        $jobId  = (int) $was['job_id'];
        $status = $this->_poll($jobId);
        $state  = $status['state'] ?? ($status === null ? null : 'UNKNOWN');

        if ($state === 'RUNNING') {
            $this->_problem('warning', $tag, "STILL RUNNING in LORIS {$tag} - job {$jobId}; left alone, the next run checks it again");
            $this->stats['visits_running']++;
            $this->results[$lk] = ['status' => 'running', 'job_id' => $jobId];
            return true;
        }
        if ($state === 'SUCCESS' && ($was['fingerprint'] ?? $fp) === $fp) {
            $parsed = self::parseOutput($this->_output($status));
            $this->_log("  {$tag}: job {$jobId} from an earlier run finished - SUCCESS, recorded");
            $tracking[$lk] = array_merge($was, ['status' => 'success', 'fingerprint' => $fp, 'timestamp' => date('c'),
                'file_ids' => array_values(array_unique(array_merge(
                    array_column($parsed['inserted'], 'file_id'), array_column($parsed['already'], 'file_id'))))]);
            $this->_saveTracking(PngPaths::of($this->projectPath, PngPaths::IMPORT_TRACKING), $tracking);
            $this->stats['visits_skipped']++;
            $this->results[$lk] = ['status' => 'skipped'];
            return true;
        }
        if ($state === null) {
            $age = time() - (int) strtotime((string) ($was['started_at'] ?? 'now'));
            if ($age < self::LOST_JOB_HOURS * 3600) {
                $this->_log("  {$tag}: job {$jobId} cannot be checked now (LORIS not answering) - left for the next run");
                $this->stats['visits_running']++;
                return true;
            }
        }
        // ERROR, NOT_FOUND, lost: retried now. Safe: images already inserted
        // are skipped by hash.
        $this->_log("  {$tag}: job {$jobId} from an earlier run ended " . ($state ?? 'LOST') . ' - retrying');
        $tracking[$lk]['status'] = 'failed';
        return false;
    }

    // =========================================================================
    //  Output parsing (import_png_dataset.py messages)
    // =========================================================================

    /**
     * What a run of import_png_dataset.py said.
     *
     * @return array{
     *   summary: ?array{inserted: int, already: int, failed: int, validated: int},
     *   inserted: array<int, array{image: string, file_id: int, path: string}>,
     *   already: array<int, array{image: string, file_id: int, path: string}>,
     *   errors: string[], warnings: string[]
     * }
     */
    public static function parseOutput(string $text): array
    {
        $out = ['summary' => null, 'inserted' => [], 'already' => [], 'errors' => [], 'warnings' => []];

        if (preg_match('/(\d+) file\(s\) inserted, (\d+) already registered, (\d+) failed, out of (\d+) validated/',
            $text, $m)) {
            $out['summary'] = ['inserted' => (int) $m[1], 'already' => (int) $m[2], 'failed' => (int) $m[3],
                'validated' => (int) $m[4]];
        }
        if (preg_match_all('/^(\S+\.png) inserted as FileID (\d+) \(([^)]+)\)/mi', $text, $mm, PREG_SET_ORDER)) {
            foreach ($mm as $m) {
                $out['inserted'][] = ['image' => $m[1], 'file_id' => (int) $m[2], 'path' => $m[3]];
            }
        }
        if (preg_match_all('/(\S+\.png) is already registered in the files table as\s+(\S+) \(FileID (\d+)\)/i',
            $text, $mm, PREG_SET_ORDER)) {
            foreach ($mm as $m) {
                $out['already'][] = ['image' => $m[1], 'file_id' => (int) $m[3], 'path' => $m[2]];
            }
        }
        // Validation report lines: "  ERROR    sub-X/ses-Y/echo_001.png: message"
        if (preg_match_all('/^\s+(ERROR|WARNING)\s{2,}(.+)$/m', $text, $mm, PREG_SET_ORDER)) {
            foreach ($mm as $m) {
                $out[$m[1] === 'ERROR' ? 'errors' : 'warnings'][] = trim($m[2]);
            }
        }
        // Errors logged on their own (one image failing to insert, etc.).
        if (preg_match_all('/^ERROR: (?!\d+ error\(s\))(.+)$/m', $text, $mm)) {
            foreach ($mm[1] as $line) {
                if (!str_contains($line, 'file(s) inserted')) {
                    $out['errors'][] = trim($line);
                }
            }
        }
        $out['errors']   = array_values(array_unique($out['errors']));
        $out['warnings'] = array_values(array_unique($out['warnings']));

        return $out;
    }

    /**
     * Per image of the visit: its LORIS FileID and path, from this run's
     * output, keeping what earlier runs found.
     */
    private function _imageIds(array $v, array $parsed, array $previous): array
    {
        $byName = [];
        foreach (array_merge($parsed['inserted'], $parsed['already']) as $r) {
            $byName[basename($r['image'])] = $r;
        }
        $prev = [];
        foreach ($previous as $p) {
            $prev[$p['original_name'] ?? ''] = $p;
        }
        $out = [];
        foreach ($v['images'] ?? [] as $img) {
            $name = $img['original_name'];
            $hit  = $byName[$name] ?? null;
            $out[] = [
                'original_name' => $name,
                'original_path' => $img['original_path'],
                'md5'           => $img['md5'],
                'loris_file_id' => $hit['file_id'] ?? ($prev[$name]['loris_file_id'] ?? null),
                'loris_path'    => $hit['path'] ?? ($prev[$name]['loris_path'] ?? null),
            ];
        }
        return $out;
    }

    /**
     * Images ingested for this visit before that are no longer part of it
     * (removed from the delivery, or replaced by new content under the same
     * name). LORIS keeps them - nothing is deleted there automatically - so
     * each is named once, with its FileID, and remembered under 'retired'.
     *
     * @return array<int, array<string, mixed>> all retired images of the visit
     */
    private function _retired(string $tag, array $v, array $previous): array
    {
        $now = [];
        foreach ($v['images'] ?? [] as $img) {
            $now[$img['original_name']] = $img['md5'];
        }
        $retired = (array) ($previous['retired'] ?? []);
        foreach ((array) ($previous['images'] ?? []) as $old) {
            if (empty($old['loris_file_id'])) {
                continue;
            }
            $name = (string) $old['original_name'];
            $why  = !isset($now[$name]) ? 'no longer delivered'
                : ($now[$name] !== $old['md5'] ? 'replaced by new content with the same name' : null);
            if ($why === null) {
                continue;
            }
            $this->_problem('warning', $tag, "STILL IN LORIS {$tag}/{$name}: {$why} - the earlier image is FileID "
                . "{$old['loris_file_id']} ({$old['loris_path']}); delete it in LORIS if it should not be there");
            $retired[] = $old + ['retired_at' => date('c'), 'reason' => $why];
        }
        return $retired;
    }

    /**
     * Fix hints for the messages a site or the LORIS team can act on.
     */
    private function _advice(string $output): string
    {
        $hints = [];
        if (preg_match_all("/scan type '([^']+)' has no entry in the mri_scan_type table/", $output, $m)) {
            $hints[] = 'add scan type(s) ' . implode(', ', array_unique($m[1])) . ' to mri_scan_type in LORIS '
                . '(docs/png.md), or remove them from png.scan_types';
        }
        if (str_contains($output, 'is not a visit label configured in LORIS')) {
            $hints[] = 'configure the visit label in LORIS';
        }
        if (str_contains($output, 'no session for CandID')) {
            $hints[] = 'the session is missing in LORIS - run run_png_reidentifier.php --force';
        }
        if (str_contains($output, 'no candidate in the candidate table with this PSCID')) {
            $hints[] = 'the candidate is missing in LORIS - run run_png_participant_sync.php';
        }
        return $hints === [] ? '' : ' -> ' . implode('; ', $hints);
    }

    // =========================================================================
    //  Checks
    // =========================================================================

    /**
     * Null when the reidentified folder holds exactly the images recorded,
     * byte for byte; otherwise why not.
     */
    private function _integrity(array $v): ?string
    {
        $want = [];
        foreach ($v['images'] ?? [] as $img) {
            $want[$img['original_name']] = $img['md5'];
        }
        $have = [];
        foreach (glob(rtrim((string) $v['directory'], '/') . '/*') ?: [] as $f) {
            if (is_file($f) && preg_match('/\.png$/i', $f)) {
                $have[basename($f)] = $f;
            }
        }
        $missing = array_diff_key($want, $have);
        $extra   = array_diff_key($have, $want);
        $changed = [];
        foreach (array_intersect_key($have, $want) as $name => $path) {
            if (md5_file($path) !== $want[$name]) {
                $changed[] = $name;
            }
        }
        if ($missing === [] && $extra === [] && $changed === []) {
            return null;
        }
        return 'the reidentified copy no longer matches what was reidentified ('
            . trim(($missing ? 'missing: ' . implode(', ', array_keys($missing)) . ' ' : '')
                . ($extra ? 'not reidentified: ' . implode(', ', array_keys($extra)) . ' ' : '')
                . ($changed ? 'changed: ' . implode(', ', $changed) : ''))
            . ') - nothing sent; run run_png_reidentifier.php --force to rebuild it';
    }

    /**
     * True: session exists. False: LORIS says 404. Null: could not tell (the
     * import then reports its own error if the session really is missing).
     */
    private function _sessionExists(string $candId, string $visit): ?bool
    {
        try {
            $r = $this->http->get("api/{$this->apiVersion}/candidates/" . rawurlencode($candId) . '/' . rawurlencode($visit), [
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
                'timeout' => 15,
            ]);
            $code = $r->getStatusCode();
            return $code === 200 ? true : ($code === 404 ? false : null);
        } catch (\Throwable $e) {
            return null;
        }
    }

    // =========================================================================
    //  HTTP
    // =========================================================================

    private function _authenticate(): bool
    {
        $api      = $this->config['loris'] ?? $this->config['api'] ?? [];
        $username = (string) ($api['username'] ?? '');
        $password = (string) ($api['password'] ?? '');
        $this->_log("Authenticating ({$this->apiVersion})...");
        try {
            $resp = $this->http->post("api/{$this->apiVersion}/login", ['json' => compact('username', 'password')]);
            $body = json_decode((string) $resp->getBody(), true);
            $this->token = is_array($body) ? (string) ($body['token'] ?? '') : '';
            if ($resp->getStatusCode() !== 200 || $this->token === '') {
                $this->_log('  Authentication failed: HTTP ' . $resp->getStatusCode());
                return false;
            }
            $this->_log('  Authenticated');
            return true;
        } catch (\Throwable $e) {
            $this->_log('  Authentication failed: ' . $e->getMessage());
            return false;
        }
    }

    private function _post(string $endpoint, array $payload): array
    {
        try {
            $resp = $this->http->post($endpoint, [
                'json'    => $payload,
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
                'timeout' => 60,
            ]);
            $raw  = (string) $resp->getBody();
            $body = json_decode($raw, true);
            if (!is_array($body)) {
                $body = ['raw_body' => $raw];
            }
            $body['http_status'] = $resp->getStatusCode();
            return $body;
        } catch (\Throwable $e) {
            return ['http_status' => 0, 'errors' => [['type' => 'exception', 'message' => $e->getMessage()]]];
        }
    }

    private function _poll(int $jobId): ?array
    {
        try {
            $resp = $this->http->get("cbigr_api/script/job/{$jobId}", [
                'headers' => ['Authorization' => 'Bearer ' . $this->token],
                'timeout' => 15,
            ]);
            if ($resp->getStatusCode() === 404) {
                return ['state' => 'NOT_FOUND'];
            }
            $body = json_decode((string) $resp->getBody(), true);
            return is_array($body) ? $body : null;
        } catch (\Throwable $e) {
            return null;
        }
    }

    /**
     * Poll until the job leaves RUNNING; null on timeout or when LORIS stops
     * answering for too long.
     */
    private function _waitForJob(int $jobId, int $interval, int $timeout): ?array
    {
        $t0       = microtime(true);
        $failures   = 0;
        $sleep      = 1;
        $wasRunning = false;
        while (microtime(true) - $t0 < $timeout) {
            sleep($sleep);
            $sleep  = min($interval, $sleep * 2);
            $status = $this->_poll($jobId);
            if ($status === null) {
                if (++$failures >= 10) {
                    return null;
                }
                continue;
            }
            $failures = 0;
            $state    = (string) ($status['state'] ?? 'UNKNOWN');
            if ($this->verbose) {
                $this->_log(sprintf('    [%ds] job %d -> %s', (int) (microtime(true) - $t0), $jobId, $state));
            }
            if ($state !== 'RUNNING') {
                return $status + ['was_running' => $wasRunning];
            }
            $wasRunning = true;
        }
        return null;
    }

    private function _output(array $status): string
    {
        $parts = [];
        foreach (['stdout', 'stderr', 'error_detail', 'progress', 'output'] as $k) {
            if (!empty($status[$k]) && is_string($status[$k]) && !in_array($status[$k], $parts, true)) {
                $parts[] = $status[$k];
            }
        }
        return implode("\n", $parts);
    }

    // =========================================================================
    //  Tracking, logs, summary
    // =========================================================================

    private function _fail(array &$tracking, string $lk, string $tag, string $reason, ?string $trackPath,
        bool $record, array $extra = []): void
    {
        $this->_problem('error', $tag, "FAILED {$tag}: {$reason}");
        $this->stats['visits_failed']++;
        $this->results[$lk] = ['status' => 'failed', 'reason' => $reason];
        if ($record && $trackPath !== null) {
            // No fingerprint stored: a failed visit is always retried.
            $tracking[$lk] = array_merge(['images' => $tracking[$lk]['images'] ?? []], $extra, [
                'status' => 'failed', 'detail' => $reason, 'timestamp' => date('c')]);
            $this->_saveTracking($trackPath, $tracking);
        }
    }

    /** @var array<string, string> file => generated_at already carried into an email */
    private array $carried = [];

    private function _saveTracking(string $path, array $tracking): void
    {
        ksort($tracking, SORT_STRING);
        PngPaths::writeJson($path, ['updated_at' => date('c'), 'carried' => $this->carried, 'visits' => $tracking]);
    }

    private function _status(bool $validateOnly): string
    {
        $failed = $this->stats['visits_failed'];
        $ok     = $this->stats['visits_imported'];
        $warn   = array_filter($this->problems, static fn ($p) => $p['level'] === 'warning') !== [];
        if ($this->dryRun && !$validateOnly) {
            return 'DRY RUN';
        }
        if ($this->abortReason !== null && $ok === 0) {
            return 'FAILED';
        }
        if ($failed > 0) {
            return $ok > 0 ? 'PARTIAL' : 'FAILED';
        }
        if ($ok === 0 && $this->stats['visits_skipped'] === 0 && !$warn) {
            return 'NOTHING TO DO';
        }
        return $warn ? 'SUCCESS (warnings)' : 'SUCCESS';
    }

    private function _summary(string $status, bool $validateOnly): void
    {
        $s = $this->stats;
        $this->_log('');
        $this->_log(sprintf('Result: %s - %d visit(s): %d %s, %d skipped (unchanged), %d failed, %d no session, '
            . '%d still running, %d excluded', $status, $s['visits_found'], $s['visits_imported'],
            $validateOnly ? 'valid' : 'imported', $s['visits_skipped'], $s['visits_failed'],
            $s['visits_no_session'], $s['visits_running'], $s['visits_excluded']));
        if (!$validateOnly && !$this->dryRun) {
            $this->_log(sprintf('Images: %d inserted, %d already in LORIS, %d failed',
                $s['images_inserted'], $s['images_already'], $s['images_failed']));
        }
        $notes = array_filter($this->problems, static fn ($p) => !empty($p['carried']));
        if ($notes !== []) {
            $this->_log('Also reported (from the delivery and the earlier steps; in the email):');
            foreach ($notes as $p) {
                $this->_log('  - ' . $p['message']);
            }
        }
        if ($this->runLogPath !== null) {
            $this->_log("Run log: {$this->runLogPath}");
        }
        if ($this->errorLogPath !== null && $this->stats['visits_failed'] > 0) {
            $this->_log("Errors (full LORIS output): {$this->errorLogPath}");
        }
    }

    private function _notify(string $status): void
    {
        $project = PngPaths::readJson(PngPaths::of($this->projectPath, 'project.json'));
        $notif   = $project['notification_emails']['png'] ?? $this->config['notification_emails']['png']
            ?? $this->config['notification_defaults'] ?? [];
        if (!($notif['enabled'] ?? false) || $status === 'NOTHING TO DO') {
            return;
        }
        $ok = str_starts_with($status, 'SUCCESS');
        $to = $ok ? ($notif['on_success'] ?? []) : ($notif['on_error'] ?? $notif['default_on_error'] ?? []);
        $name = basename($this->projectPath);
        $s    = $this->stats;
        $body = "Project: {$name} (png)\nStatus:  {$status}\n\n"
            . sprintf("Visits: %d imported, %d unchanged, %d failed, %d no session, %d still running\n",
                $s['visits_imported'], $s['visits_skipped'], $s['visits_failed'], $s['visits_no_session'], $s['visits_running'])
            . sprintf("Images: %d inserted, %d already in LORIS, %d failed\n\n",
                $s['images_inserted'], $s['images_already'], $s['images_failed']);
        foreach ($this->problems as $p) {
            $body .= strtoupper($p['level']) . ': ' . self::_cut($p['message'], 0, 400, '...') . "\n";
        }
        $body .= $this->runLogPath ? "\nRun log: {$this->runLogPath}\n" : '';
        $mailer = new Notification();
        foreach ((array) $to as $address) {
            $mailer->send((string) $address, "{$status}: {$name} PNG Import", $body);
        }
    }

    private function _openLogs(): void
    {
        $dir = $this->dryRun ? null : PipelineSetup::logDir($this->projectPath, 'png');
        if ($dir === null) {
            return;
        }
        $ts = date('Y-m-d_H-i-s');
        $this->runLogPath   = "{$dir}/png_import_run_{$ts}.log";
        $this->errorLogPath = "{$dir}/png_import_errors_{$ts}.log";
        $this->runLog = @fopen($this->runLogPath, 'a') ?: null;
    }

    private function _closeLogs(): void
    {
        foreach ([$this->runLog, $this->errorLog] as $fh) {
            if (is_resource($fh)) {
                fclose($fh);
            }
        }
        if ($this->errorLogPath !== null && !is_file($this->errorLogPath)) {
            $this->errorLogPath = null;
        }
    }

    /**
     * Everything sent and received for a failed visit, untruncated.
     */
    private function _writeDetail(string $tag, array $args, array $flags, array $response): void
    {
        if ($this->errorLogPath === null) {
            return;
        }
        $this->errorLog ??= @fopen($this->errorLogPath, 'a') ?: null;
        if (!is_resource($this->errorLog)) {
            return;
        }
        $lines = ["=== {$tag}  " . date('c'), 'args: ' . json_encode($args, JSON_UNESCAPED_SLASHES),
            'flags: ' . implode(',', $flags)];
        foreach (['http_status', 'job_id', 'state', 'exit_code', 'end_time'] as $k) {
            if (isset($response[$k]) && $response[$k] !== '') {
                $lines[] = "{$k}: " . (is_scalar($response[$k]) ? (string) $response[$k] : json_encode($response[$k]));
            }
        }
        if (!empty($response['errors'])) {
            $lines[] = 'errors: ' . json_encode($response['errors'], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        }
        $seen = [];
        foreach (['stdout', 'stderr', 'error_detail', 'progress', 'output', 'raw_body'] as $k) {
            if (!empty($response[$k]) && is_string($response[$k]) && !isset($seen[$response[$k]])) {
                $seen[$response[$k]] = true;
                $lines[] = "{$k}:\n    " . str_replace("\n", "\n    ", rtrim($response[$k]));
            }
        }
        fwrite($this->errorLog, implode("\n", $lines) . "\n\n");
    }

    private function _problem(string $level, ?string $visit, string $message): void
    {
        $this->problems[] = ['level' => $level, 'visit' => $visit, 'message' => $message];
        $this->_log('    ' . ($level === 'error' ? 'ERROR ' : 'WARNING ') . $message);
    }

    private function _log(string $line): void
    {
        ($this->out)($line);
        if (is_resource($this->runLog)) {
            fwrite($this->runLog, '[' . date('H:i:s') . "] {$line}\n");
        }
    }

    private function _text(array $r): string
    {
        $t = $r['error'] ?? $r['message'] ?? $r['raw_body'] ?? json_encode($r);
        return self::_cut(is_string($t) ? $t : (string) json_encode($t), 0, 300, '...');
    }

    /** Shorten for a console line or an email (no mbstring needed). */
    private static function _cut(string $text, int $start, int $width, string $marker = '...'): string
    {
        $text = substr($text, $start);
        return strlen($text) <= $width ? $text : substr($text, 0, $width - strlen($marker)) . $marker;
    }

    private function _lastLine(string $text): string
    {
        $lines = array_values(array_filter(array_map('trim', preg_split('/\R/', $text) ?: []),
            static fn ($l) => $l !== '' && !preg_match('/^(exit \d+|RUNNING)$/', $l)));
        return self::_cut((string) end($lines), 0, 300, '...');
    }

    private function _firstLine(string $text): string
    {
        foreach (preg_split('/\R/', $text) ?: [] as $l) {
            if (preg_match('/ERROR:|Error|Traceback|failed/i', $l)) {
                return self::_cut(trim($l), 0, 300, '...');
            }
        }
        return self::_cut(trim($text), 0, 300, '...');
    }
}
