<?php

declare(strict_types=1);

namespace LORIS\Validation;

use LORIS\Pipelines\Png\PngCli;
use LORIS\Pipelines\Png\PngDelivery;
use LORIS\Pipelines\Shared\ProjectLock;
use LORIS\Pipelines\Png\PngOverview;
use LORIS\Pipelines\Png\PngPaths;
use LORIS\Pipelines\Png\PngPreflight;

/**
 * ARCHIMEDES PNG validation (scripts/test/validate_png.php): cross-checks what a site shared before anything
 * is ingested - the counterpart of RedcapValidator for PNG images.
 *
 * Checks the shared folder against the "Preparing PNG Images for Submission"
 * user guide and against the project's settings, with exactly the rules the
 * PNG pipeline applies (it runs the pipeline's own scan and pre-flight:
 * PngDelivery, PngPreflight, PngOverview), so the pre-check and the pipeline
 * can never disagree:
 *
 *   Project settings  project.json: modalities (Imaging), candidate_defaults
 *                     (project, site, cohort), visits, png.scan_types,
 *                     unfilled template values
 *   Folder            deidentified-raw/imaging/png/ exists; entries outside
 *                     the sub-<ID>/ses-<visit>/ layout; nested sub- folders
 *   participants.tsv  tab-separated (not CSV / Excel), columns, duplicate or
 *                     empty IDs, sex and dob usable, site and cohort in
 *                     project.json; rows without a folder and folders
 *                     without a row (both directions)
 *   each participant  identifier; sessions file present, readable, with
 *                     session_id; every ses- folder listed and every row a
 *                     folder; acq_date a real YYYY-MM-DD date; visit labels
 *                     (letters and digits, case clashes, in project.json
 *                     "visits"); every image: type agreed, really a PNG, not
 *                     empty, not a duplicate; subfolders and other files
 *   LORIS (read-only) the project and each site exist in LORIS; each
 *                     participant already in LORIS still has the same dob
 *                     and sex in participants.tsv (ingestion never changes
 *                     LORIS) - skipped with --offline
 *   Summary           what ingestion will use: project, sites, cohorts,
 *                     visits, scan types found, one line per participant
 *                     (with its LORIS status: new / PSCID / differs)
 *   Environment       the project lock works on this mount; output folders
 *                     writable; LORIS reachable
 *
 * LORIS is only read: login, GET /projects, GET /sites, GET /candidates,
 * GET /candidates/{CandID} and the ExternalID mapper lookup. Nothing is
 * created or changed; no EviData or email calls.
 *
 * Severities, as RedcapValidator:
 *   ERROR    the pipeline would not ingest it (a participant, a visit, or
 *            the whole delivery); fix before ingestion
 *   WARNING  ingestion goes ahead, but some images are left out or something
 *            is not as intended
 *   NOTE     information only
 *
 * PHP Version 8.1
 */
final class PngValidator
{
    public const ERROR   = 'ERROR';
    public const WARNING = 'WARNING';
    public const NOTE    = 'NOTE';

    /** @var array<int, array{scope: string, file: string, summary: string, issues: array}> */
    private array $sections = [];

    /** @var array<string, int> section key => index */
    private array $index = [];

    /**
     * Validate a project folder.
     *
     * @param string      $projectPath Project root.
     * @param array|null  $project     project.json (null: missing or invalid).
     * @param string      $note        Why project.json is null.
     * @param string|null $pngDir      Another folder holding the PNG delivery (default
     *                                 deidentified-raw/imaging/png).
     * @param array|null  $config      loris_client_config.json; null = no LORIS checks.
     *
     * @return array<int, array{scope: string, file: string, summary: string, issues: array}>
     */
    public function validateProjectFolder(
        string $projectPath,
        ?array $project,
        string $note = '',
        ?string $pngDir = null,
        ?array $config = null
    ): array
    {
        $this->sections = [];
        $this->index    = [];
        $dir = rtrim($pngDir ?? PngPaths::of($projectPath, PngPaths::RAW_SUBDIR), '/');

        $this->_section('settings', 'Project settings', 'project.json', '');
        if ($project === null) {
            $this->_add('settings', self::ERROR, "project.json cannot be used: {$note}",
                'add project.json to the project folder (see config/project.example.json)');
            return $this->sections;
        }
        $this->_checkSettings($project);

        $this->_section('folder', 'Folder', $dir, '');
        if (!is_dir($dir)) {
            $this->_add('folder', self::ERROR, 'the PNG folder does not exist - nothing was shared, or it is somewhere else',
                'the site shares its images in <project>/deidentified-raw/imaging/png/ (user guide section 1)');
            return $this->sections;
        }

        $manifest = PngDelivery::scan($projectPath, $project, $dir);
        $pre      = PngPreflight::check($manifest, $project,
            static fn (): array => array_values(array_filter(array_map('strval', (array) ($project['visits'] ?? [])))));
        $this->_section('tsv', 'participants.tsv', $manifest['participants_tsv'] ?? "{$dir}/participants.tsv",
            $this->_tsvSummary($manifest));

        // Participants get a section each, in folder order.
        foreach ($manifest['visits'] as $v) {
            $this->_participantSection((string) $v['subject'], $manifest);
        }

        // --- what the scan found -----------------------------------------------
        foreach ($manifest['problems'] as $p) {
            $sev = match (true) {
                ($p['level'] ?? '') === 'error'           => self::ERROR,
                str_contains($p['message'], 'SET ASIDE')  => self::WARNING,
                default                                   => self::WARNING,
            };
            $msg = ($p['where'] ?? '') . ': ' . preg_replace('/^SET ASIDE - /', 'NOT LOADED - ', $p['message']);
            $this->_add($this->_keyFor($p), $sev, $msg, (string) ($p['fix'] ?? ''));
        }

        // --- participants.tsv values and visits (the pipeline's pre-flight) -------
        foreach ($pre['issues'] as $i) {
            if (($i['scope'] ?? '') === 'project' && $this->_alreadyReported($i['message'])) {
                continue;
            }
            if (in_array($i['scope'], ['subject', 'session', 'visit'], true) && $this->_alreadyReported($i['message'])) {
                continue;
            }
            [$msg, $fix] = array_pad(explode(' -> ', $i['message'], 2), 2, '');
            $key = isset($i['subject']) ? $this->_participantSection((string) $i['subject'], $manifest) : 'tsv';
            $this->_add($key, self::ERROR, $msg, $fix !== '' ? $fix : $this->_fixFor($msg));
        }
        foreach ($pre['warnings'] as $w) {
            if (str_starts_with($w, 'project.json has no "visits"')) {
                $this->_add('settings', self::WARNING, $w, '');
            }
        }

        // --- site and cohort against project.json (as the REDCap pre-check) -------
        $this->_checkSitesCohorts($manifest, $project);

        // --- not matched: nothing of it will be ingested -------------------------
        foreach ($manifest['visits'] as $v) {
            if ($v['link_status'] !== 'unlinked') {
                continue;
            }
            [$msg, $fix] = explode('; until then', PngDelivery::unlinkedAdvice($v['link_reason'], $v['subject'],
                $v['session']), 2) + [1 => ''];
            $parts = explode(' - ', $msg, 2);
            $this->_add($this->_participantSection((string) $v['subject'], $manifest), self::ERROR,
                "sub-{$v['subject']}/ses-{$v['session']}: NOT INGESTED ({$v['png_files']} image(s)) - {$parts[0]}",
                $parts[1] ?? '');
        }

        // --- LORIS (read-only) -----------------------------------------------------
        $loris = null;
        if ($config !== null && ($manifest['counts']['linked'] ?? 0) + ($manifest['counts']['blocked'] ?? 0) > 0) {
            try {
                $api   = PngCli::api($config, $project);
                $loris = PngOverview::collectLoris($api, $manifest, PngCli::rowLookup($manifest, $project));
            } catch (\Throwable $e) {
                $loris = ['projects' => null, 'sites' => null, 'existing' => [], 'error' => $e->getMessage()];
            }
        }

        // --- summary and environment (from the pipeline's overview) ---------------
        $overview = new PngOverview($manifest, $project, $loris, ProjectLock::probe($projectPath, 'png'));
        foreach ($overview->lorisFix() as $line) {
            if (str_starts_with($line, '[ENVIRONMENT]')) {
                continue;                               // reported under Environment below
            }
            [$msg, $fix] = array_pad(explode(' -> ', substr($line, 8), 2), 2, '');   // drop "[LORIS] "
            $key = preg_match('/\((sub-[A-Za-z0-9]+)[,)]/', $msg, $m) && isset($this->index[$m[1]]) ? $m[1]
                : (str_contains($msg, "project '") ? 'settings' : 'tsv');
            $this->_add($key, self::ERROR, 'LORIS: ' . $msg, $fix);
        }
        $this->_section('summary', 'Summary', 'what ingestion will use',
            sprintf('%d participant folder(s), %d visit(s), %d image(s) accepted, %d not loaded',
                count(array_unique(array_column($manifest['visits'], 'subject'))), $manifest['counts']['visits'],
                $manifest['counts']['images'], $manifest['counts']['set_aside']));
        foreach ($overview->sections() as $title => $rows) {
            if (str_starts_with($title, 'ENVIRONMENT')) {
                continue;
            }
            foreach ($rows as [$status, $text]) {
                if ($status === 'NOT CHECKED') {      // offline: the value itself is still worth showing
                    $status = 'INFO';
                    $text   = str_replace(' - LORIS not checked (--offline)', ' - LORIS not checked here', $text);
                }
                $this->_add('summary', self::NOTE, sprintf('%-12s %-11s %s', strtolower(strtok($title, ' ')),
                    "[{$status}]", $text), '');
            }
        }
        $this->_section('env', 'Environment', $projectPath, 'for the ARCHIMEDES team');
        foreach ($overview->sections() as $title => $rows) {
            if (!str_starts_with($title, 'ENVIRONMENT')) {
                continue;
            }
            foreach ($rows as [$status, $text]) {
                $this->_add('env', match ($status) {
                    'FIX'   => self::ERROR,
                    'CHECK' => self::WARNING,
                    default => self::NOTE,
                }, $text, $status === 'FIX' ? 'fix the folder permissions / mount options (docs/png.md, Concurrent runs)' : '');
            }
        }
        if ($config === null) {
            $this->_add('env', self::NOTE, 'LORIS not checked (--offline, or no readable config): project, sites and participants already in '
                . 'LORIS were not compared', '');
        } else {
            foreach ($overview->couldNotCheck() as $line) {
                [$msg, $fix] = array_pad(explode(' -> ', $line, 2), 2, '');
                $this->_add('env', self::WARNING, $msg, $fix);
            }
            if ($loris !== null && $loris['error'] === null) {
                $this->_add('env', self::NOTE, 'LORIS read: project, ' . count((array) $loris['sites']) . ' site(s) visible '
                    . 'to the API user, ' . count($loris['existing']) . ' participant(s) looked up by external ID', '');
            }
        }

        return $this->sections;
    }

    // =========================================================================
    //  Checks of project.json (the REDCap pre-check checks the same block)
    // =========================================================================

    private function _checkSettings(array $p): void
    {
        $mods = array_map('strtolower', array_map('strval', (array) ($p['modalities'] ?? [])));
        if (!in_array('imaging', $mods, true)) {
            $this->_add('settings', self::ERROR, 'modalities does not include "Imaging" - the PNG pipeline skips this project',
                'add "Imaging" to "modalities"');
        }
        $cd = (array) ($p['candidate_defaults'] ?? []);
        foreach (['project' => 'the exact LORIS project name', 'site' => 'a LORIS site name',
            'cohort' => 'a LORIS cohort'] as $k => $what) {
            $v = trim((string) ($cd[$k] ?? ''));
            if ($v === '') {
                $this->_add('settings', self::NOTE, "candidate_defaults.{$k} is not set - every participants.tsv row "
                    . "must then have a {$k} value", "set candidate_defaults.{$k} to {$what}, or fill the column");
            } elseif ($this->_placeholder($v)) {
                $this->_add('settings', self::ERROR, "candidate_defaults.{$k} is still the template value '{$v}'",
                    "set it to {$what}");
            }
        }
        foreach (['visits', 'cohorts', 'sites'] as $k) {
            foreach ((array) ($p[$k] ?? []) as $v) {
                if ($this->_placeholder((string) $v)) {
                    $this->_add('settings', self::ERROR, "\"{$k}\" still holds the template value '{$v}'",
                        "list the real {$k}, or remove the key");
                    break;
                }
            }
        }
        $types = (array) ($p['png']['scan_types'] ?? []);
        foreach ($types as $t) {
            if (!preg_match('/^[A-Za-z0-9]+$/', (string) $t)) {
                $this->_add('settings', self::ERROR, "png.scan_types has '{$t}': a type read from a file name can only be "
                    . 'letters and digits (the part before the first underscore)', 'rename the type, e.g. echo, ecg');
            }
        }
        $this->_add('settings', self::NOTE, sprintf('visits: %s; scan types: %s; cohorts: %s; sites: %s',
            $this->_list($p['visits'] ?? []), $this->_list($types), $this->_list($p['cohorts'] ?? []),
            $this->_list($p['sites'] ?? [])), '');
    }

    /**
     * participants.tsv site / cohort values that are not in project.json
     * "sites" / "cohorts" (when the project lists them): WARNING, as in the
     * REDCap pre-check - if LORIS does not have that cohort or site for the
     * project, the session (or the candidate) cannot be created.
     */
    private function _checkSitesCohorts(array $manifest, array $project): void
    {
        $tsv = $manifest['participants_tsv'] ?? null;
        if ($tsv === null) {
            return;
        }
        try {
            $roster = \LORIS\Pipelines\Shared\ParticipantsTsv::load($tsv, $project['candidate_defaults'] ?? [], $project);
        } catch (\Throwable $e) {
            return;                                     // reported by the scan
        }
        foreach (['site' => 'sites', 'cohort' => 'cohorts'] as $col => $key) {
            $list = array_values(array_filter(array_map('strval', (array) ($project[$key] ?? [])),
                fn ($x) => $x !== '' && !$this->_placeholder($x)));
            if ($list === []) {
                continue;
            }
            $other = [];
            foreach ($roster->all() as $pid => $row) {
                $v = trim((string) ($row[$col] ?? ''));
                if ($v !== '' && !in_array($v, $list, true)) {
                    $other[$v][] = (string) $pid;
                }
            }
            foreach ($other as $v => $pids) {
                $this->_add('tsv', self::WARNING, sprintf("%s '%s' (%s) is not in the project's %s list (%s). If LORIS "
                    . 'does not have it for this project, %s', $col, $v, implode(', ', array_slice($pids, 0, 10)), $col,
                    implode(', ', $list), $col === 'cohort' ? 'the session cannot be created and that visit is held back'
                        : 'the candidate cannot be created'),
                    "use one of: " . implode(', ', $list) . ", or add '{$v}' to project.json \"{$key}\" (and to LORIS)");
            }
        }
    }

    // =========================================================================
    //  Report (same layout as RedcapValidator)
    // =========================================================================

    /** @return array{ERROR:int, WARNING:int, NOTE:int} */
    public static function counts(array $sections): array
    {
        return ValidationReport::counts($sections);
    }

    /**
     * Same layout as the REDCap pre-check (shared ValidationReport).
     */
    public static function renderText(array $sections, string $title, string $runAt): string
    {
        return ValidationReport::renderText($sections, 'PNG validation report', $title, $runAt, [
            'ERROR   the pipeline would not ingest it (a participant, a visit, or the',
            '        whole delivery); fix before ingestion',
            'WARNING ingestion goes ahead, but some images are not loaded or something',
            '        is not as intended',
            'NOTE    information only',
        ], [
            'wrap'             => 90,
            'ok_ignores_notes' => true,
            'no_ok_scopes'     => ['Summary', 'Environment', 'Project settings'],
            'footer'           => [
                '',
                'Reminder (guide section 4): a PNG has no header that can be de-identified. Make sure no name,',
                'record number, date of birth, exam date/time, operator, machine or hospital name is visible in',
                'the images themselves - this cannot be checked automatically.',
            ],
        ]);
    }

    /**
     * One row per problem (ERROR / WARNING) - the list to send to the site.
     */
    public static function writeCsv(array $sections, string $path): bool
    {
        return ValidationReport::writeCsv($sections, $path, false);
    }

    // =========================================================================

    private function _section(string $key, string $scope, string $file, string $summary): void
    {
        if (!isset($this->index[$key])) {
            $this->sections[] = ['scope' => $scope, 'file' => $file, 'summary' => $summary, 'issues' => []];
            $this->index[$key] = count($this->sections) - 1;
        }
    }

    private function _add(string $key, string $severity, string $message, string $fix): void
    {
        if (!isset($this->index[$key])) {
            $this->_section($key, ucfirst($key), '', '');
        }
        $this->sections[$this->index[$key]]['issues'][] = ['severity' => $severity, 'message' => $message, 'fix' => $fix];
    }

    private function _participantSection(string $subject, array $manifest): string
    {
        $key = "sub-{$subject}";
        if (!isset($this->index[$key])) {
            $visits = array_values(array_filter($manifest['visits'], static fn ($v) => (string) $v['subject'] === $subject));
            $types  = [];
            foreach ($visits as $v) {
                foreach ($v['images'] as $i) {
                    $types[$i['scan_type']] = ($types[$i['scan_type']] ?? 0) + 1;
                }
            }
            ksort($types);
            $this->_section($key, 'Participant', "sub-{$subject}", sprintf('%d visit(s): %s; images: %s',
                count($visits), implode(', ', array_map(static fn ($v) => 'ses-' . $v['session'], $visits)) ?: 'none',
                $types ? implode(', ', array_map(static fn ($t, $n) => "{$t} {$n}", array_keys($types), $types)) : 'none'));
        }
        return $key;
    }

    /** Section key of a scan problem, from its "where". */
    private function _keyFor(array $p): string
    {
        $where = (string) ($p['where'] ?? '');
        if (isset($p['subject']) && $p['subject'] !== '' && str_starts_with($where, 'sub-')) {
            $key = 'sub-' . $p['subject'];
            if (!isset($this->index[$key])) {
                $this->_section($key, 'Participant', $key, '');
            }
            return $key;
        }
        return match (true) {
            $where === 'project.json'               => 'settings',
            str_starts_with($where, 'participants') => 'tsv',
            str_starts_with($where, 'sub-')         => $this->_bareSubjectKey($where),
            default                                 => 'folder',
        };
    }

    private function _bareSubjectKey(string $where): string
    {
        $key = explode('/', $where)[0];
        if (!isset($this->index[$key])) {
            $this->_section($key, 'Participant', $key, '');
        }
        return $key;
    }

    private function _alreadyReported(string $message): bool
    {
        $head = explode(' -> ', $message, 2)[0];
        foreach ($this->sections as $s) {
            foreach ($s['issues'] as $i) {
                if (str_contains($i['message'], $head)) {
                    return true;
                }
            }
        }
        return false;
    }

    private function _fixFor(string $msg): string
    {
        return match (true) {
            str_contains($msg, ' - fix it') => '',      // the message already says how
            str_contains($msg, 'sex')   => 'use Male, Female or Other (or a code in project.json sex_mappings)',
            str_contains($msg, 'dob')   => 'use YYYY-MM-DD, YYYY-MM or YYYY',
            default                     => '',
        };
    }

    private function _tsvSummary(array $manifest): string
    {
        $tsv = $manifest['participants_tsv'] ?? null;
        if ($tsv === null) {
            return 'missing';
        }
        $rows = max(0, count(file($tsv, FILE_SKIP_EMPTY_LINES) ?: []) - 1);
        return "{$rows} row(s)";
    }

    private function _placeholder(string $v): bool
    {
        return (bool) preg_match('/^<.*>$/', trim($v));
    }

    private function _list($values): string
    {
        $v = array_values(array_filter(array_map('strval', (array) $values)));
        return $v === [] ? 'not set' : implode(', ', $v);
    }
}
