<?php

declare(strict_types=1);

namespace LORIS\Validation;

use LORIS\Endpoints\LorisApiClientInterface;
use LORIS\Pipelines\Bids\BidsVisitCheck;
use LORIS\Pipelines\Shared\ParticipantsCheck;
use LORIS\Pipelines\Shared\ParticipantSync;
use LORIS\Pipelines\Shared\ParticipantsTsv;
use LORIS\Pipelines\Shared\PipelineSetup;
use LORIS\Pipelines\Shared\ProjectLock;
use LORIS\Utils\VisitLabel;

/**
 * BidsValidator
 *
 * The BIDS pre-check behind scripts/test/validate_bids.php: what a site shared
 * under deidentified-raw/bids/, checked with the rules the BIDS pipeline
 * applies (participant sync, reidentifier, import) and with the BIDS validator
 * Loris-MRI uses, BEFORE anything is ingested. Same report as the PNG and
 * REDCap pre-checks (ValidationReport).
 *
 * READ-ONLY: LORIS is only read (login, projects, sites, the ExternalID
 * mapper, candidates); nothing is created, copied or changed.
 *
 * Severities: ERROR - not ingested (a visit, a participant, or the whole
 * dataset when the participant sync would stop); WARNING - it goes in, but
 * something is left out or not as intended; NOTE - information.
 *
 * PHP Version 8.1
 */
final class BidsValidator
{
    public const ERROR   = ValidationReport::ERROR;
    public const WARNING = ValidationReport::WARNING;
    public const NOTE    = ValidationReport::NOTE;

    /** Where a site's BIDS dataset is expected, under the project folder. */
    public const DEFAULT_DIR = 'deidentified-raw/bids';

    private array $sections = [];
    private array $index    = [];

    /**
     * @param string      $projectPath Project folder.
     * @param array|null  $project     Decoded project.json, or null (see $note).
     * @param string      $note        Why project.json could not be read.
     * @param string|null $bidsDir     The dataset folder, if not the default.
     * @param array|null  $config      loris_client_config.json; null = offline.
     * @param string|null $pythonBin   Python with bids_validator (Loris-MRI's); null = not run.
     *
     * @return array Report sections (ValidationReport).
     */
    public function validateProjectFolder(
        string $projectPath,
        ?array $project,
        string $note,
        ?string $bidsDir,
        ?array $config,
        ?string $pythonBin
    ): array {
        $this->sections = [];
        $this->index    = [];
        $projectPath    = rtrim($projectPath, '/');
        $dir            = rtrim($bidsDir ?? "{$projectPath}/" . self::DEFAULT_DIR, '/');
        $p              = $project ?? [];

        // --- project settings ------------------------------------------------------
        $this->_section('settings', 'Project settings', 'project.json', '');
        if ($project === null) {
            $this->_add('settings', self::ERROR, "project.json: {$note}", 'add the project settings (BIDS guide section 2)');
        } else {
            if (!in_array('Imaging', (array) ($p['modalities'] ?? []), true)) {
                $this->_add('settings', self::ERROR, 'modalities does not list "Imaging" - the BIDS pipeline skips this project',
                    'add "Imaging" to "modalities"');
            }
            foreach (['project', 'site', 'cohort'] as $k) {
                if (trim((string) ($p['candidate_defaults'][$k] ?? '')) === '') {
                    $this->_add('settings', $k === 'project' ? self::ERROR : self::WARNING,
                        "candidate_defaults.{$k} is not set - each participants.tsv row must then give it",
                        "set candidate_defaults.{$k} (the exact LORIS name)");
                }
            }
            $visits = VisitLabel::declared($p);
            $this->_add('settings', self::NOTE, $visits === []
                ? 'no "visits" (optional): every ses- folder name is taken as the LORIS visit, as is'
                : 'visits: ' . implode(', ', $visits) . ' - each ses- folder must be one of them exactly', '');
        }

        // --- folder ------------------------------------------------------------------
        $this->_section('folder', 'Folder', $dir, '');
        if (!is_dir($dir)) {
            $this->_add('folder', self::ERROR, "the BIDS folder does not exist: {$dir}",
                'share the dataset there, or pass --bids-dir');
            return $this->sections;
        }
        $subjects = array_map('basename', glob("{$dir}/sub-*", GLOB_ONLYDIR) ?: []);
        sort($subjects);
        $this->sections[$this->index['folder']]['summary'] = count($subjects) . ' participant folder(s)';
        if ($subjects === []) {
            $this->_add('folder', self::ERROR, 'no sub-<ID> folders', 'one folder per participant (BIDS guide section 3)');
        }
        $desc = "{$dir}/dataset_description.json";
        if (!is_file($desc)) {
            $this->_add('folder', self::ERROR, 'no dataset_description.json at the top - the import cannot load the dataset',
                'add it with "Name" and "BIDSVersion"');
        } else {
            $d = json_decode((string) file_get_contents($desc), true);
            if (!is_array($d)) {
                $this->_add('folder', self::ERROR, 'dataset_description.json is not valid JSON: ' . json_last_error_msg(), 'fix the file');
            } else {
                foreach (['Name', 'BIDSVersion'] as $k) {
                    if (trim((string) ($d[$k] ?? '')) === '') {
                        $this->_add('folder', self::ERROR, "dataset_description.json has no \"{$k}\"", "add \"{$k}\"");
                    }
                }
            }
        }
        foreach (scandir($dir) ?: [] as $e) {
            if ($e[0] === '.' || str_starts_with($e, 'sub-') || is_file("{$dir}/{$e}")
                || in_array($e, ['derivatives', 'sourcedata', 'code', 'stimuli', 'phenotype'], true)
            ) {
                continue;
            }
            $this->_add('folder', self::WARNING, "folder {$e}/ at the top is not part of BIDS - it is not imported",
                'move its content into the participant folders, or remove it');
        }
        foreach (ParticipantSync::nestedSubjectDirs($dir) as $nested) {
            $this->_add('folder', self::ERROR, "a sub- folder inside another: {$nested}", 'move it to the top of the dataset');
        }
        foreach ($subjects as $sub) {
            $this->_containment($dir, $sub);
        }

        // --- participants.tsv --------------------------------------------------------
        $tsv = "{$dir}/participants.tsv";
        $this->_section('tsv', 'participants.tsv', $tsv, '');
        $roster = null;
        if (!is_file($tsv)) {
            $this->_add('tsv', self::ERROR, 'participants.tsv not found', 'add it at the top of the dataset (BIDS guide section 4)');
        } elseif (($why = ParticipantsTsv::formatProblem($tsv)) !== null) {
            $this->_add('tsv', self::ERROR, "participants.tsv {$why}", '');
        } else {
            try {
                $roster = ParticipantsTsv::load($tsv, $p['candidate_defaults'] ?? [], $p);
            } catch (\RuntimeException $e) {
                $this->_add('tsv', self::ERROR, $e->getMessage(), '');
            }
        }

        $sessions = [];
        foreach ($subjects as $sub) {
            $sessions[substr($sub, 4)] = array_map(
                static fn ($d) => substr(basename($d), 4),
                glob("{$dir}/{$sub}/ses-*", GLOB_ONLYDIR) ?: []
            );
        }

        if ($roster !== null) {
            $this->sections[$this->index['tsv']]['summary'] = $roster->count() . ' row(s)';
            foreach ($roster->notes() as $n) {
                $this->_add('tsv', self::WARNING, $n, 'correct participants.tsv');
            }
            $check = ParticipantsCheck::check($tsv, $p, $sessions,
                static fn (): array => VisitLabel::declared($p), array_keys($sessions), 'BIDS data');
            foreach ($check['issues'] as $i) {
                $this->_add($this->_keyFor($i['message']), self::ERROR, $i['message'], '');
            }
            foreach ($check['warnings'] as $w) {
                if (str_contains($w, 'VISIT MISMATCH') || str_contains($w, 'is not project.json candidate_defaults')) {
                    continue;   // reported below (visits) and above (notes)
                }
                $this->_add($this->_keyFor($w), self::WARNING, $w, '');
            }
            $orphans = ParticipantSync::orphans($roster, $dir);
            foreach ($orphans['dirs'] as $s) {
                $this->_add("sub-{$s}", self::ERROR, "sub-{$s}: folder with no participants.tsv row - the dataset would not go in",
                    'add the row (participant_id, dob, sex), or remove the folder');
            }
            // rows with no folder: reported by ParticipantsCheck above
            $this->_idPatterns(array_keys($roster->all()));

            // Loris-MRI bids_import creates each session with the row's cohort
            // (column 'cohort' or 'subproject'); the pipeline supplies it from
            // candidate_defaults.cohort when that is set.
            if (trim((string) ($p['candidate_defaults']['cohort'] ?? '')) === '') {
                foreach ($roster->all() as $pid => $row) {
                    if (trim((string) ($row['cohort'] ?? '')) === '') {
                        $this->_add('sub-' . ParticipantSync::subjectId((string) $pid), self::ERROR,
                            "{$pid}: no cohort - Loris-MRI cannot create its sessions",
                            'fill the cohort column, or set candidate_defaults.cohort');
                    }
                }
            }
        }

        // Data with no ses- folder: Loris-MRI files it under the LORIS
        // configuration default_bids_vl, and stops when that is not set.
        foreach ($sessions as $s => $visits) {
            if ($visits === [] && (glob("{$dir}/sub-{$s}/*", GLOB_ONLYDIR) ?: []) !== []) {
                $this->_add("sub-{$s}", self::WARNING, "sub-{$s}: data with no ses-<visit> folder - Loris-MRI files it"
                    . " under the LORIS setting default_bids_vl, and stops if that is not set",
                    'put the data in a ses-<visit> folder (BIDS guide section 3)');
            }
        }

        // --- visits: held back, the rest goes in ---------------------------------
        $heldAll = BidsVisitCheck::held($dir, $p);
        foreach ($heldAll as $sub => $held) {
            foreach ($held as $ses => $why) {
                $this->_add($sub, self::ERROR, "VISIT MISMATCH {$sub}/{$ses}: {$why} - HELD BACK (not imported; the rest goes in)", '');
            }
        }

        // --- the BIDS validator Loris-MRI uses ------------------------------------
        $this->_bidsValidator($dir, $pythonBin, $heldAll);

        // --- LORIS (read-only) ----------------------------------------------------
        $this->_section('loris', 'LORIS', $config === null ? 'not checked (--offline)' : 'read-only', '');
        if ($config !== null && $project !== null && $roster !== null) {
            $this->_loris($config, $p, $roster);
        }

        // --- summary and environment --------------------------------------------------
        $this->_section('summary', 'Summary', 'what ingestion will use', '');
        $this->_add('summary', self::NOTE, sprintf('project %s, site %s, cohort %s (cohort checked by LORIS when each session is created)',
            $p['candidate_defaults']['project'] ?? '-', $p['candidate_defaults']['site'] ?? '-',
            $p['candidate_defaults']['cohort'] ?? '-'), '');
        foreach ($sessions as $s => $visits) {
            $shown = array_map(static fn ($v) => isset($heldAll["sub-{$s}"]["ses-{$v}"]) ? "{$v} (held back)" : $v, $visits);
            $this->_add('summary', self::NOTE, "sub-{$s}: visits " . ($shown ? implode(', ', $shown) : 'none (no ses- folder)'), '');
        }
        $this->_section('env', 'Environment', $projectPath, 'for the ARCHIMEDES team');
        $lock = ProjectLock::probe($projectPath, 'bids');
        $this->_add('env', match ($lock['status'] ?? 'OK') {
            'FAIL'  => self::ERROR,
            'WARN'  => self::WARNING,
            default => self::NOTE,
        }, "project lock (one run at a time): {$lock['message']}", '');
        foreach (['processed', 'deidentified-lorisid', 'logs'] as $d) {
            $path = "{$projectPath}/{$d}";
            if (is_dir($path) && !is_writable($path)) {
                $this->_add('env', self::ERROR, "{$path} is not writable by " . get_current_user(), 'fix the permissions');
            }
        }

        return $this->sections;
    }

    public static function counts(array $sections): array
    {
        return ValidationReport::counts($sections);
    }

    public static function renderText(array $sections, string $title, string $runAt): string
    {
        return ValidationReport::renderText($sections, 'BIDS validation report', $title, $runAt, [
            'ERROR   the pipeline would not ingest it (a visit, a participant, or the whole',
            '        dataset); fix before ingestion - the rest goes in unless it is the dataset',
            'WARNING it goes in, but something is left out or not as intended',
            'NOTE    information only',
        ], [
            'wrap'             => 90,
            'ok_ignores_notes' => true,
            'no_ok_scopes'     => ['Summary', 'Environment', 'Project settings'],
            'footer'           => [
                '',
                'Reminder: check the image headers and sidecar JSON files carry no name, date of birth or other',
                'identifying value - this is not checked automatically.',
            ],
        ]);
    }

    public static function writeCsv(array $sections, string $path): bool
    {
        return ValidationReport::writeCsv($sections, $path, false);
    }

    // =========================================================================

    /** Files inside sub-X that belong to another participant. */
    private function _containment(string $dir, string $sub): void
    {
        $bad = [];
        $it  = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator("{$dir}/{$sub}", \FilesystemIterator::SKIP_DOTS));
        foreach ($it as $f) {
            $name = $f->getFilename();
            if ($f->isFile() && str_starts_with($name, 'sub-') && explode('_', $name)[0] !== $sub) {
                $bad[] = substr($f->getPathname(), strlen($dir) + 1);
            }
        }
        if ($bad !== []) {
            $this->_add($sub, self::ERROR, "{$sub}: " . count($bad) . ' file(s) named for another participant, e.g. ' . $bad[0]
                . ' - the dataset would not go in', 'move or rename them');
        }
    }

    /** Participant IDs following different patterns (sub-ARCHI0001 vs sub-P14). */
    private function _idPatterns(array $ids): void
    {
        $patterns = [];
        foreach ($ids as $pid) {
            $id = ParticipantSync::subjectId((string) $pid);
            $patterns[preg_replace('/\d/', '9', preg_replace('/[A-Za-z]/', 'A', $id))][] = $pid;
        }
        if (count($patterns) > 1) {
            $this->_add('tsv', self::WARNING, 'participant IDs follow ' . count($patterns) . ' different patterns ('
                . implode('; ', array_map(static fn ($l) => implode(', ', array_slice($l, 0, 3)), $patterns)) . ')',
                'check they are not typos');
        }
    }

    private function _bidsValidator(string $dir, ?string $pythonBin, array $held): void
    {
        $this->_section('validator', 'BIDS validator', 'as Loris-MRI uses it', '');
        $helper = dirname(__DIR__, 2) . '/scripts/test/bids_validator_check.py';
        if ($pythonBin === null || !is_file($helper)) {
            $this->_add('validator', self::NOTE, 'not run (no Python with bids_validator; pass --python=, e.g. the Loris-MRI virtualenv)', '');
            return;
        }
        $out  = [];
        exec(escapeshellarg($pythonBin) . ' -I ' . escapeshellarg($helper) . ' ' . escapeshellarg($dir) . ' 2>/dev/null', $out);
        $data = json_decode(implode("\n", $out), true);
        if (!is_array($data) || isset($data['error'])) {
            $this->_add('validator', self::NOTE, 'not run: ' . ($data['error'] ?? "{$pythonBin} gave no result"), '');
            return;
        }
        $shown = 0;
        foreach ($data['invalid'] as $rel) {
            $sub = preg_match('#^/(sub-[^/]+)/(ses-[^/]+)?#', $rel, $m) ? $m[1] : 'validator';
            if (isset($m[2]) && isset($held[$sub][$m[2]]) && str_contains(basename($rel), '_ses-')) {
                continue;   // rejected for its ses- label: the visit is already held back (VISIT MISMATCH)
            }
            $shown++;
            $this->_add($sub, self::WARNING, "{$rel}: not a valid BIDS file name - Loris-MRI skips it (not imported)",
                'rename it to the BIDS pattern (sub-<ID>[_ses-<visit>]_..._<suffix>.<ext>), or remove it');
        }
        $this->sections[$this->index['validator']]['summary'] = "{$data['validator']}: {$data['checked']} file(s), "
            . count($data['invalid']) . ' rejected'
            . ($shown > 0 ? " - {$shown} listed under their participant" : '')
            . (count($data['invalid']) > $shown ? ', the rest because of a held-back visit' : '');
    }

    private function _loris(array $config, array $p, ParticipantsTsv $roster): void
    {
        try {
            $api      = PipelineSetup::api($config, $p);
            $projects = $api->getProjectNames();
            $sites    = array_column($api->getSites(), 'Name');
        } catch (\Throwable $e) {
            $this->_add('loris', self::ERROR, 'LORIS could not be read: ' . $e->getMessage(), 'check the connection and the API user');
            return;
        }
        $project = (string) ($p['candidate_defaults']['project'] ?? '');
        if ($project !== '' && !in_array($project, $projects, true)) {
            $this->_add('loris', self::ERROR, "project '{$project}' is not a LORIS project", 'use the exact LORIS name');
        }
        $siteNames = [];
        foreach ($roster->all() as $row) {
            $siteNames[(string) ($row['site'] ?? '')] = true;
        }
        foreach (array_keys($siteNames) as $site) {
            if ($site !== '' && !in_array($site, $sites, true)) {
                $this->_add('loris', self::ERROR, "site '{$site}' is not a LORIS site the pipeline user can see",
                    'use one of: ' . implode(', ', array_slice($sites, 0, 10)));
            }
        }
        $new = $in = 0;
        foreach ($roster->all() as $pid => $row) {
            $subj = ParticipantSync::subjectId((string) $pid);
            try {
                $found = $api->findCandidateByExternalId(ParticipantSync::externalId($row, $subj));
            } catch (\RuntimeException $e) {
                $level = $e->getCode() === LorisApiClientInterface::PROJECT_CONFLICT ? self::ERROR : self::WARNING;
                $this->_add("sub-{$subj}", $level, "sub-{$subj}: " . $e->getMessage(), '');
                continue;
            }
            if ($found === null) {
                $new++;
                continue;
            }
            $in++;
            $diff = ParticipantSync::lorisDifferences($api, $found, $row);
            if ($diff !== []) {
                $this->_add("sub-{$subj}", self::ERROR, "sub-{$subj}: already in LORIS as {$found['pscid']}, but "
                    . implode('; ', $diff), 'correct participants.tsv, or the candidate in LORIS (ingestion never changes LORIS)');
            }
        }
        $this->sections[$this->index['loris']]['summary'] = "{$in} participant(s) already in LORIS, {$new} new";
    }

    /** The participant section a message is about, or participants.tsv. */
    private function _keyFor(string $message): string
    {
        return preg_match('/\b(sub-[A-Za-z0-9]+)/', $message, $m) ? $m[1] : 'tsv';
    }

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
            str_starts_with($key, 'sub-')
                ? $this->_section($key, 'Participant', $key, '')
                : $this->_section($key, ucfirst($key), '', '');
        }
        foreach ($this->sections[$this->index[$key]]['issues'] as $i) {
            if ($i['message'] === $message) {
                return;
            }
        }
        $this->sections[$this->index[$key]]['issues'][] = ['severity' => $severity, 'message' => $message, 'fix' => $fix];
    }
}
