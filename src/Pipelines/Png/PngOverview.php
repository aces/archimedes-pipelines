<?php

/**
 * PngOverview
 *
 * The "what will go in" part of the PNG validation report: every value the
 * ingestion will use, where it comes from, and whether it matches - files and
 * spreadsheets, project, sites, cohorts, visits, scan types, participants (one
 * line each), and the environment (project lock, writable folders).
 *
 * Statuses, as in the DICOM report's VALUES table:
 *   OK           matches
 *   FIX          a MUST FIX item (listed there with the fix)
 *   HELD         not ingested this time (e.g. visit not in project.json "visits")
 *   SET ASIDE    images not loaded (the rest of the visit is)
 *   UNLINKED     not matched to a participant/visit: reported only
 *   CHECK        works, but worth a look (e.g. cohort not in project.json "cohorts")
 *   INFO         for information
 *   LORIS        checked by LORIS later (cohort and visit at session creation,
 *                scan types at import / run_png_import.php --validate)
 *   NOT CHECKED  LORIS could not be read, or --offline
 *
 * Reads only. PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Png;

use LORIS\Endpoints\LorisApiClientAdapter;
use LORIS\Pipelines\Shared\ParticipantsCheck;
use LORIS\Pipelines\Shared\ParticipantsTsv;
use LORIS\Pipelines\Shared\ParticipantSync;
use LORIS\Pipelines\Shared\SessionsTsv;
use LORIS\Utils\Dob;

final class PngOverview
{
    /** @var array<string, array<int, array{0: string, 1: string}>> section => [status, text] */
    private array $sections = [];

    /** @var string[] MUST FIX lines found against LORIS ("[LORIS] where: what -> fix") */
    private array $lorisFix = [];

    /** @var string[] COULD NOT CHECK lines */
    private array $couldNot = [];

    private ?ParticipantsTsv $roster = null;

    /**
     * @param array      $manifest      PngDelivery::scan() result.
     * @param array      $projectConfig project.json.
     * @param array|null $loris         null = offline; else ['projects' => ?string[], 'sites' => ?array,
     *                                  'existing' => [subject => ?array{pscid, cand_id, diff: string[]}],
     *                                  'error' => ?string] (see collectLoris()).
     * @param array|null $lock          ProjectLock::probe() result.
     */
    public function __construct(
        private array $manifest,
        private array $projectConfig,
        private ?array $loris,
        private ?array $lock = null
    ) {
        $tsv = $manifest['participants_tsv'] ?? null;
        if ($tsv !== null && is_file($tsv)) {
            try {
                $this->roster = ParticipantsTsv::load($tsv, $projectConfig['candidate_defaults'] ?? [], $projectConfig);
            } catch (\Throwable $e) {
                $this->roster = null;     // reported as MUST FIX by the scan
            }
        }
        $this->_build();
    }

    /**
     * Read what the overview needs from LORIS (read-only), for the linked
     * participants. Never throws: a failure is recorded under 'error'.
     */
    public static function collectLoris(LorisApiClientAdapter $api, array $manifest, callable $rowFor): array
    {
        $out = ['projects' => null, 'sites' => null, 'existing' => [], 'error' => null];
        try {
            $out['projects'] = $api->getProjectNames();
            $out['sites']    = $api->getSites();
            foreach ($manifest['visits'] ?? [] as $v) {
                if (array_key_exists($v['subject'], $out['existing'])) {
                    continue;
                }
                $found = $api->findCandidateByExternalId((string) $v['external_id']);
                $out['existing'][$v['subject']] = $found === null ? null : $found + [
                    'diff' => ParticipantSync::lorisDifferences($api, $found, $rowFor($v['subject']) ?? []),
                ];
            }
        } catch (\Throwable $e) {
            $out['error'] = $e->getMessage();
        }
        return $out;
    }

    /** @return string[] */
    public function lorisFix(): array
    {
        return $this->lorisFix;
    }

    /** @return string[] */
    public function couldNotCheck(): array
    {
        return $this->couldNot;
    }

    /**
     * The overview as data: section title => [[status, text], ...].
     *
     * @return array<string, array<int, array{0: string, 1: string}>>
     */
    public function sections(): array
    {
        return $this->sections;
    }

    /**
     * The overview as report text.
     */
    public function text(): string
    {
        $out = "OVERVIEW - what ingestion will use, where it comes from, and whether it matches\n";
        foreach ($this->sections as $title => $rows) {
            $out .= "\n  {$title}\n";
            foreach ($rows as [$status, $text]) {
                $out .= sprintf("    %-11s %s\n", "[{$status}]", $text);
            }
        }
        return $out . "\n";
    }

    // =========================================================================

    private function _add(string $section, string $status, string $text): void
    {
        $this->sections[$section][] = [$status, $text];
    }

    private function _build(): void
    {
        $this->_files();
        $this->_project();
        $this->_sitesAndCohorts();
        $this->_visits();
        $this->_scanTypes();
        $this->_participants();
        $this->_foldersWithoutRow();
        $this->_environment();
    }

    private function _files(): void
    {
        $S   = 'FILES';
        $dir = (string) ($this->manifest['delivery_dir'] ?? '');
        $this->_add($S, is_dir($dir) ? 'OK' : 'FIX', "delivery folder {$dir}");

        $tsv = $this->manifest['participants_tsv'] ?? null;
        if ($tsv === null) {
            $this->_add($S, 'UNLINKED', 'participants.tsv: missing - every visit is unlinked');
        } else {
            $head  = (string) @file_get_contents($tsv, false, null, 0, 65536);
            $notes = [];
            if (str_starts_with($head, "\xEF\xBB\xBF")) {
                $notes[] = 'UTF-8 with BOM (Excel) - fine';
            }
            if (str_contains($head, "\r\n")) {
                $notes[] = 'Windows line ends - fine';
            }
            if ($head !== '' && !self::_isUtf8($head)) {
                $notes[] = 'not UTF-8 - accents may be garbled';
            }
            if ($this->roster === null) {
                $this->_add($S, 'FIX', 'participants.tsv: cannot be used - '
                    . ($this->manifest['participants_tsv_error'] ?? 'see the ERRORs above'));
            } else {
                $all  = $this->roster->all();
                $cols = implode(', ', array_keys((array) (reset($all) ?: [])));
                $this->_add($S, 'OK', sprintf('participants.tsv: tab-separated, %d row(s); columns used: %s%s',
                    $this->roster->count(), $cols, $notes ? ' (' . implode('; ', $notes) . ')' : ''));
                foreach (ParticipantsCheck::columnSources($this->roster)['defaults'] as $line) {
                    $this->_add($S, 'INFO', $line);
                }
            }
        }

        // sessions files of the participants with a folder
        $subjects = array_values(array_unique(array_column($this->manifest['visits'] ?? [], 'subject')));
        $with = 0;
        $alt  = 0;
        foreach ($subjects as $subj) {
            $p = SessionsTsv::path($dir, (string) $subj);
            if (is_file($p)) {
                $with++;
                $alt += basename($p) !== 'sessions.tsv' ? 1 : 0;
            }
        }
        if ($subjects !== []) {
            $this->_add($S, $with === count($subjects) ? 'OK' : 'UNLINKED', sprintf(
                'sessions files: %d of %d participant folder(s)%s', $with, count($subjects),
                $alt ? " ({$alt} named sub-<ID>_sessions.tsv - accepted)" : ''));
        }

        $c = $this->manifest['counts'] ?? [];
        $bytes = 0;
        foreach ($this->manifest['visits'] ?? [] as $v) {
            foreach ($v['images'] ?? [] as $i) {
                $bytes += (int) $i['size'];
            }
        }
        $ignored = 0;
        foreach ($this->manifest['problems'] ?? [] as $p) {
            if (preg_match('/^(\d+) non-PNG file\(s\) ignored/', (string) $p['message'], $m)) {
                $ignored += (int) $m[1];
            }
        }
        $this->_add($S, ($c['set_aside'] ?? 0) > 0 ? 'SET ASIDE' : 'OK', sprintf(
            'PNG images: %d accepted (%s), %d set aside%s; visits: %d linked, %d unlinked, %d with a data issue',
            $c['images'] ?? 0, $bytes >= 1048576 ? sprintf('%.1f MB', $bytes / 1048576) : sprintf('%.1f KB', $bytes / 1024),
            $c['set_aside'] ?? 0,
            $ignored ? ", {$ignored} non-PNG file(s) ignored" : '',
            $c['linked'] ?? 0, $c['unlinked'] ?? 0, $c['blocked'] ?? 0));
    }

    private function _project(): void
    {
        $S   = 'PROJECT';
        $pc  = $this->projectConfig;
        $def = $pc['candidate_defaults'] ?? [];
        $mods = array_map('strtolower', array_map('strval', (array) ($pc['modalities'] ?? [])));
        $this->_add($S, in_array('imaging', $mods, true) ? 'OK' : 'FIX',
            'project.json modalities: ' . implode(', ', (array) ($pc['modalities'] ?? [])) . ' (needs Imaging)');

        $values = $this->_distinct('project');
        if ($values === []) {
            $this->_add($S, 'FIX', 'LORIS project: not set (candidate_defaults.project or a participants.tsv project column)');
        }
        foreach ($values as $v => [$n, $from]) {
            [$status, $note] = $this->_lorisList('projects', $v, static fn ($x) => $x);
            if ($status === 'FIX') {
                $this->lorisFix[] = "[LORIS] {$from}: project '{$v}' is not a LORIS project -> use the exact LORIS "
                    . 'project name: ' . implode(', ', array_slice((array) $this->loris['projects'], 0, 10));
            }
            $this->_add($S, $status, "LORIS project '{$v}' ({$from}, {$n} participant(s)){$note}");
        }
        $ext = $def['project_external_name'] ?? null;
        $this->_add($S, 'INFO', 'ExternalID project (candidatesPlus ProjectExternalName): '
            . ($ext !== null ? "'{$ext}' (candidate_defaults.project_external_name)" : 'same as the LORIS project'));
    }

    private function _sitesAndCohorts(): void
    {
        foreach (['site' => ['SITES', 'sites'], 'cohort' => ['COHORTS', 'cohorts']] as $col => [$S, $listKey]) {
            $declared = array_values(array_filter(array_map('strval', (array) ($this->projectConfig[$listKey] ?? []))));
            $declared = array_values(array_filter($declared, static fn ($x) => !str_starts_with($x, '<')));   // template placeholders
            $values   = $this->_distinct($col);
            if ($values === []) {
                $this->_add($S, 'FIX', "{$col}: not set (participants.tsv {$col} column or candidate_defaults.{$col})");
                continue;
            }
            foreach ($values as $v => [$n, $from]) {
                $text = "'{$v}' ({$from}, {$n} participant(s))";
                if ($col === 'site') {
                    [$status, $note] = $this->_lorisList('sites', $v, static fn ($s) => $s['Name']);
                    if ($status === 'FIX') {
                        $hint = null;
                        foreach ((array) $this->loris['sites'] as $s) {
                            if (strcasecmp((string) $s['Alias'], $v) === 0 || strcasecmp((string) $s['Name'], $v) === 0) {
                                $hint = $s['Name'];
                            }
                        }
                        $this->lorisFix[] = "[LORIS] {$from}: site '{$v}' is not a LORIS site name the pipeline user "
                            . 'is affiliated with -> ' . ($hint !== null ? "use '{$hint}'"
                            : 'use one of: ' . implode(', ', array_column((array) $this->loris['sites'], 'Name')));
                    }
                    $text .= $note;
                } else {
                    $status = 'LORIS';
                    $text  .= ' - LORIS checks it when each session is created';
                }
                if ($declared !== [] && !in_array($v, $declared, true) && $status !== 'FIX') {
                    $status = 'CHECK';
                    $text  .= " - NOT in project.json \"{$listKey}\" (" . implode(', ', $declared) . ')';
                }
                $this->_add($S, $status, $text);
            }
        }
    }

    private function _visits(): void
    {
        $S        = 'VISITS';
        $declared = array_values(array_filter(array_map(static fn ($v) => trim((string) $v),
            (array) ($this->projectConfig['visits'] ?? []))));
        $byLabel  = [];
        foreach ($this->manifest['visits'] ?? [] as $v) {
            $l = (string) $v['session'];
            $byLabel[$l]['subjects'][$v['subject']] = true;
            $byLabel[$l]['images']   = ($byLabel[$l]['images'] ?? 0) + count($v['images'] ?? []);
            $byLabel[$l]['status'][$v['link_status']] = ($byLabel[$l]['status'][$v['link_status']] ?? 0) + 1;
        }
        ksort($byLabel, SORT_STRING);
        $this->_add($S, $declared === [] ? 'CHECK' : 'INFO', 'project.json "visits": '
            . ($declared === [] ? 'not declared - LORIS checks each visit when the session is created'
                : implode(', ', $declared)));

        $seenLc = [];
        foreach ($byLabel as $label => $d) {
            $seenLc[strtolower($label)] = true;
            $text = sprintf('ses-%s: %d participant(s), %d image(s)', $label, count($d['subjects']), $d['images']);
            $unl  = $d['status']['unlinked'] ?? 0;
            $blk  = $d['status']['blocked'] ?? 0;
            $tail = ($unl ? ", {$unl} unlinked" : '') . ($blk ? ", {$blk} with a data issue" : '');
            $match = null;
            foreach ($declared as $dv) {
                if (strcasecmp($dv, $label) === 0) {
                    $match = $dv;
                }
            }
            if (!preg_match(PngDelivery::LABEL_REGEX, $label)) {
                $this->_add($S, 'FIX', "{$text} - label not letters and digits only{$tail}");
            } elseif ($declared === []) {
                $this->_add($S, 'LORIS', "{$text} - checked by LORIS at session creation{$tail}");
            } elseif ($match === null) {
                $this->_add($S, 'HELD', "{$text} - NOT in project.json \"visits\": held back{$tail}");
            } elseif ($match !== $label) {
                $this->_add($S, 'OK', "{$text} - ingested as LORIS visit '{$match}' (case differs){$tail}");
            } else {
                $this->_add($S, $unl || $blk ? 'CHECK' : 'OK', "{$text}{$tail}");
            }
        }
        foreach ($declared as $dv) {
            if (!isset($seenLc[strtolower($dv)])) {
                $this->_add($S, 'INFO', "{$dv}: declared, no images delivered for it");
            }
        }
    }

    private function _scanTypes(): void
    {
        $S      = 'SCAN TYPES (start of each file name)';
        $agreed = PngDelivery::scanTypes($this->projectConfig);
        $found  = [];
        foreach ($this->manifest['visits'] ?? [] as $v) {
            foreach ($v['images'] ?? [] as $i) {
                $t = $i['scan_type'];
                $found[$t]['images'] = ($found[$t]['images'] ?? 0) + 1;
                $found[$t]['visits'][$v['key']] = true;
            }
        }
        // Set-aside images, by the type their name starts with.
        $aside = [];
        foreach ($this->manifest['visits'] ?? [] as $v) {
            foreach ($v['set_aside'] ?? [] as $a) {
                $t = explode('_', (string) pathinfo($a['name'], PATHINFO_FILENAME))[0];
                $aside[$t][$a['reason']] = ($aside[$t][$a['reason']] ?? 0) + 1;
            }
        }
        $this->_add($S, $agreed === [] ? 'CHECK' : 'INFO', 'agreed at onboarding (project.json png.scan_types): '
            . ($agreed === [] ? 'not declared - every type is accepted here, LORIS checks them at import'
                : implode(', ', $agreed)));
        ksort($found, SORT_STRING);
        foreach ($found as $t => $d) {
            $this->_add($S, 'OK', sprintf("%s: %d image(s) in %d visit(s) - must exist in LORIS mri_scan_type "
                . '(checked by run_png_import.php --validate / at import)', $t, $d['images'], count($d['visits'])));
        }
        ksort($aside, SORT_STRING);
        foreach ($aside as $t => $reasons) {
            foreach ($reasons as $reason => $n) {
                $this->_add($S, 'SET ASIDE', sprintf("'%s': %d image(s) - %s", $t === '' ? '(none)' : $t, $n, $reason));
            }
        }
        foreach ($agreed as $t) {
            if (!isset($found[$t])) {
                $this->_add($S, 'INFO', "{$t}: agreed, none in this delivery");
            }
        }
    }

    private function _participants(): void
    {
        $S    = 'PARTICIPANTS';
        $rows = $this->roster?->all() ?? [];
        if ($rows === []) {
            $this->_add($S, 'FIX', 'no usable participants.tsv - see the ERRORs above');
            return;
        }
        $pc      = $this->projectConfig;
        $order   = Dob::dateOrder($pc);
        $maps    = $pc['sex_mappings'] ?? [];
        $byVisit = [];
        foreach ($this->manifest['visits'] ?? [] as $v) {
            $byVisit[$v['subject']][] = $v;
        }

        $sexCount = [];
        $dobs     = [];
        foreach ($rows as $pid => $row) {
            $sex = LorisApiClientAdapter::normaliseSexValue((string) ($row['sex'] ?? ''), $maps);
            $sex !== null && $sexCount[$sex] = ($sexCount[$sex] ?? 0) + 1;
            $d = Dob::normalize((string) ($row['dob'] ?? ''), $order);
            Dob::problem($d) === null && $dobs[] = $d;
        }
        ksort($sexCount);
        sort($dobs);
        $this->_add($S, 'INFO', sprintf('%d row(s); sex: %s; dob: %d valid%s', count($rows),
            implode(', ', array_map(static fn ($k, $n) => "{$k} {$n}", array_keys($sexCount), $sexCount)) ?: 'none valid',
            count($dobs), $dobs ? " ({$dobs[0]} ... " . end($dobs) . ", date order {$order})" : ''));

        if ($this->loris !== null && $this->loris['error'] === null && $this->loris['existing'] !== []) {
            $ex = array_filter($this->loris['existing']);
            $df = array_filter($ex, static fn ($e) => $e['diff'] !== []);
            $this->_add($S, $df ? 'FIX' : 'OK', sprintf('in LORIS: %d already (dob/sex %s), %d new - created by participant sync',
                count($ex), $df ? count($df) . ' differ - see the ERRORs above' : 'match', count($this->loris['existing']) - count($ex)));
        }

        $declaredVisits = array_map('strtolower', array_filter(array_map('strval', (array) ($pc['visits'] ?? []))));
        $declaredCohorts = array_values(array_filter(array_map('strval', (array) ($pc['cohorts'] ?? [])),
            static fn ($x) => !str_starts_with($x, '<')));
        $lorisSites = ($this->loris !== null && $this->loris['error'] === null && $this->loris['sites'] !== null)
            ? array_column((array) $this->loris['sites'], 'Name') : null;
        $rank = ['INFO' => 0, 'OK' => 1, 'CHECK' => 2, 'UNLINKED' => 3, 'HELD' => 4, 'FIX' => 5];
        $worse = static fn (string $a, string $b): string => $rank[$b] > $rank[$a] ? $b : $a;

        foreach ($rows as $pid => $row) {
            $subj = ParticipantSync::subjectId((string) $pid);
            $sexRaw = (string) ($row['sex'] ?? '');
            $sex    = LorisApiClientAdapter::normaliseSexValue($sexRaw, $maps);
            $dobRaw = (string) ($row['dob'] ?? '');
            $dob    = Dob::normalize($dobRaw, $order);
            $dobBad = Dob::problem($dob) !== null;
            $ext    = ParticipantSync::externalId($row, $subj);

            $visits = [];
            $state  = 'OK';
            foreach ($byVisit[$subj] ?? [] as $v) {
                $held = $v['link_status'] === 'linked' && $declaredVisits !== []
                    && !in_array(strtolower((string) $v['session']), $declaredVisits, true);
                $visits[] = sprintf('%s(%s)', $v['session'], match (true) {
                    $held                          => 'held: not in "visits"',
                    $v['link_status'] === 'linked' => count($v['images']) . (count($v['set_aside']) ? '+' . count($v['set_aside']) . ' aside' : ''),
                    $v['link_status'] === 'unlinked' => 'unlinked',
                    default                        => 'issue',
                });
                $state = $worse($state, match (true) {
                    $held => 'HELD',
                    $v['link_status'] === 'linked' => count($v['set_aside']) ? 'CHECK' : 'OK',
                    $v['link_status'] === 'unlinked' => 'UNLINKED',
                    default => 'FIX',
                });
            }
            if (!isset($byVisit[$subj])) {
                $state = 'INFO';
            }
            $site   = trim((string) ($row['site'] ?? ''));
            $cohort = trim((string) ($row['cohort'] ?? ''));
            $siteNote = $cohortNote = '';
            if ($lorisSites !== null && $site !== '' && !in_array($site, $lorisSites, true)) {
                $siteNote = ' NOT IN LORIS';
                $state    = $worse($state, 'FIX');
            }
            if ($declaredCohorts !== [] && $cohort !== '' && !in_array($cohort, $declaredCohorts, true)) {
                $cohortNote = ' not in project.json cohorts';
                $state      = $worse($state, 'CHECK');
            }
            if ($sex === null || $dobBad) {
                $state = 'FIX';
            }

            $lorisNote = isset($byVisit[$subj]) ? 'LORIS not checked' : 'not looked up (no images)';
            if ($this->loris !== null && $this->loris['error'] === null && array_key_exists($subj, $this->loris['existing'])) {
                $e = $this->loris['existing'][$subj];
                if ($e === null) {
                    $lorisNote = 'new';
                } else {
                    $lorisNote = "in LORIS as {$e['pscid']} ({$e['cand_id']})";
                    if ($e['diff'] !== []) {
                        $lorisNote .= ' DIFFERS: ' . implode('; ', $e['diff']);
                        $state = 'FIX';
                        $this->lorisFix[] = "[LORIS] participants.tsv ({$pid}, already in LORIS as {$e['pscid']}): "
                            . implode('; ', $e['diff']) . ' -> correct participants.tsv, or the candidate in LORIS '
                            . '(ingestion never changes LORIS)';
                    }
                }
            }

            $this->_add($S, $state, sprintf('%s  dob %s%s  sex %s%s  site %s  cohort %s  ext %s  %s  visits: %s',
                $pid,
                $dobBad ? "'{$dobRaw}' INVALID" : $dob, $dob !== $dobRaw && !$dobBad ? " (from '{$dobRaw}')" : '',
                $sex ?? "'{$sexRaw}' INVALID", $sex !== null && $sex !== $sexRaw ? " (from '{$sexRaw}')" : '',
                ($site ?: '-') . $siteNote, ($cohort ?: '-') . $cohortNote, $ext, $lorisNote,
                $visits ? implode(' ', $visits) : 'no folder'));
        }
    }

    /**
     * Participant folders with no participants.tsv row (their visits are unlinked).
     */
    private function _foldersWithoutRow(): void
    {
        $rows = $this->roster?->all() ?? [];
        $seen = [];
        foreach ($this->manifest['visits'] ?? [] as $v) {
            $subj = (string) $v['subject'];
            if (isset($seen[$subj]) || ($v['link_reason'] ?? '') !== PngDelivery::NOT_IN_PARTICIPANTS_TSV) {
                continue;
            }
            $seen[$subj] = true;
            $this->_add('PARTICIPANTS', 'UNLINKED', "sub-{$subj}  folder with no participants.tsv row - its images are "
                . 'not ingested until the row is added');
        }
        unset($rows);
    }

    private function _environment(): void
    {
        $S = 'ENVIRONMENT (for the ARCHIMEDES team)';
        if ($this->lock !== null) {
            $this->_add($S, $this->lock['status'] === 'OK' ? 'OK' : ($this->lock['status'] === 'WARN' ? 'CHECK' : 'FIX'),
                "project lock (.png_pipeline.lock, one run at a time): {$this->lock['message']} [{$this->lock['mount']}]");
            if ($this->lock['status'] === 'FAIL') {
                $this->lorisFix[] = "[ENVIRONMENT] project folder: {$this->lock['message']}";
            }
        }
        $dir = dirname(rtrim((string) ($this->manifest['delivery_dir'] ?? ''), '/'), 3);   // project root
        foreach (['processed/imaging', 'deidentified-lorisid/imaging', 'logs/png'] as $sub) {
            $p = $dir . '/' . $sub;
            $probe = is_dir($p) ? $p : $dir;
            $ok = is_writable($probe);
            $this->_add($S, $ok ? 'OK' : 'FIX', "{$sub}/: " . ($ok ? (is_dir($p) ? 'writable' : 'will be created')
                : 'NOT writable by the pipeline user'));
        }
        $this->_add($S, 'INFO', 'on the LORIS host: deidentified-lorisid/imaging/png/ must be readable by the web server '
            . 'user at the same path (the import endpoint reads it there)');
        if ($this->loris !== null && $this->loris['error'] !== null) {
            $this->couldNot[] = 'LORIS could not be read (' . $this->loris['error'] . ') - project, sites and participants '
                . 'already in LORIS were not checked -> check api.base_url, the API user and its loris_api permission';
        } elseif ($this->loris === null) {
            $this->couldNot[] = '--offline: LORIS was not checked (project, sites, participants already in LORIS)';
        }
    }

    // =========================================================================

    /**
     * Distinct values of a column: from participants.tsv, or the default.
     *
     * @return array<string, array{0: int, 1: string}> value => [participants, where from]
     */
    private function _distinct(string $col): array
    {
        $rows = $this->roster?->all() ?? [];
        $enriched = $this->roster !== null && in_array($col, $this->roster->enrichedColumns(), true);
        $out  = [];
        foreach ($rows as $row) {
            $v = trim((string) ($row[$col] ?? ''));
            if ($v !== '') {
                $out[$v][0] = ($out[$v][0] ?? 0) + 1;
                $out[$v][1] = $enriched ? 'project.json candidate_defaults' : 'participants.tsv';
            }
        }
        $def = trim((string) ($this->projectConfig['candidate_defaults'][$col] ?? ''));
        if ($out === [] && $def !== '') {
            $out[$def] = [count($rows), 'project.json candidate_defaults'];
        }
        ksort($out, SORT_STRING);
        return $out;
    }

    /** UTF-8 check without requiring ext-mbstring. */
    private static function _isUtf8(string $s): bool
    {
        return (bool) preg_match('//u', $s);
    }

    /**
     * A value against a LORIS list: [status, note].
     *
     * @return array{0: string, 1: string}
     */
    private function _lorisList(string $key, string $value, callable $name): array
    {
        if ($this->loris === null) {
            return ['NOT CHECKED', ' - LORIS not checked (--offline)'];
        }
        if ($this->loris['error'] !== null || $this->loris[$key] === null) {
            return ['NOT CHECKED', ' - LORIS not reachable'];
        }
        $names = array_map($name, (array) $this->loris[$key]);
        return in_array($value, $names, true) ? ['OK', ' - exists in LORIS'] : ['FIX', ' - NOT in LORIS (see the ERRORs above)'];
    }
}

