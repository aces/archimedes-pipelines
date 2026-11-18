<?php

/**
 * PngPreflight
 *
 * Every data issue in a PNG delivery - from the scan (folders, sessions files,
 * images) and from participants.tsv and the project configuration - found in
 * one pass, BEFORE any candidate or session is created. The PNG counterpart of
 * DicomPreflight, with the visit (sub-/ses- folder) as the unit:
 *
 *   project  nothing can safely continue (participants.tsv unusable, no
 *            participant folder, a needed LORIS column missing). Always aborts.
 *   subject  one participant (sex/dob that cannot be normalised, identifier not
 *            letters and digits, sessions file unreadable): all of its visits
 *            are excluded and no candidate is created.
 *   visit    one visit (visit label, acq_date, case clash, visit not in
 *            project.json "visits"): only that visit is excluded.
 *
 * Set-aside images are NOT issues: per the user guide they are reported back
 * and the rest of the visit goes in. Unlinked visits are not issues either:
 * they are reported and never processed, as for DICOM.
 *
 * Reads only: the scan manifest, participants.tsv, project.json.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Png;

use LORIS\Pipelines\Shared\ParticipantsCheck;

final class PngPreflight
{
    /**
     * @param array         $manifest      PngDelivery::scan() result (or the saved manifest).
     * @param array         $projectConfig Decoded project.json.
     * @param callable|null $visitLabels   fn(): string[] declared visit labels; null skips the check.
     * @param array         $exclude       ['visits' => ['SUB/VISIT'], 'subjects' => ['SUB']] already left out.
     *
     * @return array{errors: string[], warnings: string[], issues: array, linked: int, unlinked: int}
     */
    public static function check(array $manifest, array $projectConfig, ?callable $visitLabels, array $exclude = []): array
    {
        $issues   = [];
        $warnings = [];

        foreach ($manifest['problems'] ?? [] as $p) {
            $line = self::line($p);
            if (($p['level'] ?? '') !== 'error') {
                $warnings[] = $line;
                continue;
            }
            $issues[] = array_filter([
                'scope'   => $p['scope'] === 'image' ? 'visit' : $p['scope'],
                'subject' => $p['subject'] ?? null,
                'session' => $p['session'] ?? null,
                'message' => $line,
            ], static fn ($v) => $v !== null);
        }

        $visits = $manifest['visits'] ?? [];
        $linked = array_values(array_filter($visits, static fn ($v) => ($v['link_status'] ?? '') === 'linked'));
        $counts = ['linked' => count($linked), 'unlinked' => 0];

        foreach ($visits as $v) {
            if (($v['link_status'] ?? '') === 'unlinked') {
                $counts['unlinked']++;
                $warnings[] = sprintf('UNLINKED sub-%s/ses-%s: %s', $v['subject'], $v['session'],
                    PngDelivery::unlinkedAdvice($v['link_reason'] ?? null, $v['subject'], $v['session']));
            }
        }

        // participants.tsv values (sex, dob, identifiers, columns) and visit
        // labels against project.json "visits" - the shared, modality-
        // independent checks, the same messages DICOM and BIDS give.
        $tsv = $manifest['participants_tsv'] ?? null;
        if ($linked !== [] && $tsv !== null && is_file($tsv)) {
            $sessions = [];
            foreach ($linked as $v) {
                $sessions[$v['subject']][] = $v['session'];
            }
            $result = ParticipantsCheck::check(
                $tsv,
                $projectConfig,
                $sessions,
                $visitLabels,
                array_values(array_unique(array_column($visits, 'subject'))),
                'PNG images'
            );
            foreach ($result['warnings'] as $w) {
                // Rows with no folder: the scan already reports them (with the fix).
                if (str_starts_with($w, 'participants.tsv row ') && str_ends_with($w, 'has no PNG images in this delivery')) {
                    continue;
                }
                // Visits not in project.json "visits" are held back, not
                // ingested: an issue for that visit (the DICOM reidentifier
                // holds them back the same way; here the operator sees it first).
                if (preg_match("/^visit '([^']+)' \\(([^)]*)\\) is not in project\\.json \"visits\"/", $w, $m)) {
                    foreach ($linked as $v) {
                        if ($v['session'] === $m[1]) {
                            $issues[] = ['scope' => 'visit', 'subject' => $v['subject'], 'session' => $v['session'],
                                'message' => "sub-{$v['subject']}/ses-{$v['session']}: visit '{$m[1]}' is not in "
                                    . 'project.json "visits" - fix the ses- folder name, or add the visit to '
                                    . '"visits" (and set it up in LORIS)'];
                        }
                    }
                    continue;
                }
                $warnings[] = $w;
            }
            foreach ($result['issues'] as $issue) {
                $issues[] = $issue;
            }
        }

        $excludedVisits   = array_flip($exclude['visits'] ?? []);
        $excludedSubjects = array_flip($exclude['subjects'] ?? []);
        $open = array_values(array_filter($issues, static function (array $i) use ($excludedVisits, $excludedSubjects): bool {
            return match ($i['scope']) {
                'visit', 'session' => !isset($excludedVisits[($i['subject'] ?? '') . '/' . ($i['session'] ?? '')])
                    && !isset($excludedSubjects[(string) ($i['subject'] ?? '')]),
                'subject' => !isset($excludedSubjects[(string) ($i['subject'] ?? '')]),
                default   => true,
            };
        }));

        return [
            'errors'   => array_values(array_unique(array_column($open, 'message'))),
            'warnings' => array_values(array_unique($warnings)),
            'issues'   => $issues,
        ] + $counts;
    }

    /**
     * Split the delivery into good and bad visits.
     *
     * @return array{abort: string[], exclude_visits: string[], exclude_subjects: string[],
     *               bad: array<string, string[]>, good: string[]}
     */
    public static function plan(array $manifest, array $issues): array
    {
        $visits = array_values(array_filter(
            $manifest['visits'] ?? [],
            static fn ($v) => in_array($v['link_status'] ?? '', ['linked', 'blocked'], true)
        ));
        $abort = $excludeVisits = $excludeSubjects = $bad = [];

        foreach ($issues as $i) {
            $subject = (string) ($i['subject'] ?? '');
            $session = (string) ($i['session'] ?? '');
            switch ($i['scope']) {
                case 'project':
                    $abort[] = $i['message'];
                    break;
                case 'subject':
                    $excludeSubjects[$subject] = true;
                    $hit = false;
                    foreach ($visits as $v) {
                        if ($v['subject'] === $subject) {
                            $excludeVisits[$v['key']] = true;
                            $bad["sub-{$subject}/ses-{$v['session']}"][] = $i['message'];
                            $hit = true;
                        }
                    }
                    if (!$hit) {
                        $bad["sub-{$subject}"][] = $i['message'];
                    }
                    break;
                default:    // visit / session
                    $excludeVisits["{$subject}/{$session}"] = true;
                    $bad["sub-{$subject}/ses-{$session}"][] = $i['message'];
            }
        }

        // A participant with no good visit left is not created either.
        $bySubject = [];
        foreach ($visits as $v) {
            $bySubject[$v['subject']][] = isset($excludeVisits[$v['key']]) || $v['link_status'] === 'blocked';
        }
        foreach ($bySubject as $subject => $flags) {
            if (!in_array(false, $flags, true)) {
                $excludeSubjects[(string) $subject] = true;
            }
        }

        $good = [];
        foreach ($visits as $v) {
            if ($v['link_status'] === 'linked' && !isset($excludeVisits[$v['key']])) {
                $good[] = "sub-{$v['subject']}/ses-{$v['session']}";
            }
        }

        return [
            'abort'            => array_values(array_unique($abort)),
            'exclude_visits'   => array_keys($excludeVisits),
            'exclude_subjects' => array_map('strval', array_keys($excludeSubjects)),
            'bad'              => array_map(static fn ($r) => array_values(array_unique($r)), $bad),
            'good'             => $good,
        ];
    }

    /**
     * Read an exclusion file written by the bundle.
     *
     * @return array{visits: string[], subjects: string[], issues: array<string, string[]>}
     */
    public static function readExclusions(?string $path): array
    {
        if ($path === null || $path === '' || !is_file($path)) {
            return ['visits' => [], 'subjects' => [], 'issues' => []];
        }
        $data = PngPaths::readJson($path);
        return [
            'visits'   => array_values(array_map('strval', (array) ($data['visits'] ?? []))),
            'subjects' => array_values(array_map('strval', (array) ($data['subjects'] ?? []))),
            'issues'   => (array) ($data['issues'] ?? []),
        ];
    }

    /**
     * One printable line for a scan problem: where, what, how to fix.
     */
    public static function line(array $p): string
    {
        return ($p['where'] ?? '?') . ': ' . ($p['message'] ?? '')
            . (!empty($p['fix']) ? ' -> ' . $p['fix'] : '');
    }
}
