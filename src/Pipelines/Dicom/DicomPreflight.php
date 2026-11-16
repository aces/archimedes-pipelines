<?php

/**
 * DicomPreflight
 *
 * Every data issue in a DICOM delivery - from organize (the files) and from
 * participants.tsv and the LORIS configuration - found in one pass, BEFORE any
 * candidate or session is created. Each issue says what it affects, so the
 * bundle can report them all and ask whether to continue with the good
 * studies only:
 *
 *   project  - nothing can safely continue (participants.tsv unreadable or
 *              missing a needed column, configured visits unreadable, a file
 *              problem organize could not place). Always aborts.
 *   subject  - one participant (sex/dob that cannot be normalised, identifier
 *              not letters and digits). All of that participant's studies are
 *              excluded and no candidate is created for it.
 *   study    - one examination (PatientName mismatch, study in two folders,
 *              conflicting instances, visit not configured, bad visit label).
 *   place    - one sub-/ses- folder (truncated or corrupt files there):
 *              every study in that folder is excluded.
 *
 * check() also takes an exclusion list (from the bundle, after the operator
 * chose to continue with the good studies): issues about excluded studies or
 * participants are then not reported as errors again.
 *
 * The participants.tsv checks are shared with other modalities
 * (ParticipantsCheck); this class adds the DICOM parts: organize's file
 * problems, unlinked studies, and mapping issues to studies.
 *
 * Reads only: the organized manifest, participants.tsv, and the project's
 * configured visit labels from LORIS (a GET).
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Dicom;

use LORIS\Pipelines\Shared\ParticipantsCheck;
use LORIS\Pipelines\Shared\ParticipantsTsv;

final class DicomPreflight
{
    /**
     * @param string        $projectPath   Project root.
     * @param array         $projectConfig Decoded project.json.
     * @param callable|null $visitLabels   fn(): string[] configured visit labels.
     * @param array         $exclude       ['study_uids' => [], 'subjects' => []].
     * @param string|null   $manifestPath  Organized manifest to read; default
     *                                     processed/imaging/dicom_studies.json
     *                                     (the validator passes a temporary one).
     *
     * @return array{errors: string[], warnings: string[], issues: array, linked: int, unlinked: int}
     */
    public static function check(
        string $projectPath,
        array $projectConfig,
        ?callable $visitLabels,
        array $exclude = [],
        ?string $manifestPath = null
    ): array {
        $issues   = [];
        $warnings = [];

        $manifest = self::_manifest($projectPath, $manifestPath);
        $studies  = $manifest['studies'] ?? [];

        // --- organize: file-level problems -----------------------------------
        foreach ($manifest['problems'] ?? [] as $problem) {
            $message = 'organize: ' . ($problem['message'] ?? '');
            if (($problem['level'] ?? '') !== 'error') {
                $warnings[] = $message;
                continue;
            }
            if (!empty($problem['study_instance_uid'])) {
                $issues[] = ['scope' => 'study', 'uid' => $problem['study_instance_uid'], 'message' => $message];
            } elseif (array_key_exists('place_subject', $problem)) {
                $issues[] = [
                    'scope'   => 'place',
                    'subject' => $problem['place_subject'],
                    'session' => $problem['place_session'] ?? null,
                    'message' => $message,
                ];
            } else {
                $issues[] = ['scope' => 'project', 'message' => $message];
            }
        }

        $linked = array_values(array_filter($studies, static fn ($s) => ($s['link_status'] ?? '') === 'linked'));
        $counts = ['linked' => count($linked), 'unlinked' => 0];

        foreach ($studies as $s) {
            $status = $s['link_status'] ?? '';
            if ($status === 'unlinked') {
                $counts['unlinked']++;
                $warnings[] = sprintf(
                    'UNLINKED %s: %s',
                    $s['directory_name'] ?? '?',
                    self::unlinkedAdvice($s['link_reason'] ?? null, $s['subject'] ?? null)
                );
            }
        }

        // --- participants.tsv: shared, modality-independent checks ----------
        // (file and columns, identifiers, sex, dob, visit labels, configured
        // visits) - see ParticipantsCheck. Session-level issues apply to every
        // study filed under that sub-/ses- folder.
        if (!empty($linked)) {
            $tsv = ParticipantsTsv::locate($projectPath, 'dicom');
            if ($tsv === null) {
                $issues[] = ['scope' => 'project', 'message' => 'participants.tsv not found, but studies are filed under participant folders'];
            } else {
                $sessions = [];
                foreach ($linked as $s) {
                    $sessions[(string) ($s['subject'] ?? '')][] = (string) ($s['session'] ?? '');
                }
                $result = ParticipantsCheck::check(
                    $tsv,
                    $projectConfig,
                    $sessions,
                    $visitLabels,
                    array_column($studies, 'subject'),
                    'imaging'
                );
                $warnings = array_merge($warnings, $result['warnings']);

                foreach ($result['issues'] as $issue) {
                    if ($issue['scope'] !== 'session') {
                        $issues[] = $issue;
                        continue;
                    }
                    foreach ($linked as $s) {
                        if ((string) ($s['subject'] ?? '') === $issue['subject']
                            && (string) ($s['session'] ?? '') === $issue['session']
                        ) {
                            $issues[] = ['scope' => 'study', 'uid' => $s['study_instance_uid'] ?? null,
                                'message' => $issue['message']];
                        }
                    }
                }
            }
        }

        // Issues already excluded by the operator's "continue with the good
        // studies" are not errors any more for this run.
        $excludedUids     = array_flip($exclude['study_uids'] ?? []);
        $excludedSubjects = array_flip($exclude['subjects'] ?? []);
        $open = array_values(array_filter($issues, static function (array $i) use ($excludedUids, $excludedSubjects, $studies): bool {
            return match ($i['scope']) {
                'study'   => !isset($excludedUids[(string) ($i['uid'] ?? '')]),
                'subject' => !isset($excludedSubjects[(string) ($i['subject'] ?? '')]),
                'place'   => !self::_placeExcluded($i, $studies, $excludedUids),
                default   => true,
            };
        }));

        return [
            'errors'   => array_column($open, 'message'),
            'warnings' => array_values(array_unique($warnings)),
            'issues'   => $issues,
        ] + $counts;
    }

    /**
     * Split the delivery into good and bad studies from a list of issues.
     *
     * @return array{
     *     abort: string[],          project-level issues: nothing may continue
     *     exclude_uids: string[],   every study that must not go any further
     *     exclude_subjects: string[], participants that must not be created
     *     bad: array<string, string[]>, label => reasons, for the report
     *     good: string[]            organized study folders that remain
     * }
     */
    public static function plan(string $projectPath, array $issues, ?string $manifestPath = null): array
    {
        $studies = self::_manifest($projectPath, $manifestPath)['studies'] ?? [];
        $abort = $excludeUids = $excludeSubjects = $bad = [];

        $label = static fn (array $s): string => $s['directory_name'] ?? ($s['study_instance_uid'] ?? '?');

        foreach ($issues as $i) {
            switch ($i['scope']) {
                case 'project':
                    $abort[] = $i['message'];
                    break;
                case 'subject':
                    $excludeSubjects[(string) $i['subject']] = true;
                    $hit = false;
                    foreach ($studies as $s) {
                        if ((string) ($s['subject'] ?? '') === (string) $i['subject']) {
                            $excludeUids[$s['study_instance_uid']] = true;
                            $bad[$label($s)][] = $i['message'];
                            $hit = true;
                        }
                    }
                    if (!$hit) {
                        $bad['sub-' . $i['subject']][] = $i['message'];
                    }
                    break;
                case 'place':
                    $hit = false;
                    foreach ($studies as $s) {
                        if (($s['subject'] ?? null) === $i['subject'] && ($s['session'] ?? null) === $i['session']) {
                            $excludeUids[$s['study_instance_uid']] = true;
                            $bad[$label($s)][] = $i['message'];
                            $hit = true;
                        }
                    }
                    if (!$hit) {
                        $bad[sprintf('sub-%s/ses-%s', $i['subject'] ?? '?', $i['session'] ?? '?')][] = $i['message'];
                    }
                    break;
                default: // study
                    $uid = (string) ($i['uid'] ?? '');
                    $excludeUids[$uid] = true;
                    $name = null;
                    foreach ($studies as $s) {
                        if (($s['study_instance_uid'] ?? '') === $uid) {
                            $name = $label($s);
                        }
                    }
                    // A study organize refused is not in the manifest: name it by
                    // the folder its message starts with, else by UID.
                    if ($name === null && preg_match('#^organize: (sub-[^:\s]+(?:/ses-[^:\s]+)?):#', $i['message'], $m)) {
                        $name = $m[1];
                    }
                    $bad[$name ?? ('study ' . $uid)][] = $i['message'];
            }
        }

        // A participant whose every study is excluded is not created either.
        $bySubject = [];
        foreach ($studies as $s) {
            if (($s['link_status'] ?? '') === 'linked') {
                $bySubject[(string) $s['subject']][] = isset($excludeUids[$s['study_instance_uid']]);
            }
        }
        foreach ($bySubject as $subject => $flags) {
            if (!in_array(false, $flags, true)) {
                $excludeSubjects[$subject] = true;
            }
        }

        $good = [];
        foreach ($studies as $s) {
            if (($s['link_status'] ?? '') === 'linked' && !isset($excludeUids[$s['study_instance_uid']])) {
                $good[] = $label($s);
            }
        }

        return [
            'abort'            => array_values(array_unique($abort)),
            'exclude_uids'     => array_keys($excludeUids),
            'exclude_subjects' => array_keys($excludeSubjects),
            'bad'              => array_map(static fn ($r) => array_values(array_unique($r)), $bad),
            'good'             => $good,
        ];
    }

    /**
     * What to do about an unlinked study, by reason. Unlinked studies are
     * only reported; once they match a participant they are processed.
     */
    public static function unlinkedAdvice(?string $reason, ?string $subject): string
    {
        $sub = $subject !== null && $subject !== '' ? "sub-{$subject}" : 'the participant';

        return match ($reason) {
            'not_in_participants_tsv' => "no matching participant in participants.tsv - add {$sub} to "
                . 'participants.tsv (its ses- folder must be a visit configured in LORIS) and re-run',
            'no_participants_tsv'     => "no participants.tsv in the delivery - add one with a row for {$sub} and re-run",
            'not_organized'           => 'not inside a sub-<ID>/ses-<visit>/ folder - move it into one, add the '
                . 'participant to participants.tsv and re-run',
            'no_session_folder'       => "no ses-<visit> folder - move it into {$sub}/ses-<visit>/ (a configured visit) and re-run",
            'no_sessions_tsv'         => "no sessions file in {$sub}/ - add {$sub}/sessions.tsv (or {$sub}_sessions.tsv) with one row per visit (session_id, e.g. ses-V01) and re-run",
            'not_in_sessions_tsv'     => "visit not listed in {$sub}'s sessions file (sessions.tsv or {$sub}_sessions.tsv) - add a row for it (session_id ses-<visit>) and re-run",
            default                   => "add {$sub} to participants.tsv with a valid, configured session and re-run",
        } . '; otherwise it stays reported, nothing is done with it';
    }

    /**
     * Read an exclusion file written by the bundle (empty when none/invalid).
     *
     * @return array{study_uids: string[], subjects: string[]}
     */
    public static function readExclusions(?string $path): array
    {
        if ($path === null || $path === '' || !is_file($path)) {
            return ['study_uids' => [], 'subjects' => [], 'labels' => []];
        }
        $data = json_decode((string) file_get_contents($path), true) ?: [];

        return [
            'study_uids' => array_values(array_map('strval', (array) ($data['study_uids'] ?? []))),
            'subjects'   => array_values(array_map('strval', (array) ($data['subjects'] ?? []))),
            'labels'     => array_values(array_map('strval', array_keys((array) ($data['issues'] ?? [])))),
        ];
    }

    private static function _placeExcluded(array $issue, array $studies, array $excludedUids): bool
    {
        $any = false;
        foreach ($studies as $s) {
            if (($s['subject'] ?? null) === $issue['subject'] && ($s['session'] ?? null) === $issue['session']) {
                $any = true;
                if (!isset($excludedUids[$s['study_instance_uid']])) {
                    return false;
                }
            }
        }

        // A place with no organized study left (all its files unreadable) has
        // nothing to ingest; once the operator continued, it is not an error.
        return $any || !empty($excludedUids);
    }

    private static function _manifest(string $projectPath, ?string $path = null): array
    {
        $path ??= $projectPath . '/processed/imaging/dicom_studies.json';

        return is_file($path) ? (json_decode((string) file_get_contents($path), true) ?: []) : [];
    }
}
