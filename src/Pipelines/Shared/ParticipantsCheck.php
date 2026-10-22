<?php

/**
 * ParticipantsCheck
 *
 * Modality-independent pre-flight of participants.tsv, run BEFORE any
 * candidate or session is created. Any modality that delivers sub-/ses-
 * folders with a participants.tsv (DICOM, BIDS, ...) can call it; the caller
 * supplies which participants and visits it found in its own delivery.
 *
 * Checks, with the same normalisation candidate creation uses (clinical style):
 *   - participants.tsv readable, and every LORIS column present either in the
 *     file or in project.json candidate_defaults      -> scope 'project'
 *   - participant identifier letters and digits only   -> scope 'subject'
 *   - sex maps to Male/Female/Other (baseline + project.json sex_mappings)
 *   - dob normalises to a valid date (Dob, project date order)
 *   - visit label letters and digits only              -> scope 'session'
 *   - visit configured in LORIS for the project        -> scope 'session'
 *   - participants.tsv rows with no data in the delivery -> warning
 *
 * Reads only: participants.tsv and (through the callable) the configured
 * visit labels. Creates nothing.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

use LORIS\Endpoints\LorisApiClientAdapter;

use LORIS\Utils\Dob;

final class ParticipantsCheck
{
    /**
     * @param string        $tsvPath       participants.tsv (see ParticipantsTsv::locate()).
     * @param array         $projectConfig Decoded project.json.
     * @param array         $sessions      subject (no "sub-") => visit labels (no "ses-")
     *                                     the delivery has data for.
     * @param callable|null $visitLabels   fn(): string[] known visit labels (used in LORIS + project.json "visits"); null skips that check.
     * @param array         $delivered     Every subject seen in the delivery, for the
     *                                     "row with no data" warning (defaults to $sessions keys).
     * @param string        $dataNoun      Word used in that warning ("imaging", "BIDS data", ...).
     *
     * @return array{
     *     issues: array<int, array{scope: string, message: string, subject?: string, session?: string}>,
     *     warnings: string[],
     *     roster: ?ParticipantsTsv
     * }
     */
    /**
     * Each LORIS-required value: missing everywhere (an error), or not in
     * participants.tsv but taken from project.json (reported, OK).
     *
     * Shared wording for the pre-flight, the participant sync and the
     * validator, so a site sees the same message wherever it is reported.
     *
     * @param ParticipantsTsv $roster Loaded (and enriched) participants.tsv.
     *
     * @return array{missing: string[], defaults: string[]}
     */
    public static function columnSources(ParticipantsTsv $roster): array
    {
        $out   = ['missing' => [], 'defaults' => []];
        $total = $roster->count();

        foreach ($roster->columnSources() as $column => $s) {
            if ($s['empty_rows'] === []) {
                continue;                               // every row has it in the file
            }
            $n     = count($s['empty_rows']);
            $where = $s['in_file']
                ? sprintf("participants.tsv: '%s' is empty for %d of %d row(s) (%s)", $column, $n, $total,
                    implode(', ', array_slice($s['empty_rows'], 0, 5)) . ($n > 5 ? ', ...' : ''))
                : "participants.tsv has no '{$column}' column";

            if ($s['from'] === null) {
                $out['missing'][] = "{$where}, and project.json candidate_defaults has no '{$column}'"
                    . " - add the {$column} " . ($s['in_file'] ? 'values' : 'column')
                    . ", or set candidate_defaults.{$column}";
            } elseif ($s['from'] === 'participant_id') {
                $out['defaults'][] = "{$where} - using participant_id without 'sub-' for "
                    . ($s['in_file'] ? 'those rows' : "all {$total} participant(s)") . ' (OK)';
            } else {
                $out['defaults'][] = "{$where} - using project.json {$s['from']} = '{$s['value']}' for "
                    . ($s['in_file'] ? 'those rows' : "all {$total} participant(s)") . ' (OK)';
            }
        }

        return $out;
    }

    public static function check(
        string $tsvPath,
        array $projectConfig,
        array $sessions,
        ?callable $visitLabels = null,
        ?array $delivered = null,
        string $dataNoun = 'data'
    ): array {
        $issues   = [];
        $warnings = [];
        $roster   = null;

        // --- the file and its columns ----------------------------------------
        try {
            $roster = ParticipantsTsv::load($tsvPath, $projectConfig['candidate_defaults'] ?? [], $projectConfig);
            foreach (self::columnSources($roster)['missing'] as $message) {
                $issues[] = ['scope' => 'project', 'message' => $message];
            }
        } catch (\Throwable $e) {
            $issues[] = ['scope' => 'project', 'message' => 'participants.tsv: ' . $e->getMessage()];
        }

        // --- each participant with data: identifier, sex, dob, visit labels ---
        foreach ($sessions as $subject => $visits) {
            $subject = (string) $subject;

            if ($subject === '' || !preg_match('/^[A-Za-z0-9]+$/', $subject)) {
                $issues[] = ['scope' => 'subject', 'subject' => $subject,
                    'message' => "participant folder sub-{$subject}: identifier must be letters and digits only"];
            }

            $row = $roster?->get('sub-' . $subject) ?? $roster?->get($subject);
            if ($row !== null) {
                $sex = (string) ($row['sex'] ?? '');
                if (LorisApiClientAdapter::normaliseSexValue($sex, $projectConfig['sex_mappings'] ?? []) === null) {
                    $issues[] = ['scope' => 'subject', 'subject' => $subject,
                        'message' => "sub-{$subject}: sex '{$sex}' cannot be mapped to Male, Female or Other"
                            . ' - fix it, or add it to project.json sex_mappings'];
                }
                $dob     = Dob::normalize((string) ($row['dob'] ?? ''), Dob::dateOrder($projectConfig));
                $problem = Dob::problem($dob);
                if ($problem !== null) {
                    $issues[] = ['scope' => 'subject', 'subject' => $subject,
                        'message' => "sub-{$subject}: {$problem} (dob '{$row['dob']}'; accepted: "
                            . Dob::FORMATS_HINT . ')'];
                }
            }

            foreach (array_unique(array_map('strval', (array) $visits)) as $session) {
                if ($session !== '' && !preg_match('/^[A-Za-z0-9]+$/', $session)) {
                    $issues[] = ['scope' => 'session', 'subject' => $subject, 'session' => $session,
                        'message' => "sub-{$subject}/ses-{$session}: visit label must be letters and digits only"];
                }
            }
        }

        // --- visits: project.json "visits" ----------------------------------------
        // The LORIS API cannot list a project's configured visits (only those
        // existing sessions use), so the project may declare them. The list is
        // optional. Declared: folder names are matched to its spellings, and a
        // delivered visit not in it is a warning here and its studies are held
        // back by the reidentifier (the rest go in) - never the whole project.
        // Not declared: LORIS validates each visit against the cohort when the
        // session is created; a refused visit's studies are held back the same way.
        if (!empty($sessions) && $visitLabels !== null) {
            try {
                $declaredRaw = array_values(array_map('strval', (array) $visitLabels()));
                $declared    = array_map('strtolower', $declaredRaw);
                $seen        = [];
                $unlisted    = [];
                foreach ($sessions as $subject => $visits) {
                    foreach (array_unique(array_map('strval', (array) $visits)) as $session) {
                        if ($session === '') {
                            continue;
                        }
                        $seen[$session] = true;
                        if ($declared !== [] && !in_array(strtolower($session), $declared, true)) {
                            $unlisted[$session][] = "sub-{$subject}";
                        }
                    }
                }
                foreach ($unlisted as $session => $subjects) {
                    $warnings[] = sprintf(
                        "visit '%s' (%s) is not in project.json \"visits\" (%s) - its studies are held back "
                        . '(not ingested; the rest go in). Fix the ses- folder name, or add the visit to "visits" '
                        . '(and set it up in LORIS), then re-run',
                        $session,
                        implode(', ', array_slice($subjects, 0, 5)) . (count($subjects) > 5 ? ', ...' : ''),
                        implode(', ', $declaredRaw)
                    );
                }
                if ($declared === [] && $seen !== []) {
                    $warnings[] = 'project.json has no "visits" - the delivered visits ('
                        . implode(', ', array_slice(array_keys($seen), 0, 8)) . (count($seen) > 8 ? ', ...' : '')
                        . ') are checked by LORIS when each session is created. Optional: add "visits": [...] '
                        . 'to project.json to list them here';
                }
            } catch (\Throwable $e) {
                $issues[] = ['scope' => 'project', 'message' => 'cannot read the project visits: ' . $e->getMessage()];
            }
        }

        // --- rows with no data in this delivery -------------------------------
        if ($roster !== null) {
            $seen = array_flip(array_map('strval', $delivered ?? array_keys($sessions)));
            foreach (array_keys($roster->all()) as $pid) {
                $bare = str_starts_with((string) $pid, 'sub-') ? substr((string) $pid, 4) : (string) $pid;
                if (!isset($seen[$bare])) {
                    $warnings[] = "participants.tsv row {$pid} has no {$dataNoun} in this delivery";
                }
            }
        }

        return ['issues' => $issues, 'warnings' => $warnings, 'roster' => $roster];
    }
}