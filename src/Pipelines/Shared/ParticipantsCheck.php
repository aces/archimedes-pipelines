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
     * @param callable|null $visitLabels   fn(): string[] configured visit labels; null skips that check.
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
            $roster  = ParticipantsTsv::load($tsvPath, $projectConfig['candidate_defaults'] ?? [], $projectConfig);
            $missing = $roster->missingForLoris();
            if (!empty($missing)) {
                $issues[] = ['scope' => 'project', 'message' => 'participants.tsv: column(s) missing and not in candidate_defaults: '
                    . implode(', ', $missing)];
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

        // --- visits configured in LORIS ---------------------------------------
        if (!empty($sessions) && $visitLabels !== null) {
            try {
                $configured = array_map('strtolower', (array) $visitLabels());
                foreach ($sessions as $subject => $visits) {
                    foreach (array_unique(array_map('strval', (array) $visits)) as $session) {
                        if ($session !== '' && !in_array(strtolower($session), $configured, true)) {
                            $issues[] = ['scope' => 'session', 'subject' => (string) $subject, 'session' => $session,
                                'message' => "sub-{$subject}/ses-{$session}: visit '{$session}' is not configured in "
                                    . 'LORIS for this project - ask for it to be added, or fix the folder name'];
                        }
                    }
                }
            } catch (\Throwable $e) {
                $issues[] = ['scope' => 'project', 'message' => 'cannot read the configured visits from LORIS: ' . $e->getMessage()];
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
