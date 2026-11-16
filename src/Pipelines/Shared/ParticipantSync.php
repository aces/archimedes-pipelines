<?php

/**
 * ParticipantSync
 *
 * Modality-independent participant handling for any delivery laid out as
 * sub-<id>/[ses-<visit>/] folders with a participants.tsv (DICOM, BIDS, ...):
 *
 *   load()      participants.tsv, enriched from project.json candidate_defaults
 *               (ParticipantsTsv), with the LORIS columns still missing
 *   orphans()   sub- folders with no participants.tsv row, and rows with no folder
 *               (orphanFolders(): the same on exact folder names)
 *   nestedSubjectDirs()  a sub- folder inside another sub- folder
 *   externalId()         external ID column aliases (as BidsParticipantSync)
 *   sync()      find each participant in LORIS by external ID, create it when
 *               missing (dry run: report only). Sex and dob are normalised by
 *               the API adapter, clinical style (sex_mappings, Dob).
 *
 * Pre-flight of the values themselves is ParticipantsCheck. The modality
 * script decides which participants are wanted (e.g. only those with data)
 * and what to print around it.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

use LORIS\Endpoints\LorisApiClientInterface;

final class ParticipantSync
{
    /** "sub-ARCHI0001" or "ARCHI0001" -> "ARCHI0001". */
    public static function subjectId(string $participantId): string
    {
        return str_starts_with($participantId, 'sub-') ? substr($participantId, 4) : $participantId;
    }

    /** participants.tsv columns accepted as the external ID - same list as BidsParticipantSync. */
    public const EXTERNAL_ID_COLUMNS = [
        'external_id', 'ExternalID', 'externalid', 'study_id', 'StudyID',
        'studyid', 'ext_study_id', 'ExtStudyID',
    ];

    /** External ID of a row: first non-empty alias column, else the subject identifier. */
    public static function externalId(array $row, string $subject): string
    {
        foreach (self::EXTERNAL_ID_COLUMNS as $col) {
            if (!empty($row[$col])) {
                return trim((string) $row[$col]);
            }
        }
        return $subject;
    }

    /**
     * sub-* folders nested inside another participant's folder - usually a copy
     * that landed one level too deep (same check as BidsParticipantSync).
     *
     * @return string[] "sub-A/.../sub-B" paths relative to $deliveryDir.
     */
    public static function nestedSubjectDirs(string $deliveryDir): array
    {
        $root   = rtrim($deliveryDir, '/');
        $nested = [];
        foreach (glob($root . '/sub-*', GLOB_ONLYDIR) ?: [] as $subjectDir) {
            $it = new \RecursiveIteratorIterator(
                new \RecursiveDirectoryIterator($subjectDir, \FilesystemIterator::SKIP_DOTS),
                \RecursiveIteratorIterator::SELF_FIRST
            );
            foreach ($it as $entry) {
                if ($entry->isDir() && str_starts_with($entry->getFilename(), 'sub-')) {
                    $nested[] = substr($entry->getPathname(), strlen($root) + 1);
                }
            }
        }
        sort($nested);
        return $nested;
    }

    /**
     * participants.tsv enriched from candidate_defaults.
     *
     * @return array{roster: ParticipantsTsv, missing: string[], enriched: string[]}
     *
     * @throws \RuntimeException when the file cannot be read.
     */
    public static function load(string $tsvPath, array $projectConfig): array
    {
        $roster = ParticipantsTsv::load($tsvPath, $projectConfig['candidate_defaults'] ?? [], $projectConfig);

        return [
            'roster'   => $roster,
            'missing'  => $roster->missingForLoris(),
            'enriched' => $roster->enrichedColumns(),
        ];
    }

    /**
     * Both orphan directions. Neither is fatal - a delivery can be partial -
     * but both are worth naming before candidates are created.
     *
     * Directories are read from disk, not from a manifest, so a sub- folder
     * that produced no usable data is still seen as delivered.
     *
     * @param ParticipantsTsv $roster      Loaded participants.tsv.
     * @param string          $deliveryDir Folder holding the sub-* folders.
     *
     * @return array{dirs: string[], rows: string[]} subject identifiers (no "sub-").
     */
    public static function orphans(ParticipantsTsv $roster, string $deliveryDir): array
    {
        $expected = [];
        foreach (array_keys($roster->all()) as $participantId) {
            $expected[] = 'sub-' . self::subjectId((string) $participantId);
        }

        $found = self::orphanFolders($expected, $deliveryDir);

        return [
            'dirs' => array_map(static fn ($d) => substr($d, 4), $found['dirs']),
            'rows' => array_map(static fn ($d) => substr($d, 4), $found['rows']),
        ];
    }

    /**
     * Orphan check on folder names, exactly as given: sub-* folders on disk
     * that are not expected, and expected names with no folder.
     *
     * @param string[] $expected    Folder names a participant should have
     *                              (e.g. participants.tsv participant_id).
     * @param string   $deliveryDir Folder holding the sub-* folders.
     *
     * @return array{dirs: string[], rows: string[]} dirs sorted; rows in $expected order.
     */
    public static function orphanFolders(array $expected, string $deliveryDir): array
    {
        $onDisk = array_map('basename', glob(rtrim($deliveryDir, '/') . '/sub-*', GLOB_ONLYDIR) ?: []);
        sort($onDisk);

        $want = array_flip(array_map('strval', $expected));
        $have = array_flip($onDisk);

        return [
            'dirs' => array_values(array_filter($onDisk, static fn ($d) => !isset($want[$d]))),
            'rows' => array_values(array_unique(array_filter(
                array_map('strval', $expected),
                static fn ($e) => !isset($have[$e])
            ))),
        ];
    }

    /**
     * Where a participants.tsv row no longer agrees with its candidate in LORIS.
     *
     * One rule for every modality and for the validator: dob and sex, after
     * the same normalisation as candidate creation. Read-only.
     *
     * @param LorisApiClientInterface $api      Adapter (project, sex_mappings set).
     * @param array                   $existing findCandidateByExternalId() result.
     * @param array                   $row      participants.tsv row (enriched).
     *
     * @return string[] One line per differing field; empty when they agree.
     */
    public static function lorisDifferences(LorisApiClientInterface $api, array $existing, array $row): array
    {
        return $api->demographicsDiffer(
            (string) ($existing['cand_id'] ?? ''),
            isset($row['dob']) ? (string) $row['dob'] : null,
            isset($row['sex']) ? (string) $row['sex'] : null
        );
    }

    /**
     * Find or create a LORIS candidate for each participants.tsv row.
     *
     * @param ParticipantsTsv         $roster   Loaded participants.tsv.
     * @param LorisApiClientInterface $api      Adapter (project, site, sex_mappings set).
     * @param array|null              $wanted   subject => true to restrict to; null = all rows.
     * @param array                   $excluded subject => true never to create (issues reported).
     * @param bool                    $confirm  false = dry run: nothing created.
     * @param callable                $out      fn(string $line): void, progress lines.
     * @param object|null             $logger   PSR-3 style logger for errors.
     * @param bool                    $verbose  Also print matched / excluded rows.
     *
     * @return array{matched: int, created: int, skipped: int, failed: int, differs: string[]}
     *         differs: one warning per matched candidate whose dob / sex in
     *         participants.tsv no longer agree with LORIS (LORIS not changed).
     */
    public static function sync(
        ParticipantsTsv $roster,
        LorisApiClientInterface $api,
        ?array $wanted,
        array $excluded,
        bool $confirm,
        callable $out,
        ?object $logger = null,
        bool $verbose = false
    ): array {
        $n = ['matched' => 0, 'created' => 0, 'skipped' => 0, 'failed' => 0, 'differs' => []];

        foreach ($roster->all() as $participantId => $row) {
            $subject = self::subjectId((string) $participantId);

            if (isset($excluded[$subject])) {
                if ($verbose) {
                    $out("    {$subject}  excluded (issue reported) - not created");
                }
                $n['skipped']++;
                continue;
            }

            if ($wanted !== null && !isset($wanted[$subject])) {
                $n['skipped']++;
                continue;
            }

            $externalId = self::externalId($row, $subject);

            try {
                $existing = $api->findCandidateByExternalId($externalId);
            } catch (\Throwable $e) {
                $out("  ERROR lookup {$externalId}: " . $e->getMessage());
                $logger?->error("Lookup failed for {$externalId}: " . $e->getMessage());
                $n['failed']++;
                continue;
            }

            if ($existing !== null) {
                $n['matched']++;
                if ($verbose) {
                    $out(sprintf("    %-16s matched %s (%s)", $externalId, $existing['pscid'], $existing['cand_id']));
                }

                // An edited participants.tsv must not go unnoticed. The
                // candidate is not changed here (that belongs to the
                // participant metadata pipeline or LORIS); it is reported,
                // and the participant's imaging still goes in.
                try {
                    $diff = self::lorisDifferences($api, $existing, $row);
                } catch (\Throwable $e) {
                    $diff = [];
                    $logger?->warning("dob/sex compare failed for {$externalId}: " . $e->getMessage());
                }
                if ($diff !== []) {
                    $line = sprintf(
                        '%s (%s): participants.tsv differs from LORIS - %s. LORIS not changed; '
                        . 'correct the candidate in LORIS (or the participant metadata pipeline), '
                        . 'or fix participants.tsv',
                        $externalId,
                        $existing['pscid'],
                        implode('; ', $diff)
                    );
                    $out("    WARN {$line}");
                    $logger?->warning($line);
                    $n['differs'][] = $line;
                }
                continue;
            }

            if (!$confirm) {
                $out("    {$externalId}  would create");
                $n['created']++;
                continue;
            }

            try {
                $candidate = $api->createCandidate($subject, $externalId, $row['dob'] ?? null, $row['sex'] ?? null);
                $n['created']++;
                $out(sprintf("    %-16s created %s (%s)", $externalId, $candidate['pscid'], $candidate['cand_id']));
            } catch (\Throwable $e) {
                $out("  ERROR create {$externalId}: " . $e->getMessage());
                $logger?->error("Creation failed for {$externalId}: " . $e->getMessage());
                $n['failed']++;
            }
        }

        return $n;
    }
}
