<?php

declare(strict_types=1);

namespace LORIS\Utils;

/**
 * Visit policy for the imaging pipelines (DICOM, PNG, BIDS): the visit is the
 * ses-<visit> folder name, and it must be the LORIS visit label exactly.
 *
 *   ses-V01 folder  =  sessions.tsv session_id ses-V01  =  project.json "visits" V01
 *
 * Nothing is renamed on the way in: a folder that differs from a declared
 * visit (ses-v01, ses-Visit1) is flagged and its data held back, with the
 * rename to make when one is obvious (hint()). When project.json declares no
 * "visits", the folder name is taken as is (LORIS checks it against the
 * cohort when the session is created).
 *
 * The clinical pipeline keeps its own visit_mappings rule; imaging does not
 * map visits. hint() only reads visit_mappings to suggest the rename.
 *
 * Pure: no I/O.
 */
final class VisitLabel
{
    /** project.json key of the visits a project declares. */
    public const VISITS_KEY = 'visits';

    /** project.json key of the clinical source value => LORIS label mappings. */
    public const MAPPINGS_KEY = 'visit_mappings';

    /**
     * The LORIS visit label for a ses- folder name, or null when project.json
     * declares visits and the name is not exactly one of them.
     *
     * @param array $projectConfig Decoded project.json ("visits").
     */
    public static function resolve(string $value, array $projectConfig): ?string
    {
        $value = trim($value);
        if ($value === '') {
            return null;
        }
        $declared = self::declared($projectConfig);
        if ($declared === []) {
            return $value;
        }
        return in_array($value, $declared, true) ? $value : null;
    }

    /**
     * For a name resolve() refuses: the declared visit it was most likely
     * meant to be (same letters in another case, or a visit_mappings entry),
     * or null when there is no obvious one.
     */
    public static function hint(string $value, array $projectConfig): ?string
    {
        $value    = trim($value);
        $declared = self::declared($projectConfig);
        foreach ($declared as $label) {
            if (strcasecmp($label, $value) === 0) {
                return $label;
            }
        }
        $mapped = self::mappings($projectConfig)[strtolower($value)] ?? null;
        return $mapped !== null && in_array($mapped, $declared, true) ? $mapped : null;
    }

    /**
     * Why a ses- folder name is refused, for messages: "ses-v01 is not exactly
     * a project.json visit - rename it ses-V01" or "... is not in project.json
     * \"visits\" (V01, V02)". Null when resolve() accepts it.
     */
    public static function problem(string $value, array $projectConfig): ?string
    {
        if (self::resolve($value, $projectConfig) !== null) {
            return null;
        }
        $hint = self::hint($value, $projectConfig);
        if ($hint !== null) {
            return "ses-{$value} is not exactly the LORIS visit '{$hint}' - rename it ses-{$hint}";
        }
        return "visit '{$value}' is not in project.json \"visits\" ("
            . implode(', ', self::declared($projectConfig)) . ')';
    }

    /**
     * Declared visit labels (project.json "visits"), trimmed, blanks dropped.
     *
     * @return string[]
     */
    public static function declared(array $projectConfig): array
    {
        return array_values(array_filter(
            array_map(static fn ($v): string => trim((string) $v), (array) ($projectConfig[self::VISITS_KEY] ?? [])),
            static fn (string $v): bool => $v !== ''
        ));
    }

    /**
     * visit_mappings with lowercased keys, blanks dropped (rename hints only).
     *
     * @return array<string, string>
     */
    public static function mappings(array $projectConfig): array
    {
        $out = [];
        foreach ((array) ($projectConfig[self::MAPPINGS_KEY] ?? []) as $from => $to) {
            $from = strtolower(trim((string) $from));
            $to   = trim((string) $to);
            if ($from !== '' && $to !== '') {
                $out[$from] = $to;
            }
        }
        return $out;
    }
}
