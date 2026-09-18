<?php
declare(strict_types=1);

namespace LORIS\Utils;

/**
 * Date-of-birth policy - the single source of truth for where DoB comes
 * from, how it is normalised and when it is valid. Used by every
 * pipeline that creates candidates (clinical, BIDS, DICOM, and any
 * future modality), so the rule cannot drift between them.
 *
 *   Source   : the row's DoB column (any of COLUMN_NAMES, case-
 *              insensitive), else project.json -> candidate_defaults.dob.
 *   Normalise: YYYY-MM-DD. A provided day is kept (agreed with Rida,
 *              Sept 11); missing day/month -> 01. See DateNormalizer.
 *   Required : DoB is needed to create a candidate. Blank or invalid
 *              after resolve + normalise -> the caller must NOT create
 *              the candidate (error and stop).
 *
 * Pure: no I/O.
 */
final class Dob
{
    /** Header names that hold DoB, lowercased. First match wins. */
    public const COLUMN_NAMES = ['dob', 'date_of_birth', 'birth_date'];

    /** project.json -> candidate_defaults key used as the fallback. */
    public const DEFAULT_KEY = 'dob';

    /** project.json key for the ambiguous day/month order ('mdy'|'dmy'). */
    public const ORDER_KEY = 'date_input_format';

    /** Default order - matches REDCap's date_mdy. */
    public const DEFAULT_ORDER = 'mdy';

    /** Accepted-formats hint for error messages. */
    public const FORMATS_HINT = 'YYYY-MM-DD, YYYY-MM, YYYY, M/D/YYYY, D/M/YYYY';

    /**
     * Is this header name a DoB column?
     */
    public static function isColumn(string $header): bool
    {
        return in_array(strtolower(trim($header)), self::COLUMN_NAMES, true);
    }

    /**
     * Index of the DoB column in a header row, or null.
     *
     * @param array<int,string> $headers
     */
    public static function columnIndex(array $headers): ?int
    {
        $lower = array_map(static fn($h) => strtolower(trim((string)$h)), $headers);
        foreach (self::COLUMN_NAMES as $name) {
            $i = array_search($name, $lower, true);
            if ($i !== false) {
                return (int)$i;
            }
        }
        return null;
    }

    /**
     * Raw DoB from an associative row (keys matched case-insensitively),
     * or '' when absent/blank.
     */
    public static function fromRow(array $row): string
    {
        $lower = array_change_key_case($row, CASE_LOWER);
        foreach (self::COLUMN_NAMES as $name) {
            $v = trim((string)($lower[$name] ?? ''));
            if ($v !== '') {
                return $v;
            }
        }
        return '';
    }

    /**
     * Raw DoB for a row: row first, then candidate_defaults.dob.
     *
     * @return array{0: string, 1: string} [raw value, source] where
     *         source is 'row', 'candidate_defaults' or 'none'.
     */
    public static function resolve(array $row, array $candidateDefaults = []): array
    {
        $v = self::fromRow($row);
        if ($v !== '') {
            return [$v, 'row'];
        }
        $d = trim((string)($candidateDefaults[self::DEFAULT_KEY] ?? ''));
        if ($d !== '') {
            return [$d, 'candidate_defaults'];
        }
        return ['', 'none'];
    }

    /**
     * THE entry point for every pipeline: source + normalise + validate
     * in one call, so a policy change here applies to all modalities.
     *
     * @param array $row           One participant / data row (assoc).
     * @param array $projectConfig Decoded project.json (candidate_defaults,
     *                             date_input_format).
     * @return array{value: string, raw: string, source: string, problem: ?string}
     *         value   normalised YYYY-MM-DD (or the raw value if invalid)
     *         raw     as found, before normalising
     *         source  'row', 'candidate_defaults' or 'none'
     *         problem null when usable; else why candidate creation must stop
     */
    public static function prepare(array $row, array $projectConfig = []): array
    {
        [$raw, $source] = self::resolve($row, $projectConfig['candidate_defaults'] ?? []);
        $value = self::normalize($raw, self::dateOrder($projectConfig));
        return [
            'value'   => $value,
            'raw'     => $raw,
            'source'  => $source,
            'problem' => self::problem($value),
        ];
    }

    /**
     * Day/month order for ambiguous dates, from project.json.
     */
    public static function dateOrder(array $projectConfig = []): string
    {
        $o = strtolower(trim((string)($projectConfig[self::ORDER_KEY] ?? self::DEFAULT_ORDER)));
        return in_array($o, ['mdy', 'dmy'], true) ? $o : self::DEFAULT_ORDER;
    }

    /**
     * Normalise a raw DoB to YYYY-MM-DD, keeping a provided day.
     *
     * @param string $order 'mdy' or 'dmy' (project.json -> date_input_format).
     */
    public static function normalize(string $value, string $order = self::DEFAULT_ORDER): string
    {
        return DateNormalizer::normalize($value, $order, true);
    }

    /**
     * True when a (normalised) DoB is a real YYYY-MM-DD date.
     */
    public static function isValid(?string $value): bool
    {
        return DateNormalizer::isValid($value);
    }

    /**
     * Why a normalised DoB cannot be used, or null when it can.
     */
    public static function problem(?string $value): ?string
    {
        if ($value === null || trim($value) === '') {
            return 'no dob';
        }
        return self::isValid($value) ? null : "invalid dob '{$value}'";
    }
}
