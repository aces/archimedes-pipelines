<?php

declare(strict_types=1);

namespace LORIS\Utils;

/**
 * Sex policy for the imaging pipelines (DICOM, PNG, BIDS, and any future
 * modality): where a participant's sex is read from and how it is mapped to
 * the three values LORIS accepts (Male / Female / Other). The same rule as
 * the clinical pipeline (ClinicalPipeline::SEX_VALUE_MAP / SEX_COLUMN_NAMES,
 * which keeps its own copy). Companion of LORIS\Utils\Dob.
 *
 *   Source : the row's sex column (any of COLUMN_NAMES, case-insensitive).
 *   Map    : VALUE_MAP (case-insensitive), with project.json -> sex_mappings
 *            merged OVER it, e.g. {"1": "Male", "2": "Female"}.
 *   Unknown: a value that maps to nothing is NOT guessed - normalize()
 *            returns null and the caller reports it (fix the value, or add
 *            it to sex_mappings).
 *
 * Pure: no I/O.
 */
final class Sex
{
    /** Header names that hold the sex, lowercased. First match wins. */
    public const COLUMN_NAMES = ['sex', 'gender'];

    /** The values LORIS accepts. */
    public const LORIS_VALUES = ['Male', 'Female', 'Other'];

    /**
     * Baseline source encodings (lowercased) => LORIS value. A project adds
     * its own codes in project.json -> sex_mappings.
     */
    public const VALUE_MAP = [
        'm' => 'Male',   'male'   => 'Male',
        'f' => 'Female', 'female' => 'Female',
        'o' => 'Other',  'other'  => 'Other',
    ];

    /**
     * Index of the sex column in a header row, or null.
     *
     * @param array<int, string> $headers
     */
    public static function columnIndex(array $headers): ?int
    {
        $lower = array_map(static fn ($h) => strtolower(trim((string) $h)), $headers);
        foreach (self::COLUMN_NAMES as $name) {
            $i = array_search($name, $lower, true);
            if ($i !== false) {
                return (int) $i;
            }
        }
        return null;
    }

    /**
     * Raw sex from an associative row (keys matched case-insensitively), or ''.
     */
    public static function fromRow(array $row): string
    {
        $lower = array_change_key_case($row, CASE_LOWER);
        foreach (self::COLUMN_NAMES as $name) {
            $v = trim((string) ($lower[$name] ?? ''));
            if ($v !== '') {
                return $v;
            }
        }
        return '';
    }

    /**
     * The baseline merged with a project's sex_mappings, keys lowercased.
     * Mappings to anything but Male/Female/Other are ignored.
     *
     * @param array<string, string> $mappings project.json -> sex_mappings
     *
     * @return array<string, string>
     */
    public static function map(array $mappings = []): array
    {
        $map = self::VALUE_MAP;
        foreach ($mappings as $from => $to) {
            $from = strtolower(trim((string) $from));
            if ($from !== '' && in_array((string) $to, self::LORIS_VALUES, true)) {
                $map[$from] = (string) $to;
            }
        }
        return $map;
    }

    /**
     * Male / Female / Other for a raw value, or null when blank or not mappable.
     *
     * @param array<string, string> $mappings project.json -> sex_mappings
     */
    public static function normalize(?string $value, array $mappings = []): ?string
    {
        $key = strtolower(trim((string) $value));
        if ($key === '') {
            return null;
        }
        return self::map($mappings)[$key] ?? null;
    }

    /**
     * Why a raw value cannot be used, or null when it can.
     *
     * @param array<string, string> $mappings project.json -> sex_mappings
     */
    public static function problem(?string $value, array $mappings = []): ?string
    {
        if (trim((string) $value) === '') {
            return 'no sex';
        }
        return self::normalize($value, $mappings) === null
            ? "sex '" . trim((string) $value) . "' cannot be mapped to Male, Female or Other"
                . ' - fix it, or add it to project.json sex_mappings'
            : null;
    }
}
