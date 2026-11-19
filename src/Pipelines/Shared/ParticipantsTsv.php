<?php

/**
 * ParticipantsTsv
 *
 * Shared reader for participants.tsv, used by both the BIDS and DICOM
 * pipelines. The file format, the required columns and the enrichment rules
 * are identical across modalities; only the directory it lives in differs.
 *
 * Enrichment fills LORIS-internal columns the submitting site has no way to
 * know - site, cohort, project - from project.json candidate_defaults, rather
 * than requiring them in the delivery. DoB follows the shared policy in
 * LORIS\Utils\Dob (any DoB column name, then candidate_defaults.dob). This generalises the behaviour already
 * in _enrichParticipantsTsv() on the BIDS side.
 *
 * Pure: no API calls, no writes. Both pipelines wrap it with their own
 * modality-specific validation.
 *
 * PHP Version 8.1
 *
 * @category Pipeline
 * @package  Archimedes
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

use LORIS\Utils\Dob;
use LORIS\Utils\Sex;
use RuntimeException;

class ParticipantsTsv
{
    /**
     * Columns a site must supply. Everything else is enriched.
     */
    public const SITE_REQUIRED = ['participant_id'];

    /**
     * Columns LORIS needs before a candidate and its sessions can be created
     * (cohort: the session "Cohort"). Any of these
     * missing from the file is filled from candidate_defaults where possible.
     */
    public const LORIS_REQUIRED = ['participant_id', 'external_id', 'dob', 'sex', 'site', 'cohort'];

    /**
     * candidate_defaults key => participants.tsv column it fills.
     */
    private const DEFAULT_MAP = [
        'site'    => 'site',
        'cohort'  => 'cohort',
        'project' => 'project',
        'sex'     => 'sex',
    ];

    /**
     * Column => the header names it may arrive as, lowercased, in priority
     * order (Dob / Sex hold the clinical lists). Matched case-insensitively,
     * so DoB, DOB, Date_Of_Birth, Sex, Gender, Site, Cohort, Project,
     * ExtStudyID ... all work, for every modality.
     */
    public const COLUMN_ALIASES = [
        'participant_id' => ['participant_id'],
        'external_id'    => ['external_id', 'externalid', 'extstudyid', 'ext_study_id', 'study_id', 'studyid'],
        'dob'            => Dob::COLUMN_NAMES,
        'sex'            => Sex::COLUMN_NAMES,
        'site'           => ['site'],
        'cohort'         => ['cohort', 'group'],
        'project'        => ['project'],
    ];

    /** @var string[] Notes on values changed while normalising (as clinical). */
    private array $notes = [];

    /** @var array<string, array<string, string>> participant_id => row */
    private array $rows = [];

    /** @var string[] Column names as they appeared in the file. */
    private array $columns = [];

    /** @var string[] Columns that were enriched rather than supplied. */
    private array $enriched = [];

    /**
     * Where each LORIS-required value came from, recorded while enriching.
     *
     * @var array<string, array{in_file: bool, empty_rows: string[], from: ?string, value: ?string}>
     */
    private array $sources = [];

    /** @var string Path the data was read from. */
    private string $path;

    /** @var array<string, array> Rows as in the file, before normalising. */
    private array $delivered = [];

    private function __construct(string $path)
    {
        $this->path = $path;
    }

    /**
     * Read a participants.tsv and enrich it from candidate_defaults.
     *
     * @param string $path             Absolute path to participants.tsv.
     * @param array  $candidateDefaults project.json candidate_defaults block.
     * @param array  $projectConfig     Full project.json (date_input_format
     *                                  for DoB); optional.
     *
     * @throws RuntimeException on a missing, empty or malformed file.
     */
    public static function load(string $path, array $candidateDefaults = [], array $projectConfig = []): self
    {
        if (!file_exists($path)) {
            throw new RuntimeException("participants.tsv not found: {$path}");
        }

        $instance = new self($path);
        $instance->_read($path);
        $instance->_recordFileState($candidateDefaults);
        $instance->delivered = $instance->rows;
        $instance->_enrich($candidateDefaults, $projectConfig);

        return $instance;
    }

    /**
     * Find participants.tsv for a modality, without falling back across them.
     *
     * A project running both modalities keeps a separate list per delivery,
     * so BIDS and DICOM never read each other's file.
     *
     * @param string $projectPath Project root.
     * @param string $modality    'bids', 'dicom', or another modality's folder
     *                            under deidentified-raw/ (e.g. 'png', 'imaging/pngs').
     *
     * @return string|null Absolute path, or null when absent.
     */
    public static function locate(string $projectPath, string $modality): ?string
    {
        $root = rtrim($projectPath, '/');

        $path = match ($modality) {
            'bids'  => $root . '/deidentified-raw/bids/participants.tsv',
            'dicom' => $root . '/deidentified-raw/imaging/dicoms/participants.tsv',
            // Any other modality: its folder under deidentified-raw/, e.g.
            // 'png' or 'imaging/pngs' -> deidentified-raw/imaging/pngs/participants.tsv.
            default => preg_match('#^[A-Za-z0-9_-]+(/[A-Za-z0-9_-]+)*$#', $modality)
                ? $root . '/deidentified-raw/' . $modality . '/participants.tsv'
                : throw new RuntimeException("Unknown modality: {$modality}"),
        };

        return file_exists($path) ? $path : null;
    }

    /**
     * Why a delivered .tsv (participants.tsv, sessions.tsv ...) cannot be read
     * as tab-separated text, or null when it can. The usual export mistakes:
     * an Excel workbook renamed to .tsv, a comma/semicolon-separated file, an
     * empty file. A UTF-8 BOM and Windows line ends are fine.
     *
     * The message starts with "is ...", to follow the file name.
     */
    public static function formatProblem(string $path): ?string
    {
        $head = (string) @file_get_contents($path, false, null, 0, 4096);
        if ($head === '') {
            return 'is empty';
        }
        if (str_starts_with($head, "PK\x03\x04") || str_starts_with($head, "\xD0\xCF\x11\xE0")) {
            return 'is an Excel workbook, not a text file - save it as "Tab Delimited Text" (Excel) '
                . 'or "Tab-separated values" (Google Sheets)';
        }
        $first = strtok(str_replace("\r", '', ltrim($head, "\xEF\xBB\xBF")), "\n") ?: '';
        if (!str_contains($first, "\t") && (str_contains($first, ',') || str_contains($first, ';'))) {
            return 'is comma- or semicolon-separated - save it as "Tab Delimited Text" (Excel) '
                . 'or "Tab-separated values" (Google Sheets), not CSV';
        }
        return null;
    }

    /**
     * Header names with the known columns renamed to their canonical name
     * (COLUMN_ALIASES): the first alias present, case-insensitively, becomes
     * e.g. 'dob' or 'sex'. A canonical name already in the file is kept as is;
     * any other column keeps its own name.
     *
     * @param string[] $headers
     *
     * @return string[]
     */
    public static function canonicalColumns(array $headers): array
    {
        $lower = array_map(static fn ($h) => strtolower(trim((string) $h)), $headers);
        foreach (self::COLUMN_ALIASES as $canonical => $aliases) {
            if (in_array($canonical, $headers, true)) {
                continue;
            }
            foreach ($aliases as $alias) {
                $i = array_search($alias, $lower, true);
                if ($i !== false) {
                    $headers[$i] = $canonical;
                    $lower[$i]   = $canonical;
                    break;
                }
            }
        }
        return $headers;
    }

    /**
     * Parse a participants.tsv into rows, without judging them. Shared by every
     * modality (DICOM, BIDS, ...): tab-separated via fgetcsv (an empty first
     * cell stays empty - no column shift), values trimmed, short rows padded
     * and long rows cut to the header, blank lines skipped.
     *
     * @return array{
     *     columns: string[],
     *     rows: array<int, array{line: int, row: array<string, string>}>
     * } columns is [] when the file has no header line.
     *
     * @throws RuntimeException when the file cannot be opened.
     */
    public static function readRows(string $path): array
    {
        $handle = @fopen($path, 'r');
        if ($handle === false) {
            throw new RuntimeException("Could not open {$path}");
        }

        $header = fgetcsv($handle, 0, "\t");
        if ($header === false || $header === [null]) {
            fclose($handle);
            return ['columns' => [], 'rows' => []];
        }

        $columns = self::canonicalColumns(array_map(
            static fn ($c) => trim((string) $c, " \t\n\r\0\x0B\xEF\xBB\xBF"),
            $header
        ));
        $rows   = [];
        $lineNo = 1;

        while (($line = fgetcsv($handle, 0, "\t")) !== false) {
            $lineNo++;

            if ($line === [null]) {
                continue;
            }

            $values = array_map(static fn ($v) => trim((string) $v), $line);
            if (implode('', $values) === '') {
                continue;   // only tabs/spaces
            }

            // Tolerate short and long rows rather than failing the delivery;
            // sites export these from Excel and trailing tabs are common.
            $values = array_pad(array_slice($values, 0, count($columns)), count($columns), '');

            $rows[] = ['line' => $lineNo, 'row' => array_combine($columns, $values)];
        }

        fclose($handle);

        return ['columns' => $columns, 'rows' => $rows];
    }

    /**
     * Parse the file (strict: used by load()).
     */
    private function _read(string $path): void
    {
        $parsed = self::readRows($path);

        if ($parsed['columns'] === []) {
            throw new RuntimeException("participants.tsv is empty: {$path}");
        }

        $this->columns = $parsed['columns'];

        if (!in_array('participant_id', $this->columns, true)) {
            throw new RuntimeException(
                "participants.tsv has no participant_id column: {$path}"
            );
        }

        foreach ($parsed['rows'] as ['line' => $lineNo, 'row' => $row]) {
            $id = $row['participant_id'] ?? '';

            if ($id === '') {
                continue;
            }

            if (isset($this->rows[$id])) {
                throw new RuntimeException(
                    "Duplicate participant_id '{$id}' at line {$lineNo} of {$path}"
                );
            }

            $this->rows[$id] = $row;
        }

        if (empty($this->rows)) {
            throw new RuntimeException("No participant rows in {$path}");
        }
    }

    /**
     * Fill LORIS-internal columns from candidate_defaults.
     *
     * external_id defaults to participant_id with the sub- prefix stripped,
     * which is what the submission guide tells sites their identifier is.
     */
    private function _enrich(array $candidateDefaults, array $projectConfig = []): void
    {
        foreach (self::DEFAULT_MAP as $defaultKey => $column) {
            if (!isset($candidateDefaults[$defaultKey])) {
                continue;
            }

            $value   = $candidateDefaults[$defaultKey];
            $touched = false;

            foreach ($this->rows as $id => $row) {
                if (($row[$column] ?? '') === '') {
                    $this->rows[$id][$column] = $value;
                    $touched                  = true;
                }
            }

            if ($touched) {
                $this->enriched[] = $column;
                if (!in_array($column, $this->columns, true)) {
                    $this->columns[] = $column;
                }
            }
        }

        // site / cohort / project: when project.json candidate_defaults sets
        // one, it is the value used. A blank cell or a missing column takes
        // it (above); the same value in another case or spacing takes its
        // spelling; a different value is replaced by it and noted, so the
        // source can be corrected. Header names are matched in any case
        // (canonicalColumns: Site, COHORT, group, Project ...).
        foreach (['site', 'cohort', 'project'] as $column) {
            $configured = trim((string) ($candidateDefaults[$column] ?? ''));
            if ($configured === '') {
                continue;
            }
            $differs = [];
            foreach ($this->rows as $id => $row) {
                $v = trim((string) ($row[$column] ?? ''));
                if ($v === $configured) {
                    continue;
                }
                $this->rows[$id][$column] = $configured;
                if ($v !== '' && strcasecmp(preg_replace('/\s+/', ' ', $v), $configured) !== 0) {
                    $differs[$v][] = (string) $id;
                }
            }
            foreach ($differs as $v => $ids) {
                $this->notes[] = sprintf(
                    "participants.tsv %s '%s' (%s) is not project.json candidate_defaults.%s '%s' - "
                    . "'%s' is used; correct participants.tsv to avoid this note",
                    $column,
                    $v,
                    implode(', ', array_slice($ids, 0, 5)) . (count($ids) > 5 ? ', ...' : ''),
                    $column,
                    $configured,
                    $configured
                );
            }
        }

        // sex: shared policy (LORIS\Utils\Sex, as clinical) - mapped to
        // Male/Female/Other; a value it cannot map is left for the check to report.
        foreach ($this->rows as $id => $row) {
            $mapped = Sex::normalize((string) ($row['sex'] ?? ''), $projectConfig['sex_mappings'] ?? []);
            if ($mapped !== null) {
                $this->rows[$id]['sex'] = $mapped;
            }
        }

        // dob: shared policy (LORIS\Utils\Dob::prepare) - source and
        // normalisation - written to the canonical 'dob' column the DICOM
        // callers read. Blank stays blank for the DoB check.
        $dobCfg  = ['candidate_defaults' => $candidateDefaults] + $projectConfig;
        $touched = false;
        foreach ($this->rows as $id => $row) {
            $info = Dob::prepare($row, $dobCfg);
            if ($info['raw'] === '') {
                continue;
            }
            $this->rows[$id]['dob'] = $info['value'];
            if ($info['source'] === 'candidate_defaults') {
                $touched = true;
            }
        }
        if ($touched) {
            $this->enriched[] = 'dob';
        }
        if (!in_array('dob', $this->columns, true)
            && array_filter($this->rows, static fn($r) => ($r['dob'] ?? '') !== '') !== []
        ) {
            $this->columns[] = 'dob';
        }

        // external_id is derived, not configured.
        $touched = false;
        foreach ($this->rows as $id => $row) {
            if (($row['external_id'] ?? '') !== '') {
                continue;
            }

            $this->rows[$id]['external_id'] = str_starts_with($id, 'sub-')
                ? substr($id, 4)
                : $id;
            $touched = true;
        }

        if ($touched) {
            $this->enriched[] = 'external_id';
            if (!in_array('external_id', $this->columns, true)) {
                $this->columns[] = 'external_id';
            }
        }
    }

    /**
     * Before enrichment: which LORIS-required columns the file has, which rows
     * leave them empty, and what will fill them.
     */
    private function _recordFileState(array $candidateDefaults): void
    {
        foreach (self::LORIS_REQUIRED as $column) {
            if ($column === 'participant_id') {
                continue;
            }
            $empty = [];
            foreach ($this->rows as $id => $row) {
                if (trim((string) ($row[$column] ?? '')) === '') {
                    $empty[] = (string) $id;
                }
            }
            $default = $candidateDefaults[$column] ?? null;
            $from    = null;
            if ($column === 'external_id') {
                $from = 'participant_id';
            } elseif ($default !== null && trim((string) $default) !== '') {
                $from = "candidate_defaults.{$column}";
            }
            $this->sources[$column] = [
                'in_file'    => in_array($column, $this->columns, true),
                'empty_rows' => $empty,
                'from'       => $empty === [] ? null : $from,
                'value'      => ($empty !== [] && $from !== null && $column !== 'external_id') ? (string) $default : null,
            ];
        }
    }

    /**
     * Where each LORIS-required value comes from, for reporting.
     *
     * in_file    the column is in participants.tsv
     * empty_rows participant_ids whose value is empty in the file
     * from       what fills those rows: "candidate_defaults.<column>",
     *            "participant_id" (external_id), or null = nothing (missing)
     * value      the candidate_defaults value used, when from is candidate_defaults
     *
     * @return array<string, array{in_file: bool, empty_rows: string[], from: ?string, value: ?string}>
     */
    public function columnSources(): array
    {
        return $this->sources;
    }

    /**
     * Columns still missing after enrichment.
     *
     * @return string[] Empty when the file is ready for candidate creation.
     */
    public function missingForLoris(): array
    {
        $missing = [];

        foreach (self::LORIS_REQUIRED as $column) {
            foreach ($this->rows as $row) {
                if (($row[$column] ?? '') === '') {
                    $missing[] = $column;
                    break;
                }
            }
        }

        return $missing;
    }

    /**
     * @return array<string, array<string, string>> participant_id => row
     */
    public function all(): array
    {
        return $this->rows;
    }

    /**
     * Look up one participant by its participant_id, with or without sub-.
     */
    public function get(string $participantId): ?array
    {
        if (isset($this->rows[$participantId])) {
            return $this->rows[$participantId];
        }

        $prefixed = str_starts_with($participantId, 'sub-')
            ? $participantId
            : 'sub-' . $participantId;

        return $this->rows[$prefixed] ?? null;
    }

    public function has(string $participantId): bool
    {
        return $this->get($participantId) !== null;
    }

    public function count(): int
    {
        return count($this->rows);
    }

    /**
     * Columns filled from candidate_defaults rather than supplied by the site.
     *
     * @return string[]
     */
    public function enrichedColumns(): array
    {
        return array_values(array_unique($this->enriched));
    }

    public function path(): string
    {
        return $this->path;
    }

    /**
     * Values changed while normalising that the site should correct (e.g. a
     * project name that is not the configured one).
     *
     * @return string[]
     */
    public function notes(): array
    {
        return $this->notes;
    }

    /**
     * A value as written in the file, before normalising (e.g. sex 'F'
     * before 'Female'), for reports that show both. Null when not in the file.
     */
    public function delivered(string $participantId, string $column): ?string
    {
        $v = $this->delivered[$participantId][$column] ?? null;
        return $v === null || $v === '' ? null : (string) $v;
    }
}