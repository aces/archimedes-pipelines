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
 * than requiring them in the delivery. This generalises the behaviour already
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

namespace LORIS\Pipelines;

use RuntimeException;

class ParticipantsTsv
{
    /**
     * Columns a site must supply. Everything else is enriched.
     */
    public const SITE_REQUIRED = ['participant_id'];

    /**
     * Columns LORIS needs before a candidate can be created. Any of these
     * missing from the file is filled from candidate_defaults where possible.
     */
    public const LORIS_REQUIRED = ['participant_id', 'external_id', 'dob', 'sex', 'site'];

    /**
     * candidate_defaults key => participants.tsv column it fills.
     */
    private const DEFAULT_MAP = [
        'site'    => 'site',
        'cohort'  => 'cohort',
        'project' => 'project',
        'sex'     => 'sex',
    ];

    /** @var array<string, array<string, string>> participant_id => row */
    private array $rows = [];

    /** @var string[] Column names as they appeared in the file. */
    private array $columns = [];

    /** @var string[] Columns that were enriched rather than supplied. */
    private array $enriched = [];

    /** @var string Path the data was read from. */
    private string $path;

    private function __construct(string $path)
    {
        $this->path = $path;
    }

    /**
     * Read a participants.tsv and enrich it from candidate_defaults.
     *
     * @param string $path             Absolute path to participants.tsv.
     * @param array  $candidateDefaults project.json candidate_defaults block.
     *
     * @throws RuntimeException on a missing, empty or malformed file.
     */
    public static function load(string $path, array $candidateDefaults = []): self
    {
        if (!file_exists($path)) {
            throw new RuntimeException("participants.tsv not found: {$path}");
        }

        $instance = new self($path);
        $instance->read($path);
        $instance->enrich($candidateDefaults);

        return $instance;
    }

    /**
     * Find participants.tsv for a modality, without falling back across them.
     *
     * A project running both modalities keeps a separate list per delivery,
     * so BIDS and DICOM never read each other's file.
     *
     * @param string $projectPath Project root.
     * @param string $modality    'bids' or 'dicom'.
     *
     * @return string|null Absolute path, or null when absent.
     */
    public static function locate(string $projectPath, string $modality): ?string
    {
        $root = rtrim($projectPath, '/');

        $path = match ($modality) {
            'bids'  => $root . '/deidentified-raw/bids/participants.tsv',
            'dicom' => $root . '/deidentified-raw/imaging/dicoms/participants.tsv',
            default => throw new RuntimeException("Unknown modality: {$modality}"),
        };

        return file_exists($path) ? $path : null;
    }

    /**
     * Parse the file.
     */
    private function read(string $path): void
    {
        $handle = fopen($path, 'r');
        if ($handle === false) {
            throw new RuntimeException("Could not open {$path}");
        }

        $header = fgetcsv($handle, 0, "\t");
        if ($header === false) {
            fclose($handle);
            throw new RuntimeException("participants.tsv is empty: {$path}");
        }

        $this->columns = array_map('trim', $header);

        if (!in_array('participant_id', $this->columns, true)) {
            fclose($handle);
            throw new RuntimeException(
                "participants.tsv has no participant_id column: {$path}"
            );
        }

        $lineNo = 1;

        while (($line = fgetcsv($handle, 0, "\t")) !== false) {
            $lineNo++;

            if ($line === [null]) {
                continue;
            }

            $values = array_map('trim', $line);

            // Tolerate short and long rows rather than failing the delivery;
            // sites export these from Excel and trailing tabs are common.
            $values = array_pad(
                array_slice($values, 0, count($this->columns)),
                count($this->columns),
                ''
            );

            $row = array_combine($this->columns, $values);
            $id  = $row['participant_id'] ?? '';

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

        fclose($handle);

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
    private function enrich(array $candidateDefaults): void
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
}
