<?php

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

/**
 * The participant's sessions file - which visits a participant has.
 *
 * Accepted inside the participant folder under either name:
 *   sub-<ID>/sessions.tsv            (preferred; used when both exist)
 *   sub-<ID>/sub-<ID>_sessions.tsv   (BIDS name)
 *
 * Same file for every modality (it is the BIDS file, and the DICOM user guide
 * asks for it too), so the rules live here once:
 *   - the ingestion takes a visit only when its ses- folder is listed
 *     (listed(); used by the DICOM organizer to link a study)
 *   - the validator reports every mismatch between the file and the folders
 *     (check(); used by run_dicom_validate.php)
 * A new modality (BIDS, PNG ...) calls the same two methods with its own
 * delivery folder.
 *
 * Parsing is ParticipantsTsv::readRows(), the shared tab-separated reader.
 * session_id may be written "ses-V01" or "V01"; visits are returned without
 * the "ses-" prefix.
 *
 * @category Pipelines
 * @package  LORIS\Pipelines\Shared
 */
final class SessionsTsv
{
    /** Kinds returned by check(). "missing" and "no_column" block every visit of the participant. */
    public const MISSING          = 'missing';
    public const NO_COLUMN        = 'no_column';
    public const NOT_LISTED       = 'not_listed';
    public const LISTED_NO_FOLDER = 'listed_no_folder';
    public const BAD_ACQ_DATE     = 'bad_acq_date';

    /** @var array<string, array> read() results, per file, for one PHP process. */
    private static array $cache = [];

    /**
     * Accepted file names inside sub-<ID>/, in order of preference.
     *
     * @param string $subject ID without "sub-".
     *
     * @return string[]
     */
    public static function names(string $subject): array
    {
        return ['sessions.tsv', "sub-{$subject}_sessions.tsv"];
    }

    /**
     * The participant's sessions file: the first accepted name that exists,
     * else the preferred name (for messages; it does not exist).
     *
     * @param string $deliveryDir Folder holding the sub-<ID>/ folders.
     * @param string $subject     ID without "sub-".
     *
     * @return string
     */
    public static function path(string $deliveryDir, string $subject): string
    {
        $dir = rtrim($deliveryDir, '/') . "/sub-{$subject}";
        foreach (self::names($subject) as $name) {
            if (is_file("{$dir}/{$name}")) {
                return "{$dir}/{$name}";
            }
        }
        return "{$dir}/" . self::names($subject)[0];
    }

    /**
     * True when both names exist (only the preferred one is read).
     *
     * @param string $deliveryDir Folder holding the sub-<ID>/ folders.
     * @param string $subject     ID without "sub-".
     *
     * @return bool
     */
    public static function both(string $deliveryDir, string $subject): bool
    {
        $dir = rtrim($deliveryDir, '/') . "/sub-{$subject}";
        foreach (self::names($subject) as $name) {
            if (!is_file("{$dir}/{$name}")) {
                return false;
            }
        }
        return true;
    }

    /**
     * The file's content, parsed.
     *
     * @param string $deliveryDir Folder holding the sub-<ID>/ folders.
     * @param string $subject     ID without "sub-".
     *
     * @return array{exists: bool, error: ?string, has_column: bool,
     *               rows: list<array{line: int, visit: string, acq_date: string}>}
     */
    public static function read(string $deliveryDir, string $subject): array
    {
        $path = self::path($deliveryDir, $subject);
        $key  = $path . '|' . (is_file($path) ? (filemtime($path) . ':' . filesize($path)) : '-');

        if (isset(self::$cache[$key])) {
            return self::$cache[$key];
        }

        $out = ['exists' => is_file($path), 'error' => null, 'has_column' => false, 'rows' => []];

        if ($out['exists']) {
            try {
                $parsed = ParticipantsTsv::readRows($path);
                $out['has_column'] = in_array('session_id', $parsed['columns'], true);
                foreach ($out['has_column'] ? $parsed['rows'] : [] as ['line' => $line, 'row' => $row]) {
                    $sid = trim((string) ($row['session_id'] ?? ''));
                    $out['rows'][] = [
                        'line'     => (int) $line,
                        'visit'    => str_starts_with($sid, 'ses-') ? substr($sid, 4) : $sid,
                        'acq_date' => trim((string) ($row['acq_date'] ?? '')),
                    ];
                }
            } catch (\Throwable $e) {
                $out['error'] = $e->getMessage();
            }
        }

        return self::$cache[$key] = $out;
    }

    /**
     * Visits listed for the participant, or null when there is no usable file
     * (missing or unreadable). A file without a session_id column lists none.
     *
     * @param string $deliveryDir Folder holding the sub-<ID>/ folders.
     * @param string $subject     ID without "sub-".
     *
     * @return string[]|null Visit labels without "ses-".
     */
    public static function listed(string $deliveryDir, string $subject): ?array
    {
        $r = self::read($deliveryDir, $subject);

        if (!$r['exists'] || $r['error'] !== null) {
            return null;
        }

        return array_values(array_unique(array_filter(array_column($r['rows'], 'visit'))));
    }

    /**
     * Every mismatch between the file and the participant's ses- folders.
     *
     * @param string   $deliveryDir  Folder holding the sub-<ID>/ folders.
     * @param string   $subject      ID without "sub-".
     * @param string[] $folderVisits Visit labels of the ses- folders on disk (without "ses-").
     *
     * @return list<array{kind: string, visit: ?string, line: ?int, message: string, fix: string}>
     *         An unreadable file is reported by the caller (read()['error']), not here.
     */
    public static function check(string $deliveryDir, string $subject, array $folderVisits): array
    {
        $r    = self::read($deliveryDir, $subject);
        $file = basename(self::path($deliveryDir, $subject));

        if (!$r['exists']) {
            return [[
                'kind'    => self::MISSING,
                'visit'   => null,
                'line'    => null,
                'message' => "no sessions file (sessions.tsv or sub-{$subject}_sessions.tsv) - none of this participant's visits will be ingested",
                'fix'     => 'add it inside the participant folder, one row per visit (guide section 3)',
            ]];
        }
        if ($r['error'] !== null) {
            return [];
        }
        if (!$r['has_column']) {
            return [[
                'kind'    => self::NO_COLUMN,
                'visit'   => null,
                'line'    => null,
                'message' => "no session_id column - none of this participant's visits will be ingested",
                'fix'     => 'add session_id, e.g. ses-V01 (guide section 3)',
            ]];
        }

        $problems = [];
        $listed   = [];

        foreach ($r['rows'] as $row) {
            $listed[] = $row['visit'];
            if ($row['acq_date'] !== '' && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $row['acq_date'])) {
                $problems[] = [
                    'kind'    => self::BAD_ACQ_DATE,
                    'visit'   => $row['visit'],
                    'line'    => $row['line'],
                    'message' => "acq_date '{$row['acq_date']}' is not YYYY-MM-DD",
                    'fix'     => '',
                ];
            }
        }

        foreach (array_diff($folderVisits, $listed) as $v) {
            if (preg_match('/^[A-Za-z0-9]+$/', (string) $v)) {   // an invalid label is reported first, elsewhere
                $problems[] = [
                    'kind'    => self::NOT_LISTED,
                    'visit'   => (string) $v,
                    'line'    => null,
                    'message' => "visit not listed in {$file} - this visit will not be ingested",
                    'fix'     => "add a row with session_id ses-{$v}",
                ];
            }
        }

        foreach (array_diff(array_filter($listed), $folderVisits) as $v) {
            $problems[] = [
                'kind'    => self::LISTED_NO_FOLDER,
                'visit'   => (string) $v,
                'line'    => null,
                'message' => "lists ses-{$v}, but there is no ses-{$v}/ folder",
                'fix'     => 'add the folder, or remove the row',
            ];
        }

        return $problems;
    }
}
