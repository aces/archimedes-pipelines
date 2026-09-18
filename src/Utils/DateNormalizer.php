<?php
declare(strict_types=1);

namespace LORIS\Utils;

/**
 * Normalise a source date cell to YYYY-MM-DD.
 *
 * Shared by every pipeline (clinical, BIDS, DICOM, and any future
 * modality) so a date is read the same way everywhere. Pure: no I/O.
 *
 * Accepted inputs (separators - / . treated alike):
 *   YYYY-MM-DD / YYYY-MM / YYYY        unambiguous, taken as-is
 *   YYYYMMDD                           compact ISO
 *   YYYY/MM/DD, YYYY.MM                leading 4-digit year
 *   M/D/YYYY or D/M/YYYY               resolved as below
 *   "May 2024", "17 May 2024"          month names (needs a 4-digit year)
 *
 * Resolving the two-number forms:
 *   - one part > 12   -> that part MUST be the day. Order comes from the
 *                        data, not configuration.
 *   - both parts <=12 -> genuinely ambiguous (5/6/2024). $order decides:
 *                        'mdy' (default, REDCap date_mdy) or 'dmy', set
 *                        per project via project.json -> date_input_format.
 *
 * Day handling:
 *   $keepDay = false -> day forced to 01 (month precision, e.g. DoD).
 *   $keepDay = true  -> a provided day is kept; a missing day becomes 01;
 *                       an impossible date (2024-02-30) is returned
 *                       UNCHANGED so the caller's validation rejects it
 *                       rather than the pipeline guessing.
 *
 * Empty stays empty. Anything unrecognised is returned UNCHANGED.
 */
final class DateNormalizer
{
    /**
     * @param string $order   'mdy' or 'dmy' - only for the ambiguous case.
     * @param bool   $keepDay Keep a provided day instead of forcing 01.
     */
    public static function normalize(string $value, string $order = 'mdy', bool $keepDay = false): string
    {
        $value = trim($value);
        if ($value === '') {
            return $value;
        }

        // Builds the output - the one place day handling is decided.
        $fmt = static function (string $y, int $mo, ?int $d) use ($keepDay, $value): string {
            if (!$keepDay) {
                return sprintf('%s-%02d-01', $y, $mo);
            }
            if ($d !== null && !checkdate($mo, $d, (int)$y)) {
                return $value;
            }
            return sprintf('%s-%02d-%02d', $y, $mo, $d ?? 1);
        };

        if (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $value, $m)) {
            return $fmt($m[1], (int)$m[2], (int)$m[3]);
        }
        if (preg_match('/^(\d{4})-(\d{2})$/', $value, $m)) {
            return "{$m[1]}-{$m[2]}-01";
        }
        if (preg_match('/^(\d{4})$/', $value, $m)) {
            return "{$m[1]}-01-01";
        }

        // Compact ISO: YYYYMMDD
        if (preg_match('/^(\d{4})(\d{2})(\d{2})$/', $value, $m)) {
            $month = (int)$m[2];
            return ($month >= 1 && $month <= 12) ? $fmt($m[1], $month, (int)$m[3]) : $value;
        }

        // Year-first with / or . separators: YYYY/M/D, YYYY.MM.DD, YYYY/M
        if (preg_match('/^(\d{4})[\/.](\d{1,2})(?:[\/.](\d{1,2}))?$/', $value, $m)) {
            $month = (int)$m[2];
            return ($month >= 1 && $month <= 12)
                ? $fmt($m[1], $month, isset($m[3]) && $m[3] !== '' ? (int)$m[3] : null)
                : $value;
        }

        // Two numbers then a 4-digit year: M/D/YYYY or D/M/YYYY
        if (preg_match('/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})$/', $value, $m)) {
            $a    = (int)$m[1];
            $b    = (int)$m[2];
            $year = $m[3];

            if ($a > 12 && $b >= 1 && $b <= 12) {
                [$month, $day] = [$b, $a];   // first part must be the day
            } elseif ($b > 12 && $a >= 1 && $a <= 12) {
                [$month, $day] = [$a, $b];   // second part must be the day
            } elseif ($a >= 1 && $a <= 12 && $b >= 1 && $b <= 12) {
                [$month, $day] = ($order === 'dmy') ? [$b, $a] : [$a, $b];   // ambiguous
            } else {
                return $value;               // neither part is a valid month
            }

            return $fmt($year, $month, $day);
        }

        // Month names ("May 2024", "17 May 2024"). Guarded: needs letters
        // AND a 4-digit year, so relative words ("today") never parse.
        if (preg_match('/[a-z]/i', $value) && preg_match('/\b\d{4}\b/', $value)) {
            $ts = strtotime($value);
            if ($ts !== false) {
                $hasDay = preg_match('/\b\d{1,2}\b/', $value) === 1;
                return $fmt(date('Y', $ts), (int)date('n', $ts), $hasDay ? (int)date('j', $ts) : null);
            }
        }

        return $value;
    }

    /**
     * True when $value is a real calendar date in YYYY-MM-DD.
     */
    public static function isValid(?string $value): bool
    {
        return $value !== null
            && preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $value, $m) === 1
            && checkdate((int)$m[2], (int)$m[3], (int)$m[1]);
    }
}
