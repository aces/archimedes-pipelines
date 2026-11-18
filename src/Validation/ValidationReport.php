<?php

declare(strict_types=1);

namespace LORIS\Validation;

/**
 * ValidationReport
 *
 * The report every ARCHIMEDES pre-check prints and saves - shared by
 * RedcapValidator (scripts/test/validate_redcap.php) and PngValidator
 * (scripts/test/validate_png.php), so they read the same and a site gets the
 * same .csv layout whichever data it sent.
 *
 * Sections: [{scope, file, summary, issues: [{severity, message, fix}]}].
 * Severities: ERROR, WARNING, NOTE (their meaning is given per validator, in
 * the legend).
 *
 * PHP Version 8.1
 */
final class ValidationReport
{
    public const ERROR   = 'ERROR';
    public const WARNING = 'WARNING';
    public const NOTE    = 'NOTE';

    /** @return array{ERROR:int, WARNING:int, NOTE:int} */
    public static function counts(array $sections): array
    {
        $c = [self::ERROR => 0, self::WARNING => 0, self::NOTE => 0];
        foreach ($sections as $s) {
            foreach ($s['issues'] as $i) {
                $c[$i['severity']]++;
            }
        }
        return $c;
    }

    /**
     * @param string   $heading      e.g. "REDCap validation report".
     * @param string   $title        What was checked (project, files).
     * @param string[] $legend       Lines explaining the severities.
     * @param array    $options      wrap (int, message width, default 62);
     *                               ok_ignores_notes (bool: a section with only
     *                               NOTEs still says "OK - no problems found");
     *                               no_ok_scopes (string[]: never say OK for these);
     *                               footer (string[] lines after the sections).
     */
    public static function renderText(
        array $sections,
        string $heading,
        string $title,
        string $runAt,
        array $legend,
        array $options = []
    ): string {
        $wrap   = (int) ($options['wrap'] ?? 62);
        $c      = self::counts($sections);
        $out    = [];
        $out[]  = str_repeat('=', 72);
        $out[]  = $heading;
        $out[]  = $title;
        $out[]  = "Checked: {$runAt}";
        $out[]  = "Result : " . ($c[self::ERROR] > 0 ? 'NOT READY' : 'READY')
            . " - {$c[self::ERROR]} error(s), {$c[self::WARNING]} warning(s)";
        $out[]  = str_repeat('=', 72);
        foreach ($legend as $line) {
            $out[] = $line;
        }

        foreach ($sections as $s) {
            $out[] = '';
            $out[] = "[{$s['scope']}] {$s['file']}" . ($s['summary'] !== '' ? "  ({$s['summary']})" : '');
            $out[] = str_repeat('-', 72);
            $counted = !empty($options['ok_ignores_notes'])
                ? array_filter($s['issues'], static fn ($i) => $i['severity'] !== self::NOTE)
                : $s['issues'];
            if ($counted === [] && !in_array($s['scope'], $options['no_ok_scopes'] ?? [], true)) {
                $out[] = "  OK - no problems found";
                if (empty($options['ok_ignores_notes'])) {
                    continue;
                }
            }
            $order  = [self::ERROR => 0, self::WARNING => 1, self::NOTE => 2];
            $issues = $s['issues'];
            usort($issues, fn($a, $b) => $order[$a['severity']] <=> $order[$b['severity']]);
            foreach ($issues as $i) {
                $out[] = sprintf("  %-8s%s", $i['severity'], wordwrap($i['message'], $wrap, "\n          ", true));
                if ($i['fix'] !== '') {
                    $out[] = "          Fix: " . wordwrap($i['fix'], $wrap - 5, "\n               ", true);
                }
            }
        }
        foreach ($options['footer'] ?? [] as $line) {
            $out[] = $line;
        }
        $out[] = '';
        return implode("\n", $out);
    }

    /**
     * One row per issue; $includeNotes false = problems only (what to send to
     * the data provider).
     */
    public static function writeCsv(array $sections, string $path, bool $includeNotes = true): bool
    {
        $fh = @fopen($path, 'w');
        if ($fh === false) {
            return false;
        }
        fputcsv($fh, ['severity', 'checked', 'file', 'problem', 'fix'], ',', '"', '');
        foreach ($sections as $s) {
            foreach ($s['issues'] as $i) {
                if ($includeNotes || $i['severity'] !== self::NOTE) {
                    fputcsv($fh, [$i['severity'], $s['scope'], $s['file'], $i['message'], $i['fix']], ',', '"', '');
                }
            }
        }
        fclose($fh);
        return true;
    }
}
