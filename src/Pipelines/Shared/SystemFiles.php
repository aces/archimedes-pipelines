<?php

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

/**
 * Files an operating system adds to a folder: never data, always ignored.
 *
 * A delivery copied, zipped or browsed on a Mac or Windows machine picks up
 * metadata files next to the real ones:
 *   ._<name>          macOS AppleDouble (resource fork / Finder info); a
 *                     "._MR01_001.dcm" is not a DICOM and cannot be read as one
 *   __MACOSX/         folder macOS adds inside a zip it creates
 *   .DS_Store         macOS Finder settings
 *   Thumbs.db, desktop.ini, ehthumbs.db   Windows Explorer
 *
 * Every modality skips them the same way, before any file is read or hashed,
 * and reports how many it skipped (a warning, never an error), so a site
 * uploading from a Mac is not blocked by files it never meant to send.
 *
 * @category Pipelines
 * @package  LORIS\Pipelines\Shared
 */
final class SystemFiles
{
    /** Exact file names, compared case-insensitively. */
    private const NAMES = ['.ds_store', 'thumbs.db', 'desktop.ini', 'ehthumbs.db'];

    /**
     * Is this file OS metadata rather than data?
     *
     * @param string $path File path (absolute or relative).
     *
     * @return bool
     */
    public static function isIgnored(string $path): bool
    {
        $name = basename($path);

        return str_starts_with($name, '._')
            || in_array(strtolower($name), self::NAMES, true)
            || str_contains('/' . str_replace('\\', '/', $path), '/__MACOSX/');
    }

    /**
     * One warning line for the files skipped, or null when there were none.
     *
     * @param string[] $paths    Skipped files.
     * @param string   $rootPath Stripped from the examples shown.
     *
     * @return string|null
     */
    public static function summary(array $paths, string $rootPath = ''): ?string
    {
        if ($paths === []) {
            return null;
        }

        $root     = rtrim($rootPath, '/') . '/';
        $examples = array_map(
            static fn (string $p): string => str_starts_with($p, $root) ? substr($p, strlen($root)) : $p,
            array_slice($paths, 0, 3)
        );

        return sprintf(
            '%d operating-system file(s) ignored (macOS ._* / __MACOSX / .DS_Store, Windows Thumbs.db) - '
            . 'not data, nothing to fix; e.g. %s%s',
            count($paths),
            implode(', ', $examples),
            count($paths) > 3 ? ', ...' : ''
        );
    }
}
