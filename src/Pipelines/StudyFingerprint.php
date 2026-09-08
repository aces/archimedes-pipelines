<?php

/**
 * StudyFingerprint
 *
 * Two-tier change detection for a DICOM study, per the DICOM Import Pipeline
 * hashing proposal.
 *
 *   Manifest hash - the study's packing list: relative path plus size for
 *                   every file. No file is opened, so it is nearly free and
 *                   can be checked on every run.
 *
 *   Content hash  - the bytes themselves. Certain to change if one byte
 *                   changes, but every file is read, so it is only computed
 *                   when the manifest hash already says something differs.
 *
 * Cheap check every run; open the boxes only when the packing list looks
 * wrong. Fast in the normal case, certain when it matters.
 *
 * Decision table:
 *
 *   no stored hashes      -> record as baseline, do not reprocess
 *   manifest matches      -> unchanged, skip
 *   manifest differs,
 *     content matches     -> renamed or re-copied, same bytes: skip, refresh
 *   content differs       -> genuinely changed: reprocess
 *
 * Works on a list of files rather than a directory, because a study is
 * identified by StudyInstanceUID and its files may be spread across a
 * delivery tree before they are organised.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines;

use RuntimeException;

class StudyFingerprint
{
    /** Reprocess: the bytes changed. */
    public const CHANGED = 'changed';

    /** Skip: manifest matched, nothing to do. */
    public const UNCHANGED = 'unchanged';

    /** Skip, but refresh the stored manifest hash. */
    public const RELOCATED = 'relocated';

    /** Skip: tracked before hashing existed. Record hashes as a baseline. */
    public const BASELINE = 'baseline';

    /** No tracking entry at all. */
    public const NEW = 'new';

    /**
     * Manifest hash: relative path and size for every file.
     *
     * Sorted, so directory iteration order cannot change the result. Sizes are
     * stat calls - no file is opened.
     *
     * @param string[] $files    Absolute paths.
     * @param string   $rootPath Stripped from each path, so moving a whole
     *                           delivery does not invalidate the hash.
     *
     * @return string sha256 hex digest.
     */
    public static function manifestHash(array $files, string $rootPath = ''): string
    {
        $rootPath = rtrim($rootPath, '/');
        $entries  = [];

        foreach ($files as $path) {
            $size = @filesize($path);

            if ($size === false) {
                // A file that vanished between listing and hashing is itself a
                // change; record it as such rather than throwing.
                $size = -1;
            }

            $relative = ($rootPath !== '' && str_starts_with($path, $rootPath . '/'))
                ? substr($path, strlen($rootPath) + 1)
                : basename($path);

            $entries[] = $relative . "\0" . $size;
        }

        sort($entries, SORT_STRING);

        return hash('sha256', implode("\n", $entries));
    }

    /**
     * Content hash: the bytes of every file, in sorted path order.
     *
     * Each file is hashed separately and the digests are sorted before being
     * combined, so the result depends only on the bytes present - not on
     * filenames, and not on directory iteration order.
     *
     * @param string[] $files    Absolute paths.
     * @param string   $rootPath Unused; kept for signature symmetry with
     *                           manifestHash().
     *
     * @return string sha256 hex digest.
     *
     * @throws RuntimeException if a file cannot be read.
     */
    public static function contentHash(array $files, string $rootPath = ''): string
    {
        $digests = [];

        foreach ($files as $path) {
            $digest = @hash_file('sha256', $path);

            if ($digest === false) {
                throw new RuntimeException("Could not read for hashing: {$path}");
            }

            $digests[] = $digest;
        }

        // Sort the per-file digests, not the paths. The result is then a
        // fingerprint of the bytes alone: independent of filename and of
        // directory order, so a renamed or re-copied study with identical
        // content hashes the same and is correctly classified as relocated
        // rather than changed.
        sort($digests, SORT_STRING);

        return hash('sha256', implode("\n", $digests));
    }

    /**
     * Every file under a directory, recursively.
     *
     * @param string $root Directory to walk.
     *
     * @return string[] Absolute paths, sorted.
     */
    public static function listFiles(string $root): array
    {
        if (!is_dir($root)) {
            return [];
        }

        $files    = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::LEAVES_ONLY
        );

        foreach ($iterator as $fileInfo) {
            if ($fileInfo->isFile()) {
                $files[] = $fileInfo->getPathname();
            }
        }

        sort($files, SORT_STRING);

        return $files;
    }

    /**
     * Manifest hash of an entire delivery.
     *
     * Stat calls only - no file is opened - so this is cheap enough to run at
     * the top of every invocation. When it matches the last successful run,
     * nothing in the delivery has been added, removed or resized, and the
     * whole project can be skipped: no header reads, no EviData pass, no copy.
     *
     * @param string $sourceDir Delivery root.
     *
     * @return array{hash: string, file_count: int}
     */
    public static function deliveryHash(string $sourceDir): array
    {
        $files = self::listFiles($sourceDir);

        return [
            'hash'       => self::manifestHash($files, $sourceDir),
            'file_count' => count($files),
        ];
    }

    /**
     * Compare a study against its tracking entry.
     *
     * The content hash is computed only when the manifest hash differs, which
     * is the whole point of the two tiers.
     *
     * @param array|null $tracked  Stored entry, or null when untracked.
     * @param string[]   $files    Current files for the study.
     * @param string     $rootPath Stripped from paths before hashing.
     *
     * @return array{
     *     state: string,
     *     reprocess: bool,
     *     manifest_hash: string,
     *     content_hash: ?string,
     *     reason: string
     * }
     */
    public static function compare(
        ?array $tracked,
        array $files,
        string $rootPath = ''
    ): array {
        $manifest = self::manifestHash($files, $rootPath);

        if ($tracked === null) {
            return [
                'state'         => self::NEW,
                'reprocess'     => true,
                'manifest_hash' => $manifest,
                'content_hash'  => null,
                'reason'        => 'not seen before',
            ];
        }

        $storedManifest = $tracked['manifest_hash'] ?? null;
        $storedContent  = $tracked['content_hash'] ?? null;

        // Tracked before hashing existed. Record a baseline and leave it alone:
        // reprocessing every historical study the first time this ships would
        // be a surprise, not a fix.
        if ($storedManifest === null && $storedContent === null) {
            return [
                'state'         => self::BASELINE,
                'reprocess'     => false,
                'manifest_hash' => $manifest,
                'content_hash'  => null,
                'reason'        => 'tracked before hashing; recording baseline',
            ];
        }

        if ($storedManifest === $manifest) {
            return [
                'state'         => self::UNCHANGED,
                'reprocess'     => false,
                'manifest_hash' => $manifest,
                'content_hash'  => $storedContent,
                'reason'        => 'manifest unchanged',
            ];
        }

        // Packing list differs - now it is worth reading the bytes.
        $content = self::contentHash($files, $rootPath);

        if ($storedContent !== null && $storedContent === $content) {
            return [
                'state'         => self::RELOCATED,
                'reprocess'     => false,
                'manifest_hash' => $manifest,
                'content_hash'  => $content,
                'reason'        => 'files renamed or re-copied, bytes identical',
            ];
        }

        return [
            'state'         => self::CHANGED,
            'reprocess'     => true,
            'manifest_hash' => $manifest,
            'content_hash'  => $content,
            'reason'        => $storedContent === null
                ? 'manifest differs and no stored content hash to compare'
                : 'content differs',
        ];
    }

    /**
     * Hashes to store after a successful run.
     *
     * The content hash is computed here if the comparison did not need it, so
     * a stored entry always carries both. Storing only on success keeps the
     * invariant that a stored fingerprint means "this reached LORIS".
     *
     * @param array    $comparison Result of compare().
     * @param string[] $files      Current files for the study.
     * @param string   $rootPath   Stripped from paths before hashing.
     *
     * @return array{manifest_hash: string, content_hash: string, hashed_at: string}
     */
    public static function toTracking(
        array $comparison,
        array $files,
        string $rootPath = ''
    ): array {
        $content = $comparison['content_hash']
            ?? self::contentHash($files, $rootPath);

        return [
            'manifest_hash' => $comparison['manifest_hash'],
            'content_hash'  => $content,
            'hashed_at'     => date('c'),
        ];
    }
}