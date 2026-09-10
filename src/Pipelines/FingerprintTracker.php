<?php

/**
 * FingerprintTracker
 *
 * The tracking-file half of change detection, shared by every pipeline that
 * uses StudyFingerprint.
 *
 * StudyFingerprint answers "did these files change". This owns everything
 * around that: reading the tracking JSON, comparing, refreshing hashes when a
 * skip happens, storing them only after success, and writing the file back.
 *
 * Without it each pipeline repeats the same five steps by hand, and the rules
 * that are easy to get subtly wrong - refresh on skip, never store on failure -
 * are re-implemented per pipeline instead of being stated once.
 *
 * Keyed, so it serves both granularities:
 *
 *   DICOM  one entry per study      $tracker->compare($studyName, $files, $dir)
 *   BIDS   one entry, 'last_run'    $tracker->compare('last_run', $files, $dir)
 *
 * Entries are merged, not replaced, so a pipeline's own fields - status,
 * detail, timestamp, scans_found - survive alongside the hashes.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines;

use RuntimeException;

class FingerprintTracker
{
    private string $path;

    /** @var array<string, array> */
    private array $entries = [];

    /**
     * File lists by directory, walked at most once per run.
     *
     * Every caller previously walked the tree itself before calling compare(),
     * then again before recordSuccess() - two identical traversals of the same
     * directory, and the same StudyFingerprint::listFiles() call written out at
     * each site.
     *
     * @var array<string, string[]>
     */
    private array $fileCache = [];

    private ?object $logger;

    /**
     * Statuses whose stored fingerprint counts as authoritative.
     *
     * A failed run must not suppress the retry, so its entry - if any - is
     * treated as absent when comparing.
     *
     * @var string[]
     */
    private array $successStatuses = ['success', 'already_exists'];

    /**
     * @param string      $path   Tracking JSON path.
     * @param object|null $logger Optional logger.
     */
    public function __construct(string $path, ?object $logger = null)
    {
        $this->path   = $path;
        $this->logger = $logger;

        $this->load();
    }

    /**
     * Statuses treated as authoritative when comparing.
     *
     * @param string[] $statuses Status values.
     *
     * @return void
     */
    public function setSuccessStatuses(array $statuses): void
    {
        $this->successStatuses = $statuses;
    }

    // =========================================================================
    //  DIRECTORY API — walk once, reuse
    // =========================================================================

    /**
     * File list for a directory, walked at most once per run.
     *
     * @param string $dir Directory to walk.
     *
     * @return string[]
     */
    public function filesIn(string $dir): array
    {
        $key = rtrim($dir, '/');

        if (!isset($this->fileCache[$key])) {
            $this->fileCache[$key] = $this->excludeSelf(
                StudyFingerprint::listFiles($key)
            );
        }

        return $this->fileCache[$key];
    }

    /**
     * Forget a cached file list, after the directory has been modified.
     *
     * @param string $dir Directory whose contents changed.
     *
     * @return void
     */
    public function invalidate(string $dir): void
    {
        unset($this->fileCache[rtrim($dir, '/')]);
    }

    /**
     * Should the contents of this directory be processed?
     *
     * The directory form of compare(): the caller passes a path, not a file
     * list, so no pipeline has to walk the tree or call StudyFingerprint
     * itself.
     *
     * @param string $key   Entry key: a study name, or 'last_run'.
     * @param string $dir   Directory to fingerprint.
     * @param bool   $force Ignore the stored entry.
     *
     * @return array compare() result.
     */
    public function check(string $key, string $dir, bool $force = false): array
    {
        return $this->compare($key, $this->filesIn($dir), $dir, $force);
    }

    /**
     * Record a success for the contents of a directory.
     *
     * @param string     $key        Entry key.
     * @param string     $dir        Directory that was processed.
     * @param array      $fields     Pipeline fields: status, detail, etc.
     * @param array|null $comparison check() result, if already computed.
     *
     * @return void
     */
    public function succeeded(
        string $key,
        string $dir,
        array $fields = [],
        ?array $comparison = null
    ): void {
        $this->recordSuccess($key, $this->filesIn($dir), $dir, $fields, $comparison);
    }

    /**
     * Refresh stored hashes after a skip, for a directory.
     *
     * @param string $key        Entry key.
     * @param string $dir        Directory.
     * @param array  $comparison check() result.
     *
     * @return void
     */
    public function skipped(string $key, string $dir, array $comparison): void
    {
        $this->refresh($key, $comparison, $this->filesIn($dir), $dir);
    }

    // =========================================================================
    //  COMPARISON — file-list form, for callers that already have one
    // =========================================================================

    /**
     * Remove the tracking file itself from a file list.
     *
     * If the tracking file lives inside the tree being hashed, writing it
     * changes the manifest, so every run sees a change and nothing is ever
     * skipped. That is a permanent false positive and easy to miss - the
     * pipeline just quietly stops saving work.
     *
     * Filtering by exact path rather than by dotfile prefix, so genuine hidden
     * files in a delivery are still hashed.
     *
     * @param string[] $files Candidate files.
     *
     * @return string[]
     */
    private function excludeSelf(array $files): array
    {
        $self = realpath($this->path);

        if ($self === false) {
            // Not written yet, so it cannot be in the list.
            $self = $this->path;
        }

        $filtered = array_values(array_filter(
            $files,
            static function (string $file) use ($self): bool {
                $resolved = realpath($file);

                return ($resolved === false ? $file : $resolved) !== $self;
            }
        ));

        if (count($filtered) !== count($files)) {
            $this->warn(
                'Tracking file is inside the directory being hashed - excluding '
                . 'it from the fingerprint. Consider moving it outside the tree.'
            );
        }

        return $filtered;
    }

    /**
     * Should this item be processed?
     *
     * Returns the StudyFingerprint::compare() result, with `reprocess` telling
     * the caller what to do. An entry whose status is not in successStatuses is
     * treated as unseen, so a previously failed item is always retried.
     *
     * @param string   $key      Study name, or 'last_run' for a whole dataset.
     * @param string[] $files    Current files.
     * @param string   $rootPath Stripped from paths before hashing.
     * @param bool     $force    Ignore the stored entry entirely.
     *
     * @return array compare() result.
     */
    public function compare(
        string $key,
        array $files,
        string $rootPath,
        bool $force = false
    ): array {
        $previous = $force ? null : ($this->entries[$key] ?? null);

        if ($previous !== null
            && isset($previous['status'])
            && !in_array($previous['status'], $this->successStatuses, true)
        ) {
            // Recorded, but not as a success. Retry it.
            $previous = null;
        }

        return StudyFingerprint::compare($previous, $this->excludeSelf($files), $rootPath);
    }

    /**
     * Refresh the stored hashes without changing the item's status.
     *
     * Called when a comparison says skip but the manifest hash moved - a
     * renamed or re-copied item. Without this the same full byte read happens
     * on every future run.
     *
     * @param string   $key        Entry key.
     * @param array    $comparison compare() result.
     * @param string[] $files      Current files.
     * @param string   $rootPath   Root for relative paths.
     *
     * @return void
     */
    public function refresh(
        string $key,
        array $comparison,
        array $files,
        string $rootPath
    ): void {
        if (($comparison['state'] ?? null) === StudyFingerprint::UNCHANGED) {
            // Manifest already matched; nothing to refresh.
            return;
        }

        $this->merge(
            $key,
            StudyFingerprint::toTracking(
                $comparison,
                $this->excludeSelf($files),
                $rootPath
            )
        );

        $this->save();
    }

    // =========================================================================
    //  RECORDING
    // =========================================================================

    /**
     * Record a successful run and store its fingerprint.
     *
     * The fingerprint is stored ONLY here, so a stored fingerprint always means
     * "this reached its destination" and never "we looked at it". A run that
     * failed leaves the previous entry alone and is retried in full.
     *
     * @param string     $key        Entry key.
     * @param string[]   $files      Files that were processed.
     * @param string     $rootPath   Root for relative paths.
     * @param array      $fields     Pipeline fields: status, detail, etc.
     * @param array|null $comparison compare() result, if already computed -
     *                               its content hash is reused rather than the
     *                               files being read a second time.
     *
     * @return void
     */
    public function recordSuccess(
        string $key,
        array $files,
        string $rootPath,
        array $fields = [],
        ?array $comparison = null
    ): void {
        $files      = $this->excludeSelf($files);
        $comparison ??= StudyFingerprint::compare(null, $files, $rootPath);

        $this->merge(
            $key,
            array_merge(
                ['timestamp' => date('c')],
                $fields,
                StudyFingerprint::toTracking($comparison, $files, $rootPath)
            )
        );

        $this->save();
    }

    /**
     * Record a failure. Deliberately stores no fingerprint.
     *
     * @param string $key    Entry key.
     * @param array  $fields Pipeline fields.
     *
     * @return void
     */
    public function recordFailure(string $key, array $fields = []): void
    {
        $entry = array_merge(
            ['timestamp' => date('c')],
            $fields
        );

        // Drop any fingerprint from a previous success: the item is no longer
        // known-good, and keeping the hashes would let the next run skip it.
        unset($entry['manifest_hash'], $entry['content_hash'], $entry['hashed_at']);

        $this->entries[$key] = array_merge(
            $this->entries[$key] ?? [],
            $entry
        );

        unset(
            $this->entries[$key]['manifest_hash'],
            $this->entries[$key]['content_hash'],
            $this->entries[$key]['hashed_at']
        );

        $this->save();
    }

    // =========================================================================
    //  ACCESS
    // =========================================================================

    public function get(string $key): ?array
    {
        return $this->entries[$key] ?? null;
    }

    public function has(string $key): bool
    {
        return isset($this->entries[$key]);
    }

    /**
     * @return array<string, array>
     */
    public function all(): array
    {
        return $this->entries;
    }

    public function status(string $key): ?string
    {
        return $this->entries[$key]['status'] ?? null;
    }

    // =========================================================================
    //  PERSISTENCE
    // =========================================================================

    private function merge(string $key, array $fields): void
    {
        $this->entries[$key] = array_merge($this->entries[$key] ?? [], $fields);
    }

    private function load(): void
    {
        if (!file_exists($this->path)) {
            return;
        }

        $raw = @file_get_contents($this->path);

        if ($raw === false) {
            $this->warn("Cannot read tracking file: {$this->path}");
            return;
        }

        $decoded = json_decode($raw, true);

        if (!is_array($decoded)) {
            // A corrupt tracking file must not stop a run. Treating it as empty
            // means the work is redone, which is safe; failing would block the
            // pipeline on a cache.
            $this->warn(
                "Malformed tracking file, treating as empty: {$this->path}"
            );
            return;
        }

        $this->entries = $decoded;
    }

    /**
     * Write via a temp file and rename, so a reader never sees a partial file
     * and an interrupted run cannot leave a truncated one.
     */
    private function save(): void
    {
        $dir = dirname($this->path);

        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            $this->warn("Cannot create tracking directory: {$dir}");
            return;
        }

        $json = json_encode(
            $this->entries,
            JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES
        );

        if ($json === false) {
            $this->warn('Cannot encode tracking file: ' . json_last_error_msg());
            return;
        }

        $tmp = $this->path . '.tmp.' . getmypid();

        if (@file_put_contents($tmp, $json) === false) {
            $this->warn("Cannot write tracking file: {$tmp}");
            return;
        }

        if (!@rename($tmp, $this->path)) {
            @unlink($tmp);
            $this->warn("Cannot rename {$tmp} to {$this->path}");
        }
    }

    private function warn(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->warning($message);
        }
    }
}
