<?php

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

/**
 * Tracking file names, with a fallback to the name a file had before.
 *
 * Four tracking files were renamed so every step uses
 * .<modality>_<step>_tracking.json:
 *
 *   processed/imaging/.dicom_import_processed.json     -> .dicom_import_tracking.json
 *   deidentified-lorisid/.../.dicom_mapping_tracking.json -> .dicom_reidentify_tracking.json
 *   processed/bids/.bids_import_processed.json         -> .bids_import_tracking.json
 *   processed/bids/.bids_reidentifier_processed.json   -> .bids_reidentify_tracking.json
 *
 * Existing state carries over without a separate step: a read falls back to
 * the old name while only that one exists, the next write (the next real run
 * that records something) goes to the new name, and only once that write
 * succeeded is the old file removed. A dry run
 * writes no tracking file, so it reads the old one and changes nothing.
 *
 * @category Pipelines
 * @package  LORIS\Pipelines\Shared
 */
final class TrackingFile
{
    /**
     * The file to read: $path, or $legacyPath while only that one exists.
     *
     * @param string $path       Current name.
     * @param string $legacyPath Name before the rename.
     *
     * @return string
     */
    public static function readPath(string $path, string $legacyPath): string
    {
        if ($legacyPath !== $path && !file_exists($path) && file_exists($legacyPath)) {
            return $legacyPath;
        }
        return $path;
    }

    /**
     * Remove the old file once the current one has been written.
     *
     * @param string $path       Current name (just written).
     * @param string $legacyPath Name before the rename.
     *
     * @return void
     */
    public static function retireLegacy(string $path, string $legacyPath): void
    {
        if ($legacyPath !== $path && file_exists($path) && file_exists($legacyPath)) {
            @unlink($legacyPath);
        }
    }

    /**
     * The same file under its old name, in the same directory.
     *
     * @param string $path       Current path.
     * @param string $legacyName Old base name.
     *
     * @return string
     */
    public static function legacy(string $path, string $legacyName): string
    {
        return dirname($path) . '/' . $legacyName;
    }
}
