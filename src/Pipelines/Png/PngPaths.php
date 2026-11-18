<?php

/**
 * PngPaths
 *
 * Every folder and file the PNG pipeline reads or writes, in one place, so the
 * scan, sync, reidentify, import, validate and bundle steps cannot disagree.
 *
 *   deidentified-raw/imaging/png/          the site's delivery (never modified)
 *   processed/imaging/png_visits.json      scan manifest: visits, images, hashes, problems
 *   processed/imaging/png_participant_sync.json  sync warnings (for the email)
 *   processed/imaging/.png_import_tracking.json  what reached LORIS (per visit)
 *   processed/imaging/.png_excluded.json   bundle: visits/participants left out this run
 *   processed/imaging/.png_hash_cache.json md5 per image path+size+mtime (speed only)
 *   deidentified-lorisid/imaging/png/      the ONLY source that is ingested:
 *       participants.tsv                   participant_id = sub-<PSCID>
 *       sub-<PSCID>/sessions.tsv           session_id = ses-<LORIS visit>, acq_date
 *       sub-<PSCID>/ses-<Visit>/<file>.png byte-identical copies, original file names
 *   deidentified-lorisid/imaging/png_visits_lorisid.json  reidentified visits + provenance
 *   deidentified-lorisid/imaging/png_provenance.tsv       one line per image: LORIS copy <- original
 *   deidentified-lorisid/imaging/.png_reidentify_tracking.json
 *   logs/png/                              logs and validation reports
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Png;

final class PngPaths
{
    /** Modality folder under deidentified-raw/ (ParticipantsTsv::locate()). */
    public const MODALITY = 'imaging/png';

    public const RAW_SUBDIR     = 'deidentified-raw/imaging/png';
    public const LORISID_SUBDIR = 'deidentified-lorisid/imaging/png';

    public const SCAN_MANIFEST     = 'processed/imaging/png_visits.json';
    public const SYNC_WARNINGS     = 'processed/imaging/png_participant_sync.json';
    public const IMPORT_TRACKING   = 'processed/imaging/.png_import_tracking.json';
    public const EXCLUDED          = 'processed/imaging/.png_excluded.json';
    public const LORISID_MANIFEST  = 'deidentified-lorisid/imaging/png_visits_lorisid.json';
    public const PROVENANCE_TSV    = 'deidentified-lorisid/imaging/png_provenance.tsv';
    public const REIDENT_TRACKING  = 'deidentified-lorisid/imaging/.png_reidentify_tracking.json';
    public const HASH_CACHE        = 'processed/imaging/.png_hash_cache.json';

    /**
     * Absolute path of one of the constants above.
     */
    public static function of(string $projectPath, string $relative): string
    {
        return rtrim($projectPath, '/') . '/' . $relative;
    }

    /**
     * Read a JSON file; [] when missing or not valid JSON.
     */
    public static function readJson(string $path): array
    {
        if (!is_file($path)) {
            return [];
        }
        $data = json_decode((string) file_get_contents($path), true);
        return is_array($data) ? $data : [];
    }

    /**
     * Write JSON atomically (temp file + rename), creating the folder.
     *
     * @throws \RuntimeException when it cannot be written.
     */
    public static function writeJson(string $path, array $data): void
    {
        $dir = dirname($path);
        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new \RuntimeException("Cannot create {$dir}");
        }
        $tmp  = $path . '.tmp.' . getmypid();
        $json = json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        if ($json === false || @file_put_contents($tmp, $json . "\n") === false || !@rename($tmp, $path)) {
            @unlink($tmp);
            throw new \RuntimeException("Cannot write {$path}");
        }
    }
}
