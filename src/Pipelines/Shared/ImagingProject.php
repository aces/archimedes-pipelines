<?php

/**
 * ImagingProject
 *
 * Project selection rules shared by every imaging modality (DICOM, PNG, ...)
 * and their bundles - moved here from DicomStudyOrganizer once PNG used them
 * too (DicomStudyOrganizer keeps delegates, so DICOM callers are unchanged):
 *
 *   projectSkipReason()  project.json present, valid, and "Imaging" in modalities
 *   acceptProject()      enabled in the config (or named explicitly) + the rule above
 *   uploadInProgress()   the delivery changed within the quiet period: wait
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

final class ImagingProject
{

    /**
     * Why this project must not be processed for imaging (DICOM, PNG, ...), or null if it may.
     *
     * @param string $projectPath Project root (holds project.json).
     */
    public static function projectSkipReason(string $projectPath): ?string
    {
        $json = rtrim($projectPath, '/') . '/project.json';

        if (!is_file($json)) {
            return "no project.json at {$json}";
        }

        $config = json_decode((string) file_get_contents($json), true);

        if (!is_array($config)) {
            return "project.json is not valid JSON: {$json}";
        }

        $modalities = array_map(
            static fn ($m) => strtolower((string) $m),
            (array) ($config['modalities'] ?? [])
        );

        if (!in_array('imaging', $modalities, true)) {
            return 'project.json "modalities" does not include "Imaging"';
        }

        return null;
    }

    /**
     * Is the delivery still being written?
     *
     * A run that starts while a site is uploading would organize half a study (DICOM) or copy half a visit (PNG)
     * and read a half-written participants.tsv. If anything under the raw
     * delivery changed within the quiet period, the project is left for the
     * next run. Stat calls only.
     *
     * @param string $rawDir  the modality's delivery, e.g. deidentified-raw/imaging/dicoms or .../png
     * @param int    $minutes Quiet period; 0 disables the check.
     *
     * @return string|null Reason to wait, or null.
     */
    public static function uploadInProgress(string $rawDir, int $minutes): ?string
    {
        if ($minutes <= 0 || !is_dir($rawDir)) {
            return null;
        }

        $newest = 0;
        $it = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($rawDir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::SELF_FIRST
        );
        foreach ($it as $f) {
            $newest = max($newest, (int) $f->getMTime());
        }

        $age = time() - $newest;
        if ($newest > 0 && $age < $minutes * 60) {
            return sprintf(
                'delivery changed %ds ago (quiet period %d min) - probably still uploading; '
                . 'left for the next run',
                $age,
                $minutes
            );
        }

        return null;
    }

    /**
     * Apply both rules to one configured project.
     *
     * @param string $label            collection/project, for messages.
     * @param string $projectPath      Project root.
     * @param bool   $enabled          Collection AND project enabled in the config.
     * @param bool   $namedExplicitly  Selected by --project (or its collection by
     *                                 --collection, for a disabled collection).
     *
     * @return bool True when the project should be processed. Writes the reason
     *              to STDERR when it is skipped or run despite being disabled.
     */
    public static function acceptProject(
        string $label,
        string $projectPath,
        bool $enabled,
        bool $namedExplicitly
    ): bool {
        if (!$enabled && !$namedExplicitly) {
            return false;   // silent: --all / --collection only cover enabled projects
        }

        $reason = self::projectSkipReason($projectPath);

        if ($reason !== null) {
            fwrite(STDERR, "  SKIP {$label}: {$reason}\n");
            return false;
        }

        if (!$enabled) {
            fwrite(STDERR, "  WARNING {$label} is disabled in the config - "
                . "running only because it was named explicitly\n");
        }

        return true;
    }

    /**
     * Projects selected by --all / --collection / --project, for every
     * imaging pipeline (DICOM, PNG, BIDS ...), one rule for all of them:
     *
     *   - names match case-insensitively (--collection=ARCHIMEDES finds
     *     "archimedes");
     *   - --all takes enabled collections and enabled projects;
     *   - a collection or project named explicitly is taken even when disabled;
     *   - acceptProject() decides the rest (project.json, "Imaging" modality).
     *
     * --project without --collection is the caller's usage error to report.
     *
     * @param array $config  Pipeline config (collections[]).
     * @param array $options getopt() result: all, collection, project.
     *
     * @return array<int, array{collection: string, project: string, path: string}>
     */
    public static function targets(array $config, array $options): array
    {
        $wantAll        = isset($options['all']);
        $wantCollection = $options['collection'] ?? null;
        $wantProject    = $options['project'] ?? null;

        // Collection and project names are identifiers, not data.
        $matches = static function (?string $wanted, string $actual): bool {
            return $wanted === null || strcasecmp($wanted, $actual) === 0;
        };

        $targets = [];

        foreach ($config['collections'] ?? [] as $collection) {
            $collectionName = $collection['name'] ?? null;
            if ($collectionName === null) {
                continue;
            }

            $collectionWanted = $matches($wantCollection, $collectionName);
            // Named only when --collection was given (matches() is also true
            // for no name, which must not let --all take disabled ones).
            $collectionNamed = $wantCollection !== null && $collectionWanted;

            if (empty($collection['enabled']) && !$collectionNamed) {
                continue;
            }
            if (!$wantAll && $wantCollection !== null && !$collectionWanted) {
                continue;
            }

            $basePath = rtrim((string) ($collection['base_path'] ?? ''), '/');

            foreach ($collection['projects'] ?? [] as $project) {
                $projectName = $project['name'] ?? null;
                if ($projectName === null) {
                    continue;
                }

                $projectWanted = $matches($wantProject, $projectName);
                if ($wantProject !== null && !$projectWanted) {
                    continue;
                }
                $projectNamed = $wantProject !== null && $projectWanted;

                $path = $basePath . '/' . $projectName;
                if (!self::acceptProject(
                    "{$collectionName}/{$projectName}",
                    $path,
                    !empty($collection['enabled']) && !empty($project['enabled']),
                    $projectNamed || ($collectionNamed && empty($collection['enabled']))
                )) {
                    continue;
                }

                $targets[] = [
                    'collection' => $collectionName,
                    'project'    => $projectName,
                    'path'       => $path,
                ];
            }
        }

        return $targets;
    }
}
