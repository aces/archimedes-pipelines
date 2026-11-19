<?php

declare(strict_types=1);

namespace LORIS\Pipelines\Bids;

use LORIS\Utils\VisitLabel;

/**
 * BIDS visits held back: the ses- folders that cannot go into LORIS as they
 * are. Each is flagged and left out; the rest of the dataset goes in.
 *
 *   - the folder is not exactly a LORIS visit (project.json "visits", when
 *     declared - LORIS\Utils\VisitLabel, the DICOM and PNG rule)
 *   - a file in it is labelled with another ses- (sub-X_ses-1_T1w.nii.gz in
 *     ses-V01/): its data would be filed under the wrong visit
 *
 * BIDS needs no sessions.tsv: the visits are the ses- folder names. Used by
 * the participant sync (to flag) and the reidentifier (to leave out), so both
 * agree on what is held.
 */
final class BidsVisitCheck
{
    /**
     * @param string $bidsDir       Dataset root (holding sub-*).
     * @param array  $projectConfig Decoded project.json ("visits", optional).
     *
     * @return array<string, array<string, string>> sub-<ID> => [ses-<visit> => why, ...]
     */
    public static function held(string $bidsDir, array $projectConfig): array
    {
        $held = [];
        foreach (glob(rtrim($bidsDir, '/') . '/sub-*', GLOB_ONLYDIR) ?: [] as $subDir) {
            $sub = basename($subDir);
            foreach (glob($subDir . '/ses-*', GLOB_ONLYDIR) ?: [] as $sesDir) {
                $ses   = basename($sesDir);
                $visit = substr($ses, 4);
                if ($visit === '' || !preg_match('/^[A-Za-z0-9]+$/', $visit)) {
                    $held[$sub][$ses] = "'{$ses}' is not a valid visit folder name (letters and digits only after ses-)";
                    continue;
                }
                $why = VisitLabel::problem($visit, $projectConfig);
                if ($why !== null) {
                    $held[$sub][$ses] = $why . (VisitLabel::hint($visit, $projectConfig) !== null
                        ? " (the folder and the {$ses} in its file names)"
                        : ' - fix the folder name, or add the visit to project.json "visits" (and set it up in LORIS)');
                    continue;
                }
                $off = self::_mislabelled($sesDir, $ses);
                if ($off !== null) {
                    $held[$sub][$ses] = "file {$off['file']} is labelled {$off['label']} but sits in {$ses}"
                        . ($off['count'] > 1 ? " ({$off['count']} such files)" : '')
                        . ' - rename the files (or the folder) so the labels match';
                }
            }
        }
        return $held;
    }

    /**
     * The first file under a ses- folder whose name carries a different ses-
     * label, with how many there are; null when all match.
     */
    private static function _mislabelled(string $sesDir, string $ses): ?array
    {
        $first = null;
        $count = 0;
        $it = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($sesDir, \FilesystemIterator::SKIP_DOTS)
        );
        foreach ($it as $file) {
            if (!$file->isFile()) {
                continue;
            }
            if (preg_match('/_(ses-[^_]+)_/', $file->getFilename(), $m) && $m[1] !== $ses) {
                $count++;
                $first ??= ['file' => substr($file->getPathname(), strlen(dirname($sesDir)) + 1), 'label' => $m[1]];
            }
        }
        return $first === null ? null : $first + ['count' => $count];
    }
}
