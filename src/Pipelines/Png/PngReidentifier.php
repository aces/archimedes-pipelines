<?php

/**
 * PngReidentifier
 *
 * Step 2 of the PNG pipeline. For every linked visit of the scan manifest:
 *
 *   1. find the participant's LORIS candidate by external ID (created by
 *      participant sync; --create-candidates creates it here instead)
 *   2. make sure the session exists (PUT /candidates/{CandID}/{Visit})
 *   3. copy the visit's accepted images, byte for byte, to
 *        deidentified-lorisid/imaging/png/sub-<PSCID>/ses-<Visit>/<original name>
 *      and write participants.tsv (sub-<PSCID>) and sub-<PSCID>/sessions.tsv
 *      (ses-<Visit>, acq_date) there - the layout import_png_dataset.py reads
 *   4. record provenance: every LORIS-side copy <- the file the site sent
 *
 * How this differs from DICOM reidentification, on purpose:
 *   - nothing inside a PNG identifies anyone (no header), so nothing is
 *     rewritten: the identity is the folder names, and "reidentify" means
 *     renaming sub-<site ID> to sub-<PSCID> and ses-<folder> to the LORIS visit
 *     label. The image bytes are never altered (user guide section 4); every
 *     copy is verified against the md5 taken at scan time.
 *   - the original file names are kept: they carry the scan type, and the
 *     LORIS script gives each file its own name (<CandID>_<Visit>_<type>_<NNN>.png)
 *     when it inserts it, so a re-delivery never overwrites anything.
 *   - the unit is the visit, and identity is the image hash. An image already
 *     reidentified for another visit/participant is set aside (LORIS would
 *     skip it as already registered, so it would silently stay where it was).
 *
 * Safety rules shared with DICOM:
 *   - a visit already reidentified is redone only when its fingerprint (images
 *     + hashes + acq_date) changed, or with --force
 *   - never downgraded: a visit that lost its participants.tsv row stays as is
 *   - removed from the delivery: the working copy is removed, LORIS is NOT
 *     changed, and it is reported on every run
 *   - a LORIS error creating a candidate or session is an error (the bundle
 *     stops before the import); a visit LORIS refuses for the cohort, or one
 *     not in project.json "visits", is held back with a warning (the rest go in)
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Png;

use LORIS\Endpoints\LorisApiClientInterface;

class PngReidentifier
{
    private bool $dryRun = true;
    private bool $force = false;
    private bool $createCandidates = false;

    /** @var array<string, true> raw visit keys "SUB/VISIT" left out by the operator */
    private array $excludedVisits = [];

    /** @var array<string, true> raw subjects left out by the operator */
    private array $excludedSubjects = [];

    /** @var string[]|null */
    private ?array $configuredVisits = null;

    /** @var array<int, array{level: string, key: ?string, message: string}> */
    private array $problems = [];

    /** @var callable */
    private $out;

    private array $stats = [
        'visits_total'     => 0,
        'visits_mapped'    => 0,
        'visits_unchanged' => 0,
        'visits_held_back' => 0,
        'visits_failed'    => 0,
        'visits_unlinked'  => 0,
        'visits_excluded'  => 0,
        'images_copied'    => 0,
        'images_set_aside' => 0,
        'candidates_created' => 0,
        'sessions_ensured' => 0,
    ];

    public function __construct(
        private string $projectPath,
        private LorisApiClientInterface $api,
        ?callable $out = null
    ) {
        $this->out = $out ?? static function (string $line): void {
            fwrite(STDERR, $line . "\n");
        };
    }

    public function setDryRun(bool $v): void
    {
        $this->dryRun = $v;
    }

    public function setForce(bool $v): void
    {
        $this->force = $v;
    }

    public function setCreateCandidates(bool $v): void
    {
        $this->createCandidates = $v;
    }

    public function setExcluded(array $visits, array $subjects): void
    {
        $this->excludedVisits   = array_flip(array_map('strval', $visits));
        $this->excludedSubjects = array_flip(array_map('strval', $subjects));
    }

    public function getStats(): array
    {
        return $this->stats;
    }

    public function getProblems(): array
    {
        return $this->problems;
    }

    public function hasErrors(): bool
    {
        foreach ($this->problems as $p) {
            if ($p['level'] === 'error') {
                return true;
            }
        }
        return false;
    }

    // =========================================================================

    /**
     * Reidentify every linked visit of a scan manifest.
     *
     * @param array    $manifest PngDelivery::scan() result / saved manifest.
     * @param callable $rowFor   fn(string $subject): ?array participants.tsv row (enriched).
     *
     * @return array<string, array> Reidentified visits by raw key (this run + kept).
     */
    public function map(array $manifest, callable $rowFor): array
    {
        $trackPath = PngPaths::of($this->projectPath, PngPaths::REIDENT_TRACKING);
        $tracking  = PngPaths::readJson($trackPath);
        $tracked   = (array) ($tracking['visits'] ?? []);
        $root      = PngPaths::of($this->projectPath, PngPaths::LORISID_SUBDIR);

        if (!$this->dryRun && !is_dir($root) && !@mkdir($root, 0775, true) && !is_dir($root)) {
            throw new \RuntimeException("Cannot create {$root}");
        }

        $seen = [];
        foreach ($manifest['visits'] ?? [] as $visit) {
            $key = (string) $visit['key'];
            $seen[$key] = true;
            $this->stats['visits_total']++;
            $was = $tracked[$key] ?? null;

            if (isset($this->excludedVisits[$key]) || isset($this->excludedSubjects[(string) $visit['subject']])) {
                $this->_info("  sub-{$visit['subject']}/ses-{$visit['session']}: excluded (issue reported) - no session, no copy this run");
                $this->stats['visits_excluded']++;
                continue;
            }

            if (($visit['link_status'] ?? '') !== 'linked') {
                if ($was !== null && ($was['link_status'] ?? '') === 'linked' && empty($was['removed_at'])) {
                    $this->_problem('warning', $key, sprintf(
                        'sub-%s/ses-%s is now %s (%s), but was already reidentified as %s - kept as is, '
                        . 'not re-imported. Fix the delivery, or delete it in LORIS if it really must go',
                        $visit['subject'], $visit['session'], $visit['link_status'], $visit['link_reason'] ?? '?',
                        $was['lorisid_key'] ?? '?'
                    ));
                } elseif (($visit['link_status'] ?? '') === 'unlinked') {
                    $this->stats['visits_unlinked']++;
                }
                continue;
            }

            if ($was !== null && !$this->force && empty($was['removed_at'])
                && ($was['fingerprint'] ?? null) === $visit['fingerprint']
                && ($was['external_id'] ?? null) === $visit['external_id']
                && is_dir((string) ($was['directory'] ?? ''))
                // An image set aside last time because another visit had it:
                // that visit may be gone now, so look again.
                && array_filter($was['set_aside'] ?? [], static fn ($s): bool
                    => str_contains((string) ($s['reason'] ?? ''), 'already reidentified')) === []
            ) {
                $this->_info("  sub-{$visit['subject']}/ses-{$visit['session']}: unchanged since " . ($was['mapped_at'] ?? '?') . ' - skipped');
                $this->stats['visits_unchanged']++;
                continue;
            }

            // Already reidentified to one candidate; the site identifier
            // changed (external_id column edited). Not moved silently.
            if ($was !== null && !$this->force && empty($was['removed_at'])
                && ($was['external_id'] ?? null) !== $visit['external_id']
            ) {
                $this->_problem('warning', $key, sprintf(
                    '%s: external ID changed from %s to %s, but the visit is already reidentified as %s - '
                    . 'kept, NOT moved in LORIS. Undo the change, or re-run with --force to re-assign it',
                    $key, $was['external_id'] ?? '?', $visit['external_id'], $was['lorisid_key'] ?? '?'
                ));
                continue;
            }

            $entry = $this->_mapVisit($visit, $was, $tracked, $rowFor($visit['subject']));
            if ($entry === null) {
                continue;
            }

            $tracked[$key] = $entry;
            if (!$this->dryRun) {
                $this->_saveTracking($trackPath, $tracked);
            }
        }

        // Reidentified before, gone from the delivery now.
        foreach ($tracked as $key => $was) {
            if (isset($seen[$key]) || !empty($was['removed_at']) || ($was['link_status'] ?? '') !== 'linked') {
                continue;
            }
            $this->_problem('warning', $key, sprintf(
                'REMOVED from delivery: sub-%s/ses-%s (%s, %d image(s)) - %s; LORIS is not changed',
                $was['subject'] ?? '?', $was['session'] ?? '?', $was['lorisid_key'] ?? '?',
                count($was['images'] ?? []),
                $this->dryRun ? 'would remove the reidentified copy' : 'reidentified copy removed'
            ));
            if (!$this->dryRun) {
                $this->_removeCopy($was, []);
                $tracked[$key]['removed_at'] = date('c');
                $this->_saveTracking($trackPath, $tracked);
            }
        }

        if (!$this->dryRun) {
            $this->_writeLorisidTsvs($root, $tracked);
            $this->_writeManifest($tracked);
        }

        return $tracked;
    }

    /**
     * One visit: candidate, session, copy.
     */
    private function _mapVisit(array $visit, ?array $was, array $tracked, ?array $row): ?array
    {
        $key     = (string) $visit['key'];
        $subject = (string) $visit['subject'];
        $session = (string) $visit['session'];
        $label   = "sub-{$subject}/ses-{$session}";

        $this->_info("  {$label}" . ($was !== null ? ' (changed - redone)' : ''));

        // --- visit label: project.json "visits" spelling --------------------------
        $visitLabel = $this->_resolveVisitLabel($session);
        if ($visitLabel === null) {
            $this->stats['visits_held_back']++;
            $this->_problem('warning', $key, sprintf(
                'VISIT MISMATCH %s: not reidentified, not ingested - %s. The ses- folder, its sessions.tsv '
                . 'session_id and project.json "visits" must match exactly; fix it, then re-run',
                $label, (string) $this->api->visitProblem($session)
            ));
            return null;
        }
        if (!preg_match(PngDelivery::LABEL_REGEX, $visitLabel)) {
            return $this->_reject($key, "{$label}: LORIS visit label '{$visitLabel}' is not letters and digits - "
                . 'import_png_dataset.py cannot read it from a ses- folder name. Ask the LORIS team how to '
                . 'ingest PNG images for this visit');
        }

        // --- candidate ----------------------------------------------------------------
        $externalId = (string) $visit['external_id'];
        try {
            $candidate = $this->api->findCandidateByExternalId($externalId);
        } catch (\Throwable $e) {
            return $this->_reject($key, "candidate lookup failed for {$externalId}: " . $e->getMessage());
        }

        if ($candidate === null) {
            if ($this->dryRun) {
                $this->_info("    candidate for {$externalId} would be created by participant sync (dry run)");
                $candidate = ['pscid' => 'PSCID-PENDING', 'cand_id' => 'CANDID-PENDING'];
            } elseif (!$this->createCandidates) {
                return $this->_reject($key, "no candidate for external ID '{$externalId}' - run "
                    . 'run_png_participant_sync.php first, or check the participants.tsv row');
            } else {
                try {
                    $candidate = $this->api->createCandidate(
                        $subject, $externalId, $row['dob'] ?? null, $row['sex'] ?? null, $row['site'] ?? null
                    );
                    $this->stats['candidates_created']++;
                } catch (\Throwable $e) {
                    return $this->_reject($key, "candidate creation failed for {$externalId}: " . $e->getMessage());
                }
            }
        }

        $pscid  = (string) ($candidate['pscid'] ?? '');
        $candId = (string) ($candidate['cand_id'] ?? '');
        if ($candId === '') {
            return $this->_reject($key, "no CandID resolved for {$externalId} (PSCID {$pscid}) - the session "
                . 'cannot be created (the API user may lack access to the candidate\'s site or project)');
        }
        if (!$this->dryRun && !preg_match(PngDelivery::LABEL_REGEX, $pscid)) {
            return $this->_reject($key, "PSCID '{$pscid}' of {$externalId} is not letters and digits - "
                . 'import_png_dataset.py reads the participant from a sub-<PSCID> folder name and cannot '
                . 'read this one. Ask the LORIS team (PSCID format of the project)');
        }

        $lorisKey = "{$pscid}/{$visitLabel}";
        foreach ($tracked as $otherKey => $other) {
            if ($otherKey !== $key && empty($other['removed_at']) && ($other['lorisid_key'] ?? '') === $lorisKey
                && !$this->dryRun
            ) {
                return $this->_reject($key, "{$label} and sub-{$other['subject']}/ses-{$other['session']} both "
                    . "resolve to {$lorisKey} (same candidate and visit) - merge them into one visit folder, "
                    . 'or correct the identifier');
            }
        }

        // --- images already reidentified elsewhere ------------------------------------
        $elsewhere = [];
        foreach ($tracked as $otherKey => $other) {
            if ($otherKey === $key || !empty($other['removed_at'])) {
                continue;
            }
            foreach ($other['images'] ?? [] as $img) {
                $elsewhere[$img['md5']] = ($other['lorisid_key'] ?? $otherKey) . '/' . ($img['original_name'] ?? '?');
            }
        }
        $images   = [];
        $setAside = $visit['set_aside'] ?? [];
        foreach ($visit['images'] as $img) {
            if (isset($elsewhere[$img['md5']])) {
                $reason = 'is byte-identical to ' . $elsewhere[$img['md5']] . ', already reidentified for '
                    . 'another visit - LORIS would skip it as already registered, so it is not sent again';
                $setAside[] = ['name' => $img['name'], 'path' => $img['path'], 'reason' => $reason];
                $this->_problem('warning', $key, "SET ASIDE {$label}/{$img['name']}: {$reason}");
                continue;
            }
            $images[] = $img;
        }
        $this->stats['images_set_aside'] += count($setAside);

        if ($images === []) {
            $this->stats['visits_held_back']++;
            $this->_problem('warning', $key, "{$label}: no image left to ingest - nothing done for this visit");
            return null;
        }

        // --- session ------------------------------------------------------------------
        if (!$this->dryRun) {
            try {
                $this->api->ensureSession($candId, $visitLabel, $row['cohort'] ?? null);
                $this->stats['sessions_ensured']++;
            } catch (\Throwable $e) {
                if ($e->getCode() !== LorisApiClientInterface::VISIT_REFUSED) {
                    return $this->_reject($key, "session creation failed for {$candId}/{$visitLabel}: "
                        . $e->getMessage());
                }
                $this->stats['visits_held_back']++;
                $this->_problem('warning', $key, sprintf(
                    "VISIT NOT IN LORIS %s: not reidentified, not ingested - %s. Set up visit '%s' for the "
                    . 'project and cohort in LORIS (or fix the ses- folder name), then re-run',
                    $label, $e->getMessage(), $visitLabel
                ));
                return null;
            }
        }

        // --- copy ---------------------------------------------------------------------
        $target = PngPaths::of($this->projectPath, PngPaths::LORISID_SUBDIR) . "/sub-{$pscid}/ses-{$visitLabel}";
        $this->_info("    -> sub-{$pscid}/ses-{$visitLabel} (" . count($images) . ' image(s))');

        $this->_reportImageChanges($label, $was, $images);

        $copied = [];
        if (!$this->dryRun) {
            // Moved to another candidate/visit with --force: the old copy goes.
            if ($was !== null && ($was['directory'] ?? '') !== $target) {
                $this->_removeCopy($was, []);
            }
            try {
                $this->_ensureDir($target);
                $keep = array_column($images, 'name');
                $this->_removeCopy(['directory' => $target], $keep);   // images no longer delivered
                foreach ($images as $img) {
                    $this->_copyVerified($img['path'], $target . '/' . $img['name'], $img['md5']);
                    $this->stats['images_copied']++;
                }
            } catch (\Throwable $e) {
                return $this->_reject($key, "copy to {$target} failed: " . $e->getMessage());
            }
        }
        foreach ($images as $img) {
            $copied[] = [
                'original_name' => $img['name'],
                'original_path' => $img['path'],
                'lorisid_path'  => $target . '/' . $img['name'],
                'scan_type'     => $img['scan_type'],
                'md5'           => $img['md5'],
                'size'          => $img['size'],
            ];
        }

        $this->stats['visits_mapped']++;

        return [
            'key'              => $key,
            'link_status'      => 'linked',
            'subject'          => $subject,
            'session'          => $session,
            'external_id'      => $externalId,
            'pscid'            => $pscid,
            'cand_id'          => $candId,
            'visit_label'      => $visitLabel,
            'lorisid_key'      => $lorisKey,
            'directory'        => $target,
            'source_directory' => $visit['directory'],
            'acq_date'         => $visit['acq_date'],
            'fingerprint'      => $visit['fingerprint'],
            'lorisid_fingerprint' => PngDelivery::fingerprint(
                array_map(static fn ($i) => ['name' => $i['original_name'], 'md5' => $i['md5']], $copied),
                $visit['acq_date']
            ),
            'images'           => $copied,
            'set_aside'        => array_map(static fn ($s) => ['name' => $s['name'], 'reason' => $s['reason']], $setAside),
            'mapped_at'        => date('c'),
        ];
    }

    /**
     * A visit redone after the site changed it: images removed or replaced
     * stay in LORIS (nothing is ever deleted there automatically).
     */
    private function _reportImageChanges(string $label, ?array $was, array $images): void
    {
        if ($was === null) {
            return;
        }
        $now = [];
        foreach ($images as $i) {
            $now[$i['name']] = $i['md5'];
        }
        foreach ($was['images'] ?? [] as $old) {
            $name = $old['original_name'];
            if (!isset($now[$name])) {
                $this->_problem('warning', null, "REMOVED image {$label}/{$name}: no longer delivered - "
                    . 'removed from the reidentified copy; if it was ingested it is STILL IN LORIS');
            } elseif ($now[$name] !== $old['md5']) {
                $this->_problem('warning', null, "CHANGED image {$label}/{$name}: new content - the new image "
                    . 'is ingested as an additional file; the previous one is STILL IN LORIS (delete it there '
                    . 'if it was wrong)');
            }
        }
    }

    /**
     * The ses- folder name when it is exactly a project.json "visits" entry
     * (or none are declared), else null - held back (LORIS\Utils\VisitLabel).
     */
    private function _resolveVisitLabel(string $session): ?string
    {
        $this->configuredVisits ??= $this->api->getConfiguredVisitLabels();
        return $this->api->resolveVisitLabel($session);
    }

    /**
     * Copy through a temporary name, then check the copy's md5 against the
     * hash taken at scan time: what goes to LORIS is exactly what the site sent.
     */
    private function _copyVerified(string $source, string $target, string $md5): void
    {
        $now = @md5_file($source);
        if ($now !== $md5) {
            // Stale md5 (file changed, size and mtime kept): forget the cache so
            // the next run reads every image again.
            @unlink(PngPaths::of($this->projectPath, PngPaths::HASH_CACHE));
            throw new \RuntimeException("{$source} changed since it was scanned - re-run the bundle");
        }
        if (is_file($target) && md5_file($target) === $md5) {
            return;                                    // already there, identical
        }
        $tmp = dirname($target) . '/.' . basename($target) . '.tmp';
        if (!@copy($source, $tmp)) {
            throw new \RuntimeException("cannot copy {$source}");
        }
        if (md5_file($tmp) !== $md5) {
            @unlink($tmp);
            throw new \RuntimeException("copy of {$source} does not match the original (disk or network problem)");
        }
        @chmod($tmp, 0664);
        if (!@rename($tmp, $target)) {
            @unlink($tmp);
            throw new \RuntimeException("cannot write {$target}");
        }
    }

    private function _ensureDir(string $dir): void
    {
        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new \RuntimeException("cannot create {$dir}");
        }
    }

    /**
     * Remove the PNG copies of a reidentified visit, except $keep (names). The
     * folder goes when it is empty. Only *.png files inside
     * deidentified-lorisid/ are ever removed.
     */
    private function _removeCopy(array $entry, array $keep): void
    {
        $dir  = (string) ($entry['directory'] ?? '');
        $root = PngPaths::of($this->projectPath, PngPaths::LORISID_SUBDIR) . '/';
        if ($dir === '' || !str_starts_with($dir, $root) || !is_dir($dir)) {
            return;
        }
        $keepSet = array_flip($keep);
        foreach (glob($dir . '/*') ?: [] as $f) {
            if (is_file($f) && preg_match('/\.png$/i', $f) && !isset($keepSet[basename($f)])) {
                @unlink($f);
            }
        }
        if ($keep === [] && (glob($dir . '/*') ?: []) === []) {
            @rmdir($dir);
        }
    }

    /**
     * participants.tsv and each sub-<PSCID>/sessions.tsv of the reidentified
     * tree, from what is reidentified (only the identifier, never dob/sex: the
     * LORIS script needs participant_id only, and the candidate already has the
     * demographics). Every sub- folder on disk gets a row, so the LORIS script
     * never meets a participant folder without one.
     */
    private function _writeLorisidTsvs(string $root, array $tracked): void
    {
        $sessions = [];
        foreach ($tracked as $e) {
            if (!empty($e['removed_at']) || ($e['link_status'] ?? '') !== 'linked') {
                continue;
            }
            $sessions[$e['pscid']][$e['visit_label']] = $e['acq_date'] ?? '';
        }
        foreach (glob($root . '/sub-*', GLOB_ONLYDIR) ?: [] as $d) {
            $pscid = substr(basename($d), 4);
            // A participant whose every visit was removed: its folder only
            // holds the old sessions.tsv now - remove both, so it is not
            // listed any more.
            if (!isset($sessions[$pscid]) && (glob($d . '/ses-*', GLOB_ONLYDIR) ?: []) === []) {
                @unlink($d . '/sessions.tsv');
                if ((glob($d . '/*') ?: []) === [] && @rmdir($d)) {
                    continue;
                }
            }
            $sessions[$pscid] ??= [];
        }
        ksort($sessions, SORT_STRING);

        $lines = ["participant_id"];
        foreach (array_keys($sessions) as $pscid) {
            $lines[] = "sub-{$pscid}";
        }
        $this->_writeText($root . '/participants.tsv', implode("\n", $lines) . "\n");

        foreach ($sessions as $pscid => $visits) {
            $dir = "{$root}/sub-{$pscid}";
            if (!is_dir($dir)) {
                continue;
            }
            ksort($visits, SORT_STRING);
            $rows = ["session_id\tacq_date"];
            foreach ($visits as $visit => $acq) {
                $rows[] = "ses-{$visit}\t{$acq}";
            }
            $this->_writeText("{$dir}/sessions.tsv", implode("\n", $rows) . "\n");
        }
    }

    private function _writeManifest(array $tracked): void
    {
        $visits = array_values(array_filter($tracked, static fn ($e) => empty($e['removed_at'])
            && ($e['link_status'] ?? '') === 'linked'));
        PngPaths::writeJson(PngPaths::of($this->projectPath, PngPaths::LORISID_MANIFEST), [
            'generated_at' => date('c'),
            'visits'       => $visits,
            'problems'     => $this->problems,
            'stats'        => $this->stats,
        ]);

        $tsv = ["lorisid_path\toriginal_path\tmd5\tscan_type\texternal_id\tpscid\tcand_id\tvisit_label"];
        foreach ($visits as $v) {
            foreach ($v['images'] as $i) {
                $tsv[] = implode("\t", [$i['lorisid_path'], $i['original_path'], $i['md5'], $i['scan_type'],
                    $v['external_id'], $v['pscid'], $v['cand_id'], $v['visit_label']]);
            }
        }
        $this->_writeText(PngPaths::of($this->projectPath, PngPaths::PROVENANCE_TSV), implode("\n", $tsv) . "\n");
    }

    private function _saveTracking(string $path, array $tracked): void
    {
        PngPaths::writeJson($path, ['updated_at' => date('c'), 'visits' => $tracked]);
    }

    private function _writeText(string $path, string $text): void
    {
        $tmp = $path . '.tmp.' . getmypid();
        if (@file_put_contents($tmp, $text) === false || !@rename($tmp, $path)) {
            @unlink($tmp);
            throw new \RuntimeException("Cannot write {$path}");
        }
        @chmod($path, 0664);
    }

    private function _reject(string $key, string $message): ?array
    {
        $this->stats['visits_failed']++;
        $this->_problem('error', $key, $message);
        return null;
    }

    private function _problem(string $level, ?string $key, string $message): void
    {
        $this->problems[] = ['level' => $level, 'key' => $key, 'message' => $message];
        ($this->out)('    ' . strtoupper($level === 'error' ? 'ERROR' : 'WARN') . ' ' . $message);
    }

    private function _info(string $message): void
    {
        ($this->out)($message);
    }
}
