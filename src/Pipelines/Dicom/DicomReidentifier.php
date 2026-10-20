<?php
/**
 * DicomReidentifier
 *
 * Stage 2 of the ARCHIMEDES DICOM pipeline.
 *
 * Consumes the stage 1 manifest, resolves each study's external identifier to
 * a LORIS candidate (creating one where needed), confirms the visit label is
 * configured, copies the study into the LORIS-id working area, and rewrites
 * the patient identity headers on that copy.
 *
 * All LORIS interaction goes through LorisApiClientInterface - this class
 * never opens a database connection.
 *
 * Unlinked studies (no matching participant or visit) are only reported:
 * no candidate, no session, no copy, nothing to ingest.
 *
 * PHP Version 8.1
 *
 * @category Pipeline
 * @package  Archimedes
 */

namespace LORIS\Pipelines\Dicom;

use LORIS\Endpoints\LorisApiClientInterface;
use LORIS\Pipelines\Shared\TrackingFile;

class DicomReidentifier
{
    /** Studies are written under <target>/dicoms/ */
    const STUDY_SUBDIR = 'dicoms';

    /** Idempotency state, keyed on StudyInstanceUID. */
    const TRACK_FILE        = '.dicom_reidentify_tracking.json';
    const LEGACY_TRACK_FILE = '.dicom_mapping_tracking.json';

    /** Handoff to the archiving stage. */
    const MANIFEST_FILE = 'dicom_studies_lorisid.json';

    /**
     * Per-study provenance sidecar, written next to each study directory as
     * <directory_name>.provenance.json.
     *
     * The manifest
     * records link_status, but both can be lost: directories get renamed, and
     * a manifest can be separated from the data it describes. The sidecar
     * travels with the study and states plainly whether reidentification
     * actually happened - which matters because everything under
     * deidentified-lorisid/ looks reidentified by virtue of living there.
     *
     * Written beside the directory rather than inside it so that
     * import_dicom_study.py does not sweep it into the tarchive as an
     * unexpected non-DICOM file.
     */
    const PROVENANCE_SUFFIX = '.provenance.json';

    /** @var LorisApiClientInterface */
    private $api;

    /** @var DicomHeaderWriter */
    private $headerWriter;

    /** @var string Absolute path to the stage 2 output root. */
    private $targetDir;

    /** @var object|null */
    private $logger;

    /** @var bool */
    private $dryRun = false;

    /** @var bool Remap every study, ignoring tracking. */
    private $force = false;

    /**
     * Candidate creation belongs to run_dicom_participant_sync.php, which owns
     * participants.tsv and can report orphans across the whole delivery. This
     * stage only resolves. Left as a flag rather than removed so a single-step
     * run can still be made to work, but it defaults off.
     *
     * @var bool
     */
    private $createCandidates = false;

    /** @var string[]|null Lazily fetched. */
    private $configuredVisits = null;

    /** @var array<int, array> */
    private $problems = [];

    /** @var array */
    private $stats = [
        'studies_total'       => 0,
        'studies_mapped'      => 0,
        'studies_skipped'     => 0,
        'candidates_created'  => 0,
        'candidates_matched'  => 0,
        'unlinked_studies'    => 0,
        'studies_degraded'    => 0,
        'files_rewritten'     => 0,
    ];

    public function __construct(
        LorisApiClientInterface $api,
        DicomHeaderWriter $headerWriter,
        string $targetDir,
                                $logger = null
    ) {
        $this->api          = $api;
        $this->headerWriter = $headerWriter;
        $this->targetDir    = rtrim($targetDir, '/');
        $this->logger       = $logger;
    }

    public function setDryRun(bool $v): void
    {
        $this->dryRun = $v;
    }

    /**
     * Allow this stage to create candidates it cannot find.
     *
     * Off by default. run_dicom_participant_sync.php is the owner: it sees the
     * whole participants.tsv, so it can report orphan directories and orphan
     * rows together, which this stage cannot - it only ever sees studies that
     * organized successfully.
     *
     * @param bool $v Whether to create missing candidates.
     *
     * @return void
     */
    public function setForce(bool $v): void
    {
        $this->force = $v;
    }

    public function setCreateCandidates(bool $v): void
    {
        $this->createCandidates = $v;
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
        foreach ($this->problems as $problem) {
            if ($problem['level'] === 'error') {
                return true;
            }
        }

        return false;
    }

    // =========================================================================
    //  ENTRY POINT
    // =========================================================================

    /**
     * Map every study in a stage 1 manifest.
     *
     * @param string $manifestPath    Path to dicom_studies.json.
     * @param array  $participantRows participant_id => row, for demographics.
     *
     * @return array Stage 2 manifest entries.
     *
     * @throws \RuntimeException on unrecoverable error.
     */
    public function mapFromManifest(string $manifestPath, array $participantRows = []): array
    {
        $this->headerWriter->assertAvailable();

        $stage1 = $this->_readJson($manifestPath);

        if (!isset($stage1['studies']) || !is_array($stage1['studies'])) {
            throw new \RuntimeException("Manifest has no studies array: {$manifestPath}");
        }

        // A dry-run manifest describes studies that were never written. Acting
        // on it for real would copy directories that do not exist.
        if (!empty($stage1['dry_run']) && !$this->dryRun) {
            throw new \RuntimeException(
                "Manifest at {$manifestPath} was produced by a dry run. "
                . 'Re-run run_dicom_organize.php with --confirm first.'
            );
        }

        $this->stats['studies_total'] = count($stage1['studies']);
        $this->_ensureTargetDir();

        $tracking = $this->_readTracking();
        $output   = [];
        $seen     = [];

        // Studies the operator left out (issues reported): untouched this run,
        // and not mistaken for studies removed from the delivery.
        foreach ($this->excludedUids as $uid => $_) {
            $seen[$uid] = true;
        }

        foreach ($stage1['studies'] as $study) {
            $studyUid = $study['study_instance_uid'] ?? null;

            if ($studyUid === null) {
                $this->_problem('error', null, 'manifest entry without study_instance_uid');
                continue;
            }

            $tracked = $tracking['studies'][$studyUid] ?? null;
            $seen[$studyUid] = true;

            if (isset($this->excludedUids[$studyUid])) {
                $this->_info("  {$studyUid}: excluded (issue reported) - no session, no reidentify this run");
                $this->stats['studies_skipped']++;
                if ($tracked !== null) {
                    $output[] = $tracked;
                }
                continue;
            }

            // A study already reidentified to a LORIS candidate is not downgraded
            // to unlinked because its participants.tsv row went missing: that
            // is almost always an edit mistake, and re-archiving it unlinked
            // would rewrite PatientName in LORIS while it stays in the visit.
            // Keep the reidentified copy and say so; withdrawals are a LORIS-side
            // deletion, never an automatic one.
            // Never downgrade a reidentified study - not for a missing row, an
            // unconfigured visit, and not with --force:
            // re-archiving it would overwrite PatientName in LORIS with the
            // site identifier while the study stays in its visit.
            if ($tracked !== null
                && ($tracked['link_status'] ?? null) === 'linked'
                && ($study['link_status'] ?? null) === 'unlinked'
            ) {
                $this->_warn(sprintf(
                    '%s: now %s (%s), but already reidentified to %s (%s) - kept as matched, '
                    . 'not re-imported. Fix the delivery (e.g. restore the participants.tsv row), '
                    . 'or remove the study in LORIS if it really must be unlinked.',
                    $studyUid,
                    $study['link_status'],
                    $study['link_reason'] ?? 'declared',
                    $tracked['loris_patient_name'] ?? ($tracked['cand_id'] ?? '?'),
                    basename((string) ($tracked['directory'] ?? ''))
                ));
                $this->stats['studies_skipped']++;
                $output[] = $tracked;
                continue;
            }

            // An already reidentified study that moved to another participant
            // or visit folder is not re-assigned automatically: that would move
            // archived data between candidates or sessions in LORIS on the
            // strength of a folder rename. Kept, warned; --force re-assigns.
            if ($tracked !== null && !$this->force
                && ($tracked['link_status'] ?? null) === 'linked'
                && ($study['link_status'] ?? null) === 'linked'
            ) {
                $extWas   = $tracked['source_external_id'] ?? null;
                $extNow   = $study['external_id'] ?? null;
                $visitWas = $tracked['visit_label'] ?? null;
                $visitNow = $this->_resolveVisitLabel($study['session'] ?? null);

                if (($extWas !== null && $extNow !== $extWas)
                    || ($visitWas !== null && $visitNow !== null && $visitNow !== $visitWas)
                ) {
                    $this->_warn(sprintf(
                        '%s: moved in the delivery from %s/%s to %s/%s, but already reidentified as %s - '
                        . 'kept, NOT moved in LORIS. Undo the move, or re-run with --force to re-assign it.',
                        $studyUid,
                        $extWas ?? '?',
                        $visitWas ?? '?',
                        $extNow ?? '?',
                        $visitNow ?? '?',
                        $tracked['loris_patient_name'] ?? basename((string) ($tracked['directory'] ?? ''))
                    ));
                    $this->stats['studies_skipped']++;
                    $output[] = $tracked;
                    continue;
                }
            }

            $why     = $this->_remapReason($tracked, $study);

            if ($why === null) {
                $this->_info("  Already mapped, skipping: {$studyUid}");
                $this->stats['studies_skipped']++;
                $output[] = $tracked;
                continue;
            }

            if ($tracked !== null) {
                $this->_info("  Remapping {$studyUid}: {$why}");
            }

            // A remapped study may reuse its own folder: without this, new
            // series in ARCHI0001_300001_V01_MR would land in ..._MR_2.
            $this->ownDir = $tracked !== null
                ? basename((string) ($tracked['directory'] ?? ''))
                : null;
            $entry = $this->_mapStudy($study, $participantRows);
            $this->ownDir = null;

            if ($entry === null) {
                $this->stats['studies_skipped']++;
                continue;
            }

            // The previous copy (e.g. unlinked-ARCHI0003_V01_MR, now
            // ARCHI0003_300003_V01_MR) would otherwise stay in
            // deidentified-lorisid/ and be imported again later.
            if ($tracked !== null && !$this->dryRun
                && ($tracked['directory'] ?? null) !== ($entry['directory'] ?? null)
            ) {
                $this->_removeStudyCopy($tracked);
            }

            $this->_writeProvenance($entry);

            $output[] = $entry;
            $tracking['studies'][$studyUid] = $entry;

            if (!$this->dryRun) {
                $this->_writeTracking($tracking);
            }
        }

        // Reidentified before, gone from the organized manifest now: the study was
        // removed from the delivery. Remove the working copy so it cannot be
        // imported again, keep the tracking entry so a re-deposit is matched.
        foreach ($tracking['studies'] ?? [] as $uid => $tracked) {
            if (isset($seen[$uid]) || !empty($tracked['removed_at'])) {
                continue;
            }
            $this->_warn(sprintf(
                'REMOVED from delivery: %s (%s) - %s; LORIS is not changed',
                $uid,
                basename((string) ($tracked['directory'] ?? '?')),
                $this->dryRun ? 'would remove reidentified copy' : 'reidentified copy removed'
            ));
            if (!$this->dryRun) {
                $this->_removeStudyCopy($tracked);
                $tracking['studies'][$uid]['removed_at'] = date('c');
                $this->_writeTracking($tracking);
            }
        }

        if (!$this->dryRun) {
            $this->_writeManifest($output);
        }

        return $output;
    }

    // =========================================================================
    //  PER-STUDY
    // =========================================================================

    /**
     * Resolve, copy and rewrite one study.
     *
     * @param array $study           Stage 1 manifest entry.
     * @param array $participantRows Demographics keyed on participant_id.
     *
     * @return array|null Null when the study is skipped.
     */
    private function _mapStudy(array $study, array $participantRows): ?array
    {
        $studyUid   = $study['study_instance_uid'];
        $subject    = $study['subject'] ?? null;
        $session    = $study['session'] ?? null;
        $linkStatus = $study['link_status'] ?? 'linked';
        $isUnlinked = ($linkStatus === 'unlinked');
        $sourceDir  = $study['directory'] ?? null;
        $modalities = $study['modalities'] ?? [];

        if ($sourceDir === null || !is_dir($sourceDir)) {
            // In a dry run, organize only previews: a new study has no
            // processed/ folder yet. That is expected, not a failure.
            if ($this->dryRun) {
                $this->_info("  {$studyUid}: would reidentify once organized (dry run)");
                return null;
            }
            return $this->_reject($studyUid, "stage 1 directory missing: {$sourceDir}");
        }

        $this->_info("  {$studyUid}");

        // --- Unlinked: no participant, so nothing to do ----------------------
        // No candidate, no session, no reidentify, no copy - and so nothing for
        // the import to ingest. Reported; it is picked up on the run after its
        // participants.tsv row arrives.
        if ($isUnlinked) {
            $this->stats['unlinked_studies']++;
            $this->_problem('warning', $studyUid, sprintf(
                'UNLINKED %s: not synced, not reidentified, not ingested - %s',
                $study['directory_name'] ?? $studyUid,
                DicomPreflight::unlinkedAdvice($study['link_reason'] ?? null, $study['subject'] ?? null)
            ));
            return null;
        }

        // --- Visit must be configured -----------------------------------------
        $visitLabel = $this->_resolveVisitLabel($session);

        if ($visitLabel === null) {
            // A participant study in a visit LORIS does not have: a mismatch,
            // not an unlinked study. Rejected (the pre-flight in participant
            // sync normally stops the run before this point).
            return $this->_reject($studyUid, sprintf(
                "sub-%s: visit label '%s' is not configured in LORIS - not processed",
                $subject,
                $session ?? '?'
            ));
        }

        // --- Candidate ---------------------------------------------------------
        // external_id is set by stage 1: the sub- folder of the participant.
        $externalId = $study['external_id']
            ?? $study['patient_name']
            ?? $subject;

        try {
            $candidate = $this->api->findCandidateByExternalId($externalId);
        } catch (\Throwable $e) {
            return $this->_reject(
                $studyUid,
                "candidate lookup failed for {$externalId}: " . $e->getMessage()
            );
        }

        if ($candidate === null) {
            // Dry run of a first delivery: participant sync only previewed, so
            // the candidate it will create with --confirm does not exist yet.
            // Not an error when the participants.tsv row is there.
            if ($this->dryRun && !$this->createCandidates
                && isset($participantRows['sub-' . $subject])
            ) {
                $this->_info("    candidate for {$externalId} would be created by participant sync (dry run)");
                $candidate = ['pscid' => 'PSCID-PENDING', 'cand_id' => 'CANDID-PENDING'];
            } elseif (!$this->createCandidates) {
                return $this->_reject(
                    $studyUid,
                    "no candidate for external ID '{$externalId}' - run "
                    . 'run_dicom_participant_sync.php first, or check that the '
                    . 'participants.tsv row exists for this subject'
                );
            }

            $demographics = $participantRows['sub-' . $subject] ?? [];

            if ($this->dryRun) {
                $this->_info("    would create candidate for {$externalId}");
                $candidate = ['pscid' => 'PSCID-PENDING', 'cand_id' => 'CANDID-PENDING'];
            } else {
                try {
                    $candidate = $this->api->createCandidate(
                        $demographics['participant_id'] ?? $externalId,
                        $externalId,
                        $demographics['dob'] ?? null,
                        $demographics['sex'] ?? null
                    );
                } catch (\Throwable $e) {
                    return $this->_reject(
                        $studyUid,
                        "candidate creation failed for {$externalId}: " . $e->getMessage()
                    );
                }
                $this->stats['candidates_created']++;
            }
        } else {
            $this->stats['candidates_matched']++;
        }

        $pscid  = $candidate['pscid'];
        $candId = $candidate['cand_id'];

        // Without a CandID the name would be PSCID__Visit and no session could
        // be created, so the import could never attach it to a visit.
        if ($candId === '' || $candId === null) {
            return $this->_reject(
                $studyUid,
                "no CandID resolved for {$externalId} (PSCID {$pscid}) - "
                . 'cannot build PSCID_CandID_Visit or create the session'
            );
        }

        // --- Session -----------------------------------------------------------
        if (!$this->dryRun) {
            try {
                $this->api->ensureSession(
                    $candId,
                    $visitLabel,
                    $participantRows['sub-' . $subject]['cohort'] ?? null
                );
            } catch (\Throwable $e) {
                return $this->_reject(
                    $studyUid,
                    "session creation failed for {$candId}/{$visitLabel}: " . $e->getMessage()
                );
            }
        }

        // --- Copy and rewrite ---------------------------------------------------
        $lorisName = "{$pscid}_{$candId}_{$visitLabel}";
        $dirName   = $this->_lorisDirName($lorisName, $modalities);
        $targetDir = $this->_studyRoot() . '/' . $dirName;

        $this->_info("    -> {$dirName}");

        $rewritten = 0;

        if (!$this->dryRun) {
            $this->_copyTree($sourceDir, $targetDir);

            try {
                $rewritten = $this->headerWriter->rewritePatientIdentity(
                    $targetDir,
                    $lorisName,
                    (string) ($study['patient_name'] ?? '')
                );
            } catch (\Throwable $e) {
                // Leave the partial copy in place for inspection rather than
                // deleting evidence of what went wrong.
                return $this->_reject(
                    $studyUid,
                    'header rewrite failed: ' . $e->getMessage()
                );
            }

            $this->stats['files_rewritten'] += $rewritten;
        }

        $this->stats['studies_mapped']++;

        return $this->_entry($study, $targetDir, $dirName, [
            'link_status'           => 'linked',
            'link_reason'           => null,
            'pscid'                 => $pscid,
            'cand_id'               => $candId,
            'visit_label'           => $visitLabel,
            'loris_patient_name'    => $lorisName,
            'original_patient_name' => $study['patient_name'] ?? null,
            'files_rewritten'       => $rewritten,
        ]);
    }

    /**
     * Match a session identifier against the known visit labels (used in LORIS or
     * declared in project.json "visits"); an unknown one is returned as is.
     *
     * Compared case-insensitively, since site folder naming is inconsistent
     * and the configured label is authoritative for the returned value.
     *
     * @param string|null $session Session identifier from the folder name.
     *
     * @return string|null The known label (LORIS spelling), the label as given, or null for none.
     */
    private function _resolveVisitLabel(?string $session): ?string
    {
        if ($session === null || $session === '') {
            return null;
        }

        if ($this->configuredVisits === null) {
            $this->configuredVisits = $this->api->getConfiguredVisitLabels();
        }

        foreach ($this->configuredVisits as $label) {
            if (strcasecmp($label, $session) === 0) {
                return $label;
            }
        }

        // Not known (no session uses it yet, not in project.json "visits"):
        // the folder's label as is. LORIS checks it against the project's
        // cohort when the session is created and rejects the study if wrong.
        return $session;
    }

    /** @var array<string, true> StudyInstanceUIDs excluded by the operator. */
    private array $excludedUids = [];

    public function setExcluded(array $studyUids): void
    {
        $this->excludedUids = array_flip(array_map('strval', $studyUids));
    }

    /** @var string|null Folder of the study being remapped; free to reuse. */
    private $ownDir = null;

    private function _lorisDirName(string $lorisName, array $modalities): string
    {
        $suffix = empty($modalities) ? 'NA' : implode('-', $modalities);

        return $this->_ownDirFor($lorisName)
            ?? $this->_unique($lorisName . '_' . $suffix);
    }

    /**
     * The study's previous folder, when it differs from $base only in the
     * modality suffix. Keeping it keeps the LORIS archive name, so
     * import_dicom_study.py overwrites the archive in place instead of
     * leaving the old DCM_..._CT-PT.tar behind when a series is added/removed.
     */
    private function _ownDirFor(string $prefix): ?string
    {
        return ($this->ownDir !== null && $this->ownDir !== ''
            && str_starts_with($this->ownDir, $prefix . '_'))
            ? $this->ownDir
            : null;
    }

    /**
     * Append a counter when the directory name is already taken.
     *
     * @param string $base Desired name.
     *
     * @return string
     */
    private function _unique(string $base): string
    {
        if ($base === $this->ownDir || !is_dir($this->_studyRoot() . '/' . $base)) {
            return $base;
        }

        $n = 2;
        while (is_dir($this->_studyRoot() . "/{$base}_{$n}")) {
            $n++;
        }

        return "{$base}_{$n}";
    }

    /**
     * Why a tracked study must be mapped again, or null to skip it.
     *
     * Tracking used to be "seen this StudyInstanceUID, skip forever", which
     * left a study unlinked after its subject was added to participants.tsv,
     * and stale after organize re-wrote it with new series.
     *
     * @param array|null $tracked Previous mapping, if any.
     * @param array      $study   Current stage 1 entry.
     *
     * @return string|null
     */
    private function _remapReason(?array $tracked, array $study): ?string
    {
        if ($tracked === null) {
            return 'new';
        }
        if ($this->force) {
            return '--force';
        }

        $linkNow    = $study['link_status'] ?? null;
        $linkBefore = $tracked['source_link_status'] ?? $tracked['link_status'] ?? null;

        if ($linkNow !== $linkBefore) {
            return "link status {$linkBefore} -> {$linkNow}";
        }

        if (array_key_exists('source_external_id', $tracked)
            && ($study['external_id'] ?? null) !== $tracked['source_external_id']
        ) {
            return 'external ID changed';
        }

        $hashNow = $study['content_hash'] ?? $study['manifest_hash'] ?? null;

        // Entries written before source_hash existed are taken as current.
        if (array_key_exists('source_hash', $tracked)
            && $hashNow !== null
            && $hashNow !== $tracked['source_hash']
        ) {
            return 'organized study changed';
        }

        if (!is_dir((string) ($tracked['directory'] ?? ''))) {
            return 'copy missing';
        }

        return null;
    }

    /**
     * Remove a previous copy and its provenance sidecar from the working area.
     *
     * @param array $tracked Previous mapping.
     *
     * @return void
     */
    private function _removeStudyCopy(array $tracked): void
    {
        $dir = realpath((string) ($tracked['directory'] ?? ''));

        if ($dir === false || !$this->_deleteTree($dir)) {
            return;
        }

        $sidecar = $dir . self::PROVENANCE_SUFFIX;
        if (is_file($sidecar)) {
            unlink($sidecar);
        }

        $this->_info('    removed previous copy ' . basename($dir));
    }

    private function _entry(array $study, string $targetDir, string $dirName, array $extra): array
    {
        return array_merge(
            [
                'study_instance_uid' => $study['study_instance_uid'],
                'directory'          => $targetDir,
                'directory_name'     => $dirName,
                'source_directory'   => $study['directory'],
                'subject'            => $study['subject'] ?? null,
                'session'            => $study['session'] ?? null,
                'modalities'         => $study['modalities'] ?? [],
                'series_count'       => $study['series_count'] ?? null,
                'file_count'         => $study['file_count'] ?? null,
                'source_link_status' => $study['link_status'] ?? null,
                'source_external_id' => $study['external_id'] ?? null,
                'source_hash'        => $study['content_hash'] ?? $study['manifest_hash'] ?? null,
                'mapped_at'          => date('c'),
            ],
            $extra
        );
    }

    // =========================================================================
    //  FILESYSTEM
    // =========================================================================

    /**
     * Recursively copy a directory tree.
     *
     * @param string $source Source directory.
     * @param string $target Destination directory.
     *
     * @return void
     */
    private function _copyTree(string $source, string $target): void
    {
        // Start from an empty target: a re-mapped study may have lost files
        // since its last copy, and those must not linger and be archived.
        if (is_dir($target)) {
            $this->_deleteTree($target);
        }

        if (!is_dir($target) && !@mkdir($target, 0775, true) && !is_dir($target)) {
            throw new \RuntimeException("Could not create {$target}");
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($source, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::SELF_FIRST
        );

        foreach ($iterator as $item) {
            $dest = $target . '/' . $iterator->getSubPathName();

            if ($item->isDir()) {
                if (!is_dir($dest) && !@mkdir($dest, 0775, true) && !is_dir($dest)) {
                    throw new \RuntimeException("Could not create {$dest}");
                }
                continue;
            }

            if (!copy($item->getPathname(), $dest)) {
                throw new \RuntimeException("Could not copy {$item->getPathname()}");
            }
        }
    }

    /**
     * Recursively delete a directory, only inside this stage's study root.
     *
     * @param string $dir Directory to delete.
     *
     * @return bool True when deleted.
     */
    private function _deleteTree(string $dir): bool
    {
        $root = realpath($this->_studyRoot());
        $dir  = realpath($dir);

        if ($root === false || $dir === false || !str_starts_with($dir, $root . '/')) {
            return false;
        }

        $it = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST
        );
        foreach ($it as $file) {
            $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        }

        return rmdir($dir);
    }

    // =========================================================================
    //  STATE
    // =========================================================================

    private function _studyRoot(): string
    {
        return $this->targetDir . '/' . self::STUDY_SUBDIR;
    }

    private function _ensureTargetDir(): void
    {
        if ($this->dryRun) {
            return;
        }

        foreach ([$this->targetDir, $this->_studyRoot()] as $dir) {
            if (is_dir($dir)) {
                continue;
            }

            if (!@mkdir($dir, 0775, true) && !is_dir($dir)) {
                throw new \RuntimeException("Could not create {$dir}");
            }
        }
    }

    private function _readTracking(): array
    {
        $path = TrackingFile::readPath(
            $this->targetDir . '/' . self::TRACK_FILE,
            $this->targetDir . '/' . self::LEGACY_TRACK_FILE
        );

        if (!file_exists($path)) {
            return ['studies' => []];
        }

        $data = $this->_readJson($path);

        if (!isset($data['studies']) || !is_array($data['studies'])) {
            $data['studies'] = [];
        }

        return $data;
    }

    private function _writeTracking(array $tracking): void
    {
        $tracking['updated_at'] = date('c');
        $this->_atomicWriteJson($this->targetDir . '/' . self::TRACK_FILE, $tracking);
        TrackingFile::retireLegacy(
            $this->targetDir . '/' . self::TRACK_FILE,
            $this->targetDir . '/' . self::LEGACY_TRACK_FILE
        );
    }

    private function _writeManifest(array $manifest): void
    {
        $this->_atomicWriteJson(
            $this->targetDir . '/' . self::MANIFEST_FILE,
            [
                'generated_at' => date('c'),
                'study_count'  => count($manifest),
                'stats'        => $this->stats,
                'problems'     => $this->problems,
                'studies'      => $manifest,
            ]
        );
    }

    /**
     * Write the per-study provenance sidecar.
     *
     * States what was and was not done, in terms that do not depend on the
     * directory it happens to sit in.
     *
     * @param array $entry Stage 2 manifest entry.
     *
     * @return void
     */
    private function _writeProvenance(array $entry): void
    {
        if ($this->dryRun) {
            return;
        }

        $status    = $entry['link_status'] ?? 'linked';
        $rewritten = (int) ($entry['files_rewritten'] ?? 0);

        $summary = match ($status) {
            'linked'   => 'Reidentified: PatientName replaced with the LORIS '
                . 'identifier, original preserved in OtherPatientNames.',
            default    => 'NOT reidentified: no participant link could be '
                . 'resolved. Headers are unchanged from the delivery; not ingested.',
        };

        $provenance = [
            'archimedes_stage'      => 'dicom_reidentifier',
            'reidentified'          => $status === 'linked',
            'link_status'           => $status,
            'link_reason'           => $entry['link_reason'] ?? null,
            'summary'               => $summary,
            'study_instance_uid'    => $entry['study_instance_uid'] ?? null,
            'source_directory'      => $entry['source_directory'] ?? null,
            'headers_rewritten'     => $rewritten > 0,
            'files_rewritten'       => $rewritten,
            'original_patient_name' => $entry['original_patient_name'] ?? null,
            'loris_patient_name'    => $entry['loris_patient_name'] ?? null,
            'pscid'                 => $entry['pscid'] ?? null,
            'cand_id'               => $entry['cand_id'] ?? null,
            'visit_label'           => $entry['visit_label'] ?? null,
            'processed_at'          => $entry['mapped_at'] ?? date('c'),
        ];

        $path = rtrim((string) $entry['directory'], '/') . self::PROVENANCE_SUFFIX;

        try {
            $this->_atomicWriteJson($path, $provenance);
        } catch (\RuntimeException $e) {
            // Provenance is evidence, not a precondition - a study that copied
            // successfully should not fail because its sidecar could not be
            // written. Say so loudly instead.
            $this->_problem(
                'warning',
                $entry['study_instance_uid'] ?? null,
                'Could not write provenance sidecar: ' . $e->getMessage()
            );
        }
    }

    private function _readJson(string $path): array
    {
        if (!file_exists($path)) {
            throw new \RuntimeException("File not found: {$path}");
        }

        $raw = file_get_contents($path);
        if ($raw === false) {
            throw new \RuntimeException("Could not read {$path}");
        }

        $data = json_decode($raw, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \RuntimeException(
                "Invalid JSON in {$path}: " . json_last_error_msg()
            );
        }

        return $data;
    }

    private function _atomicWriteJson(string $path, array $data): void
    {
        $json = json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

        if ($json === false) {
            throw new \RuntimeException(
                'Could not encode JSON: ' . json_last_error_msg()
            );
        }

        $tmp = $path . '.tmp.' . getmypid();

        if (file_put_contents($tmp, $json) === false) {
            throw new \RuntimeException("Could not write {$tmp}");
        }

        if (!rename($tmp, $path)) {
            @unlink($tmp);
            throw new \RuntimeException("Could not rename {$tmp} to {$path}");
        }
    }

    // =========================================================================
    //  REPORTING
    // =========================================================================

    private function _reject(string $studyUid, string $message)
    {
        $this->_problem('error', $studyUid, $message);
        return null;
    }

    private function _problem(string $level, ?string $studyUid, string $message): void
    {
        $this->problems[] = [
            'level'              => $level,
            'study_instance_uid' => $studyUid,
            'message'            => $message,
        ];

        if ($this->logger === null) {
            return;
        }

        if ($level === 'error' && method_exists($this->logger, 'error')) {
            $this->logger->error($message);
        } else {
            $this->logger->warning($message);
        }
    }

    private function _warn(string $message): void
    {
        $this->_problem('warning', null, $message);
    }

    private function _info(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->info($message);
        }
    }
}