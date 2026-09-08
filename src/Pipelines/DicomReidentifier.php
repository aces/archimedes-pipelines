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
 * Phantom studies skip candidate resolution entirely and are flagged for the
 * archiver, which attaches them to the scanner candidate instead.
 *
 * PHP Version 8.1
 *
 * @category Pipeline
 * @package  Archimedes
 */

namespace LORIS\Pipelines;

/**
 * The LORIS operations this stage needs.
 *
 * Implement this as an adapter over the existing ARCHIMEDES API client rather
 * than changing that client to match.
 */
interface LorisApiClientInterface
{
    /**
     * Find a candidate by the site's own identifier.
     *
     * @param string $externalId Site identifier (the sub- value).
     *
     * @return array{pscid: string, cand_id: string}|null Null when not found.
     */
    public function findCandidateByExternalId(string $externalId): ?array;

    /**
     * Create a candidate.
     *
     * @param string      $pscid      PSCID to assign.
     * @param string      $externalId Site identifier, stored as ExtStudyID.
     * @param string|null $dob        Jittered to YYYY-MM-01 before sending.
     * @param string|null $sex        Male, Female or Other.
     *
     * @return array{pscid: string, cand_id: string}
     */
    public function createCandidate(
        string $pscid,
        string $externalId,
        ?string $dob,
        ?string $sex
    ): array;

    /**
     * Visit labels configured for the project.
     *
     * @return string[]
     */
    public function getConfiguredVisitLabels(): array;

    /**
     * Ensure a session exists for this candidate and visit.
     *
     * @param string $candId     CandID.
     * @param string $visitLabel Visit label.
     *
     * @return void
     */
    public function ensureSession(string $candId, string $visitLabel): void;
}

class DicomReidentifier
{
    /** Studies are written under <target>/dicoms/ */
    const STUDY_SUBDIR = 'dicoms';

    /** Idempotency state, keyed on StudyInstanceUID. */
    const TRACK_FILE = '.dicom_mapping_tracking.json';

    /** Handoff to the archiving stage. */
    const MANIFEST_FILE = 'dicom_studies_lorisid.json';

    /**
     * Per-study provenance sidecar, written next to each study directory as
     * <directory_name>.provenance.json.
     *
     * The directory name carries an unlinked-/phantom- prefix and the manifest
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
        'phantom_studies'     => 0,
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
     * organised successfully.
     *
     * @param bool $v Whether to create missing candidates.
     *
     * @return void
     */
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

        foreach ($stage1['studies'] as $study) {
            $studyUid = $study['study_instance_uid'] ?? null;

            if ($studyUid === null) {
                $this->_problem('error', null, 'manifest entry without study_instance_uid');
                continue;
            }

            if (isset($tracking['studies'][$studyUid])) {
                $this->_info("  Already mapped, skipping: {$studyUid}");
                $this->stats['studies_skipped']++;
                $output[] = $tracking['studies'][$studyUid];
                continue;
            }

            $entry = $this->_mapStudy($study, $participantRows);

            if ($entry === null) {
                $this->stats['studies_skipped']++;
                continue;
            }

            $this->_writeProvenance($entry);

            $output[] = $entry;
            $tracking['studies'][$studyUid] = $entry;

            if (!$this->dryRun) {
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
        $linkStatus = $study['link_status'] ?? (!empty($study['is_phantom']) ? 'phantom' : 'linked');
        $isPhantom  = ($linkStatus === 'phantom');
        $isUnlinked = ($linkStatus === 'unlinked');
        $sourceDir  = $study['directory'] ?? null;
        $modalities = $study['modalities'] ?? [];

        if ($sourceDir === null || !is_dir($sourceDir)) {
            return $this->_reject($studyUid, "stage 1 directory missing: {$sourceDir}");
        }

        $this->_info("  {$studyUid}");

        // --- Phantom or already unlinked: no candidate, no identity rewrite ---
        if ($isPhantom || $isUnlinked) {
            if ($isPhantom) {
                $this->stats['phantom_studies']++;
            } else {
                $this->stats['unlinked_studies']++;
            }

            $dirName = $isPhantom
                ? $this->_phantomDirName($subject, $session, $modalities)
                : $this->_unlinkedDirName($subject, $session, $modalities);
            $targetDir = $this->_studyRoot() . '/' . $dirName;

            $this->_info(sprintf(
                '    -> %s   [%s]',
                $dirName,
                $isPhantom ? 'phantom' : 'unlinked'
            ));

            if (!$this->dryRun) {
                $this->_copyTree($sourceDir, $targetDir);
            }

            $this->stats['studies_mapped']++;

            return $this->_entry($study, $targetDir, $dirName, [
                'link_status'         => $linkStatus,
                'link_reason'         => $study['link_reason'] ?? ($isPhantom ? 'declared' : 'unknown'),
                'is_phantom'          => $isPhantom,
                'pscid'               => null,
                'cand_id'             => null,
                'visit_label'         => null,
                'loris_patient_name'  => null,
                'original_patient_name' => $study['patient_name'] ?? null,
                'files_rewritten'     => 0,
            ]);
        }

        // --- Visit must be configured -----------------------------------------
        $visitLabel = $this->_resolveVisitLabel($session);

        if ($visitLabel === null) {
            $this->_problem('warning', $studyUid, sprintf(
                "sub-%s: visit label '%s' is not configured - archiving UNLINKED",
                $subject,
                $session ?? '?'
            ));

            $this->stats['unlinked_studies']++;
            $this->stats['studies_degraded']++;

            $dirName   = $this->_unlinkedDirName($subject, $session, $modalities);
            $targetDir = $this->_studyRoot() . '/' . $dirName;

            $this->_info("    -> {$dirName}   [unlinked: visit not configured]");

            if (!$this->dryRun) {
                $this->_copyTree($sourceDir, $targetDir);
            }

            $this->stats['studies_mapped']++;

            return $this->_entry($study, $targetDir, $dirName, [
                'link_status'           => 'unlinked',
                'link_reason'           => 'visit_not_configured',
                'is_phantom'            => false,
                'pscid'                 => null,
                'cand_id'               => null,
                'visit_label'           => null,
                'loris_patient_name'    => null,
                'original_patient_name' => $study['patient_name'] ?? null,
                'files_rewritten'       => 0,
            ]);
        }

        // --- Candidate ---------------------------------------------------------
        // external_id is set by stage 1: the sub- folder for a real subject,
        // the header value for a phantom.
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
            if (!$this->createCandidates) {
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

        // --- Session -----------------------------------------------------------
        if (!$this->dryRun) {
            try {
                $this->api->ensureSession($candId, $visitLabel);
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
            'is_phantom'            => false,
            'pscid'                 => $pscid,
            'cand_id'               => $candId,
            'visit_label'           => $visitLabel,
            'loris_patient_name'    => $lorisName,
            'original_patient_name' => $study['patient_name'] ?? null,
            'files_rewritten'       => $rewritten,
        ]);
    }

    /**
     * Match a session identifier against the configured visit labels.
     *
     * Compared case-insensitively, since site folder naming is inconsistent
     * and the configured label is authoritative for the returned value.
     *
     * @param string|null $session Session identifier from the folder name.
     *
     * @return string|null The configured label, or null when unmatched.
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

        return null;
    }

    private function _lorisDirName(string $lorisName, array $modalities): string
    {
        $suffix = empty($modalities) ? 'NA' : implode('-', $modalities);

        return $this->_unique($lorisName . '_' . $suffix);
    }

    private function _unlinkedDirName(?string $subject, ?string $session, array $modalities): string
    {
        $suffix = empty($modalities) ? 'NA' : implode('-', $modalities);

        return $this->_unique(sprintf(
            'unlinked-%s_%s_%s',
            $subject ?? 'unknown',
            $session ?? 'nosession',
            $suffix
        ));
    }

    private function _phantomDirName(?string $subject, ?string $session, array $modalities): string
    {
        $suffix = empty($modalities) ? 'NA' : implode('-', $modalities);

        return $this->_unique(sprintf(
            'phantom-%s_%s_%s',
            $subject ?? 'unknown',
            $session ?? 'nosession',
            $suffix
        ));
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
        if (!is_dir($this->_studyRoot() . '/' . $base)) {
            return $base;
        }

        $n = 2;
        while (is_dir($this->_studyRoot() . "/{$base}_{$n}")) {
            $n++;
        }

        return "{$base}_{$n}";
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
        $path = $this->targetDir . '/' . self::TRACK_FILE;

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
            'phantom'  => 'NOT reidentified: declared phantom acquisition. '
                . 'Headers are unchanged from the delivery.',
            default    => 'NOT reidentified: no participant link could be '
                . 'resolved. Headers are unchanged from the delivery and this '
                . 'study will archive with a NULL SessionID.',
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

    private function _info(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->info($message);
        }
    }
}
