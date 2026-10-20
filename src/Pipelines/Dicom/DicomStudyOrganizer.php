<?php
/**
 * DicomStudyOrganizer
 *
 * Stage 1 of the ARCHIMEDES DICOM pipeline.
 *
 * Walks a delivered upload laid out as sub-<id>/ses-<visit>/..., groups the
 * DICOMs by StudyInstanceUID, validates the delivery against the submission
 * guide, and writes an organized copy plus a manifest.
 *
 * Modality-agnostic: MR, PT, CT and anything else group identically. Modality
 * is recorded, never used to decide grouping.
 *
 * No database connection, no LORIS identifiers, no header rewriting - those
 * belong to stage 2. Each study directory produced here is a valid --source
 * for import_dicom_study.py as-is.
 *
 * PHP Version 8.1
 *
 * @category Pipeline
 * @package  Archimedes
 */

namespace LORIS\Pipelines\Dicom;

use LORIS\Pipelines\Shared\SessionsTsv;
use LORIS\Pipelines\Shared\StudyFingerprint;

class DicomStudyOrganizer
{
    /** Studies are written under <target>/dicoms/ */
    const STUDY_SUBDIR = 'dicoms';

    /** Idempotency state, keyed on StudyInstanceUID. */
    const TRACK_FILE = '.dicom_organize_tracking.json';

    /** Handoff to the next pipeline stage. */
    const MANIFEST_FILE = 'dicom_studies.json';

    /**
     * DICOM tags read from every file, as [group, element, label].
     * Order matters: it is the order passed to get_dicom_info.pl.
     */
    const TAGS = [
        ['0020', '000d', 'study_instance_uid'],
        ['0020', '000e', 'series_instance_uid'],
        ['0010', '0010', 'patient_name'],
        ['0010', '0020', 'patient_id'],
        ['0008', '0060', 'modality'],
        ['0020', '0011', 'series_number'],
        ['0008', '103e', 'series_description'],
        ['0008', '0020', 'study_date'],
    ];

    /** Files handed to get_dicom_info.pl per invocation. */
    const CHUNK_SIZE = 500;

    /**
     * How headers are read: 'dcmdump' (default), 'pydicom', or
     * 'get_dicom_info'.
     *
     * pydicom is what import_dicom_study.py uses, so the organizer and the
     * importer agree by construction. It also avoids the Perl environment
     * that the LORIS-MRI install script sets up - PERL5LIB, NeuroDB/, a
     * writable temp directory - none of which a pipeline host is guaranteed
     * to have.
     *
     * @var string
     */
    private $readerMode = 'dcmdump';

    /** @var string Absolute path to dcmdump. */
    private $dcmdumpPath = '/usr/bin/dcmdump';

    /** @var string Path to get_dicom_info.pl, when readerMode says so. */
    private $dicomInfoPath;

    /** @var string Python interpreter for the pydicom reader. */
    private $pythonBin = 'python3';

    /** @var string Path to dicom_header_dump.py. */
    private $headerDumpPath = '';

    /** @var string Absolute path to the output root. */
    private $targetDir;

    /** @var object|null Anything exposing info()/warning()/error(). */
    private $logger;

    /** @var bool */
    private $dryRun = false;

    /** @var bool */
    private $move = false;

    /** @var bool Abort on the first study-level error rather than skipping. */
    private $strict = false;

    /** @var bool Ignore the tracking file and reprocess every study. */
    private $force = false;

    /**
     * When true (the default), a study that cannot be resolved to a
     * participant and visit is carried forward as unlinked (reported, never
     * ingested) rather than rejected. The reason is in the manifest as
     * link_reason.
     *
     * @var bool
     */
    private $fallbackToUnlinked = true;

    /** @var array<string, array> participant_id => row from participants.tsv */
    private $participants = [];

    /** @var string Delivery root of the current organize() call. */
    private string $sourceRoot = '';


    /** @var array<int, array> Problems found, for the caller to report. */
    private $problems = [];

    /** Log the failing command line once, not per chunk. */
    private $_commandLogged = false;

    /** Explicit PERL5LIB, when the layout is not the standard one. */
    private $perl5Lib = null;

    /**
     * Keep the delivered directory name when a study came from exactly one
     * directory. Link status still travels in the manifest and the provenance
     * sidecar, so nothing is lost by not encoding it in the name.
     *
     * @var bool
     */
    private $preserveSourceNames = true;

    /** @var array */
    private $stats = [
        'files_scanned'   => 0,
        'files_skipped'   => 0,
        'studies_found'   => 0,
        'studies_written' => 0,
        'studies_skipped' => 0,
        'series_found'    => 0,
        'files_organized' => 0,
        'unlinked_studies' => 0,
        'studies_degraded' => 0,
        'studies_relocated' => 0,
        'studies_baselined' => 0,
        'studies_rechanged' => 0,
    ];

    /**
     * @param string      $dicomInfoPath Absolute path to get_dicom_info.pl.
     * @param string      $targetDir     Absolute path to the output root.
     * @param object|null $logger        Optional logger.
     */
    public function __construct(
        string $dicomInfoPath,
        string $targetDir,
               $logger = null
    ) {
        $this->dicomInfoPath = $dicomInfoPath;
        $this->targetDir     = rtrim($targetDir, '/');
        $this->logger        = $logger;
    }

    public function setDryRun(bool $v): void
    {
        $this->dryRun = $v;
    }

    public function setMove(bool $v): void
    {
        $this->move = $v;
    }

    /**
     * Override the PERL5LIB directory used for get_dicom_info.pl.
     *
     * Only needed when NeuroDB/ is not the grandparent of get_dicom_info.pl.
     *
     * @param string|null $path Directory containing NeuroDB/.
     *
     * @return void
     */
    public function setPerl5Lib(?string $path): void
    {
        $this->perl5Lib = $path;
    }

    /**
     * Use pydicom instead of get_dicom_info.pl to read headers.
     *
     * @param string $pythonBin      Interpreter with pydicom available.
     * @param string $headerDumpPath Path to dicom_header_dump.py.
     *
     * @return void
     */
    /**
     * Read headers with DCMTK dcmdump.
     *
     * The default. DCMTK is already required for dcmodify and for the EviData
     * extract, so this adds no dependency and keeps the repository PHP-only.
     *
     * @param string $dcmdumpPath Absolute path to dcmdump.
     *
     * @return void
     */
    public function useDcmdumpReader(string $dcmdumpPath = '/usr/bin/dcmdump'): void
    {
        $this->readerMode  = 'dcmdump';
        $this->dcmdumpPath = $dcmdumpPath;
    }

    public function usePydicomReader(string $pythonBin, string $headerDumpPath): void
    {
        $this->readerMode     = 'pydicom';
        $this->pythonBin      = $pythonBin;
        $this->headerDumpPath = $headerDumpPath;
    }

    /**
     * Use get_dicom_info.pl to read headers.
     *
     * @return void
     */
    public function useGetDicomInfoReader(): void
    {
        $this->readerMode = 'get_dicom_info';
    }

    /**
     * Whether to keep the delivered directory name for single-directory
     * studies. Off falls back to sub-/ses-/modality naming.
     *
     * @param bool $v Whether to preserve.
     *
     * @return void
     */
    public function setPreserveSourceNames(bool $v): void
    {
        $this->preserveSourceNames = $v;
    }

    public function setStrict(bool $v): void
    {
        $this->strict = $v;
    }

    /**
     * Reprocess studies already recorded in the tracking file.
     *
     * @param bool $v Whether to ignore tracking.
     *
     * @return void
     */
    public function setForce(bool $v): void
    {
        $this->force = $v;
    }

    /** @var bool Ignore participants.tsv (every study unlinked). Validator only. */
    private bool $ignoreParticipants = false;

    /**
     * Organize as if there were no participants.tsv. Used only by
     * run_dicom_validate.php when participants.tsv itself is unusable, so the
     * files can still be checked. Never set by the pipeline.
     */
    public function setIgnoreParticipants(bool $v): void
    {
        $this->ignoreParticipants = $v;
    }

    /**
     * Carry unresolvable studies forward as unlinked instead of skipping them.
     *
     * Unlinked means reported with no participant link, never ingested.
     *
     * @param bool $v Whether to degrade rather than skip.
     *
     * @return void
     */
    public function setFallbackToUnlinked(bool $v): void
    {
        $this->fallbackToUnlinked = $v;
    }

    public function getStats(): array
    {
        return $this->stats;
    }

    public function getProblems(): array
    {
        return $this->problems;
    }

    /**
     * True when any recorded problem was fatal to a study.
     *
     * @return bool
     */
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
     * Organize every DICOM under $sourceDir.
     *
     * @param string $sourceDir Root of the delivered upload.
     *
     * @return array Manifest entries, one per StudyInstanceUID.
     *
     * @throws \RuntimeException on unrecoverable error.
     */
    public function organize(string $sourceDir): array
    {
        $sourceDir = rtrim($sourceDir, '/');

        if (!is_dir($sourceDir) || !is_readable($sourceDir)) {
            throw new \RuntimeException(
                "Source not a readable directory: {$sourceDir}"
            );
        }

        // Fail here rather than after a warning per chunk.
        if ($this->readerMode === 'dcmdump') {
            if (!is_executable($this->dcmdumpPath)) {
                throw new \RuntimeException(
                    "dcmdump not executable: {$this->dcmdumpPath} "
                    . '(install the dcmtk package)'
                );
            }

            $this->_info('Header reader: dcmdump');
        } elseif ($this->readerMode === 'pydicom') {
            if (!is_readable($this->headerDumpPath)) {
                throw new \RuntimeException(
                    "dicom_header_dump.py not readable: {$this->headerDumpPath}"
                );
            }

            $probe = [];
            $code  = 0;
            exec(
                escapeshellarg($this->pythonBin)
                . ' -c ' . escapeshellarg('import pydicom') . ' 2>&1',
                $probe,
                $code
            );

            if ($code !== 0) {
                throw new \RuntimeException(sprintf(
                    'pydicom is not importable by %s: %s. Point '
                    . 'imaging.python_bin at the LORIS-MRI virtualenv '
                    . 'interpreter.',
                    $this->pythonBin,
                    implode(' ', array_slice($probe, 0, 2))
                ));
            }

            $this->_info('Header reader: pydicom via ' . $this->pythonBin);
        } else {
            if (!is_executable($this->dicomInfoPath)) {
                throw new \RuntimeException(
                    "get_dicom_info.pl not executable: {$this->dicomInfoPath}"
                );
            }

            $neuroDb = $this->perl5Lib() . '/NeuroDB';

            if (!is_dir($neuroDb)) {
                throw new \RuntimeException(
                    "NeuroDB/ not found under {$this->perl5Lib()} - "
                    . 'get_dicom_info.pl cannot load NeuroDB::MRI. Locate it '
                    . 'with `find /opt/LorisMRI -name MRI.pm -path "*NeuroDB*"` '
                    . 'and set imaging.perl5lib to the directory that CONTAINS '
                    . 'NeuroDB/.'
                );
            }

            $this->_info('Header reader: get_dicom_info.pl, PERL5LIB='
                . $this->perl5Lib());
        }

        $this->_ensureTargetDir();

        $this->participants = $this->ignoreParticipants ? [] : $this->_loadParticipants($sourceDir);
        $this->sourceRoot     = rtrim($sourceDir, '/');
        $this->_info(sprintf(
            'Loaded %d participant row(s) from participants.tsv',
            count($this->participants)
        ));

        $candidates = $this->_findDicomCandidates($sourceDir);
        if (empty($candidates)) {
            throw new \RuntimeException("No candidate files found under {$sourceDir}");
        }

        $this->stats['files_scanned'] = count($candidates);
        $this->_info(sprintf('Scanning %d file(s)', count($candidates)));

        $headers = $this->_readHeaders($candidates);
        if (empty($headers)) {
            throw new \RuntimeException('No readable DICOM files found');
        }

        $headers = $this->_checkInstances($candidates, $headers);

        $this->_checkEmptyVisitFolders($sourceDir, $headers);

        $studies = $this->_groupByStudy($headers);
        $this->stats['studies_found'] = count($studies);
        $this->_info(sprintf('Found %d DICOM study/studies', count($studies)));
        $this->_info('');

        $tracking = $this->_readTracking();
        $manifest = [];
        $usedDirs = [];

        // A study keeps the folder it was first organized into. Reserve those
        // names up front so a new study can never take one, and so re-organizing
        // (or --force) rewrites in place instead of creating MR_2, MR_3, ...
        foreach ($tracking['studies'] ?? [] as $trackedStudy) {
            if (!empty($trackedStudy['directory_name'])) {
                $usedDirs[$trackedStudy['directory_name']] = true;
            }
        }

        foreach ($studies as $studyUid => $seriesMap) {
            // Change detection. The old behaviour - "seen this StudyInstanceUID
            // before, skip forever" - silently ignored studies that grew new
            // series or had files corrected between deposits. Now the cheap
            // manifest hash is checked every run and the bytes are only read
            // when it differs.
            $tracked = $tracking['studies'][$studyUid] ?? null;

            $studyFiles = [];
            foreach ($seriesMap as $rows) {
                foreach ($rows as $row) {
                    $studyFiles[] = $row['path'];
                }
            }

            // A study that was removed from an earlier delivery and is back
            // has no processed/ folder any more: organize it again. Its hashes
            // are unchanged, so the later steps recognise it and nothing is
            // re-imported.
            $returned = $tracked !== null && !empty($tracked['removed_at']);
            if ($returned) {
                $this->_info("  {$studyUid}: back in the delivery (removed {$tracked['removed_at']})");
                $this->stats['studies_returned'] = ($this->stats['studies_returned'] ?? 0) + 1;
            }

            $comparison = StudyFingerprint::compare(
                ($this->force || $returned) ? null : $tracked,
                $studyFiles,
                $sourceDir
            );

            if ($tracked !== null && !$comparison['reprocess']) {
                $this->_info(sprintf(
                    '  Skipping %s: %s',
                    $studyUid,
                    $comparison['reason']
                ));

                $this->stats['studies_skipped']++;

                if ($comparison['state'] === StudyFingerprint::RELOCATED) {
                    $this->stats['studies_relocated']++;
                } elseif ($comparison['state'] === StudyFingerprint::BASELINE) {
                    $this->stats['studies_baselined']++;
                }

                // The files are unchanged, but participants.tsv may not be: a
                // subject added to it since the last run turns an unlinked study
                // into a linked one. Link status is therefore re-evaluated every
                // run; only the copy is skipped.
                $current = $this->_buildEntry(
                    $studyUid,
                    $seriesMap,
                    $usedDirs,
                    $tracked['directory_name'] ?? null
                );

                $entry = array_merge(
                    $tracked,
                    StudyFingerprint::toTracking($comparison, $studyFiles, $sourceDir)
                );

                if ($current !== null) {
                    $entry = $this->_refreshLink($studyUid, $entry, $current);

                    $old = $tracked['directory_name'] ?? null;
                    if ($old !== null && $old !== $current['directory_name']) {
                        $this->_info("    renamed {$old} -> {$current['directory_name']}");
                        if (!$this->dryRun && is_dir($this->_studyRoot() . '/' . $old)) {
                            rename(
                                $this->_studyRoot() . '/' . $old,
                                $current['directory']
                            );
                        }
                        $entry['directory_name'] = $current['directory_name'];
                        $entry['directory']      = $current['directory'];
                    }
                }

                // Refresh the stored hashes even when skipping, so a renamed
                // copy does not force a byte read on every future run.
                $tracking['studies'][$studyUid] = $entry;

                if (!$this->dryRun) {
                    $this->_writeTracking($tracking);
                }

                $manifest[] = $entry;
                continue;
            }

            if ($tracked !== null) {
                $this->stats['studies_rechanged']++;
                $this->_warn(sprintf(
                    '%s: %s - re-organizing',
                    $studyUid,
                    $comparison['reason']
                ));
            }

            $entry = $this->_buildEntry(
                $studyUid,
                $seriesMap,
                $usedDirs,
                $tracked['directory_name'] ?? null
            );

            if ($entry === null) {
                $this->stats['studies_skipped']++;
                continue;
            }

            if ($tracked !== null && !$this->dryRun) {
                // Rewriting the same folder: clear the previous copy so changed
                // or removed files do not linger, and unchanged ones are not
                // duplicated as name_1.dcm.
                $this->_removeStudyDir($entry['directory_name']);
                if (($tracked['directory_name'] ?? '') !== ''
                    && $tracked['directory_name'] !== $entry['directory_name']
                ) {
                    $this->_removeStudyDir($tracked['directory_name']);
                }
            }

            $this->_writeStudy($entry, $seriesMap);

            // Hashes are stored only after a successful write, so a stored
            // fingerprint always means "this study was organized".
            $entry = array_merge(
                $entry,
                StudyFingerprint::toTracking($comparison, $studyFiles, $sourceDir)
            );

            $manifest[] = $entry;
            $tracking['studies'][$studyUid] = $entry;

            if (!$this->dryRun) {
                $this->_writeTracking($tracking);
            }
        }

        $this->_handleRemovedStudies($tracking, $studies);

        // The manifest is written even in a dry run, marked as such, so the
        // downstream steps have something to read and the whole chain can be
        // previewed. Study files and tracking state are still not written.
        $this->_writeManifest($manifest);

        return $manifest;
    }

    // =========================================================================
    //  DISCOVERY
    // =========================================================================

    /**
     * Recursively collect candidate files, capturing sub-/ses- path context.
     *
     * No extension filter: scanner exports routinely ship DICOMs with no
     * extension. get_dicom_info.pl decides what is really a DICOM.
     *
     * @param string $dir Upload root.
     *
     * @return array<string, array{subject: ?string, session: ?string}>
     *         Keyed by absolute path.
     */
    private function _findDicomCandidates(string $dir): array
    {
        $found    = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::LEAVES_ONLY
        );

        foreach ($iterator as $fileInfo) {
            if (!$fileInfo->isFile() || !$fileInfo->isReadable()) {
                continue;
            }

            $name = $fileInfo->getFilename();

            // DICOMDIR is an index, not an image; it confuses the archiver.
            if (strtoupper($name) === 'DICOMDIR') {
                continue;
            }

            // Our own sidecars.
            if (substr($name, -4) === '.tsv' || substr($name, -5) === '.json') {
                continue;
            }

            $path         = $fileInfo->getPathname();
            $found[$path] = $this->_pathContext($dir, $path);
        }

        ksort($found);

        return $found;
    }

    /**
     * Extract subject and session from the sub-/ses- path segments.
     *
     * Per the submission guide only the top two levels are fixed, so every
     * segment is scanned rather than assuming a depth.
     *
     * @param string $root Upload root.
     * @param string $path Absolute file path.
     *
     * @return array{subject: ?string, session: ?string}
     */
    private function _pathContext(string $root, string $path): array
    {
        $relative = ltrim(substr($path, strlen($root)), '/');
        $segments = explode('/', $relative);

        $subject = null;
        $session = null;

        foreach ($segments as $segment) {
            if ($subject === null && strpos($segment, 'sub-') === 0) {
                $subject = substr($segment, 4);
            }
            if ($session === null && strpos($segment, 'ses-') === 0) {
                $session = substr($segment, 4);
            }
        }

        return [
            'subject' => $subject,
            'session' => $session,
            // The directory the file was delivered in. When every file of a
            // study shares one, that name is the site's own identifier for
            // the study and is worth preserving rather than inventing one.
            'srcdir'  => count($segments) > 1
                ? implode('/', array_slice($segments, 0, -1))
                : '',
        ];
    }

    // =========================================================================
    //  HEADER EXTRACTION  (the only DICOM-aware part)
    // =========================================================================

    /**
     * Read self::TAGS from every candidate via get_dicom_info.pl.
     *
     * @param array $candidates Path => path context.
     *
     * @return array<int, array<string, string>> One row per readable DICOM.
     */
    private function _readHeaders(array $candidates): array
    {
        if ($this->readerMode === 'dcmdump') {
            return $this->_readHeadersDcmdump($candidates);
        }

        return $this->_readHeadersBatched($candidates);
    }

    /**
     * Read headers with dcmdump, one invocation per file.
     *
     * dcmdump takes a single file, so this is one process per DICOM rather
     * than one per 500. Slower than a batching reader - roughly a minute per
     * 6,000 files - but it keeps the repository PHP-only and uses a package
     * already required for dcmodify and the EviData extract.
     *
     * Output is tag-filtered with +P so only the wanted attributes are printed.
     *
     * @param array $candidates Path => path context.
     *
     * @return array<int, array<string, string>>
     */
    private function _readHeadersDcmdump(array $candidates): array
    {
        $filter = '';

        foreach (self::TAGS as [$group, $element, $_label]) {
            $filter .= ' +P ' . escapeshellarg(
                    strtolower($group) . ',' . strtolower($element)
                );
        }

        // SOPInstanceUID too (this reader only), to catch the same instance
        // delivered twice - see _checkInstances().
        $filter .= ' +P ' . escapeshellarg('0008,0018');

        $rows      = [];
        $scanned   = 0;
        $unreadable = 0;

        foreach ($candidates as $path => $context) {
            $scanned++;

            if (($scanned % 1000) === 0) {
                $this->_info(sprintf('    read %d file(s)', $scanned));
            }

            $cmd = escapeshellarg($this->dcmdumpPath)
                . ' --quiet --print-short' . $filter . ' '
                . escapeshellarg($path) . ' 2>/dev/null';

            $output   = [];
            $exitCode = 0;
            exec($cmd, $output, $exitCode);

            if ($exitCode !== 0) {
                // Not a DICOM, or unreadable. Expected in a mixed delivery.
                $unreadable++;
                continue;
            }

            $row = $this->_parseDcmdump($output, $path);

            if ($row === null) {
                $unreadable++;
                continue;
            }

            $row['subject'] = $context['subject'];
            $row['session'] = $context['session'];
            $row['srcdir']  = $context['srcdir'];

            foreach ($output as $line) {
                if (preg_match('/^\s*\(0008,0018\)\s+UI\s+\[([^\]]*)\]/i', $line, $sm)) {
                    $row['sop_instance_uid'] = trim($sm[1]);
                }
            }

            $rows[] = $row;
        }

        $this->stats['files_skipped'] = $unreadable;

        return $rows;
    }

    /**
     * Map a dcmdump into the same row shape the other readers produce.
     *
     * Lines look like:
     *   (0008,0080) LO [Some Hospital]   #  14, 1 InstitutionName
     *
     * @param string[] $output dcmdump lines.
     * @param string   $path   The file they came from.
     *
     * @return array<string, string>|null Null when unusable.
     */
    private function _parseDcmdump(array $output, string $path): ?array
    {
        $byTag = [];

        foreach ($output as $line) {
            if (!preg_match(
                '/^\s*\(([0-9a-fA-F]{4}),([0-9a-fA-F]{4})\)\s+\w\w\s+(.*)$/',
                $line,
                $m
            )) {
                continue;
            }

            $value = '';

            if (preg_match('/\[([^\]]*)\]/', $m[3], $vm)) {
                // A tab or newline inside a free-text value would corrupt any
                // downstream splitting; normalise here, at the source.
                $value = trim(str_replace(
                    ["\t", "\r", "\n"],
                    ' ',
                    $vm[1]
                ));
            }

            $byTag[strtolower($m[1]) . ',' . strtolower($m[2])] = $value;
        }

        $row = ['path' => $path];

        foreach (self::TAGS as [$group, $element, $label]) {
            $row[$label] = $byTag[strtolower($group) . ',' . strtolower($element)] ?? '';
        }

        if ($row['study_instance_uid'] === '' || $row['series_instance_uid'] === '') {
            return null;
        }

        return $row;
    }

    private function _readHeadersBatched(array $candidates): array
    {
        // Both readers emit the same contract: one tab-separated row per
        // readable file, path first, then the tags in this order.
        if ($this->readerMode === 'pydicom') {
            $tagArgs = '';
            foreach (self::TAGS as [$group, $element, $_label]) {
                $tagArgs .= ' ' . escapeshellarg(
                        strtolower($group) . ',' . strtolower($element)
                    );
            }

            $readerCmd = escapeshellarg($this->pythonBin) . ' '
                . escapeshellarg($this->headerDumpPath) . $tagArgs;
        } else {
            $attArgs = '';
            foreach (self::TAGS as [$group, $element, $_label]) {
                $attArgs .= ' -attvalue ' . escapeshellarg($group)
                    . ' ' . escapeshellarg($element);
            }

            $readerCmd = 'PERL5LIB=' . escapeshellarg($this->perl5Lib()) . ' '
                . escapeshellarg($this->dicomInfoPath) . ' -stdin' . $attArgs;
        }

        $paths = array_keys($candidates);
        $rows  = [];

        foreach (array_chunk($paths, self::CHUNK_SIZE) as $chunk) {
            $listFile = tempnam(sys_get_temp_dir(), 'archi_dcm_');
            if ($listFile === false) {
                throw new \RuntimeException('Could not create temp file list');
            }

            if (file_put_contents($listFile, implode("\n", $chunk) . "\n") === false) {
                @unlink($listFile);
                throw new \RuntimeException('Could not write temp file list');
            }

            // stderr is captured, not discarded: a usage error from
            // get_dicom_info.pl prints there, and swallowing it turns a
            // one-line fix into a guessing game.
            $errFile = tempnam(sys_get_temp_dir(), 'archi_err_');

            $cmd = $readerCmd
                . ' < ' . escapeshellarg($listFile)
                . ' 2> ' . escapeshellarg((string) $errFile);

            $output   = [];
            $exitCode = 0;
            exec($cmd, $output, $exitCode);

            @unlink($listFile);

            if ($exitCode !== 0) {
                $stderr = $errFile !== false
                    ? trim((string) @file_get_contents($errFile))
                    : '';

                $this->_warn(sprintf(
                    'header reader exited %d on a chunk of %d file(s)%s',
                    $exitCode,
                    count($chunk),
                    $stderr !== ''
                        ? ': ' . substr(str_replace("\n", ' | ', $stderr), 0, 500)
                        : ' (no stderr output)'
                ));

                // The command line is the thing being got wrong, so show it
                // once rather than leaving it to be reconstructed.
                if (!$this->_commandLogged) {
                    $this->_commandLogged = true;
                    $this->_warn('Command was: ' . $readerCmd . ' < <file list>');
                }
            }

            if ($errFile !== false) {
                @unlink($errFile);
            }

            foreach ($output as $line) {
                $row = $this->_parseInfoLine($line);

                if ($row === null) {
                    continue;
                }

                $context = $candidates[$row['path']] ?? null;
                if ($context === null) {
                    // A path we did not submit; ignore rather than guess.
                    continue;
                }

                $row['subject'] = $context['subject'];
                $row['session'] = $context['session'];
                $row['srcdir']  = $context['srcdir'];

                $rows[] = $row;
            }
        }

        $this->stats['files_skipped'] = $this->stats['files_scanned'] - count($rows);

        return $rows;
    }

    /**
     * Directory to put on PERL5LIB for get_dicom_info.pl.
     *
     * The directory holding NeuroDB/. Usually the LORIS-MRI root, but some
     * installs put it under uploadNeuroDB/, so both are probed. Overridable
     * via imaging.perl5lib.
     *
     * @return string
     */
    private function perl5Lib(): string
    {
        if ($this->perl5Lib !== null) {
            return $this->perl5Lib;
        }

        // NeuroDB/ is not always beside dicom-archive/. On some installs it
        // lives under uploadNeuroDB/, so look in the plausible places rather
        // than assuming one layout and failing with a module error that says
        // nothing about paths.
        $mriRoot = dirname(dirname($this->dicomInfoPath));

        $candidates = [
            $mriRoot,
            $mriRoot . '/uploadNeuroDB',
            dirname($this->dicomInfoPath),
        ];

        foreach ($candidates as $candidate) {
            if (is_dir($candidate . '/NeuroDB')) {
                $this->perl5Lib = $candidate;
                return $this->perl5Lib;
            }
        }

        // Nothing found. Return the conventional root so the pre-flight check
        // can report a specific path rather than an empty string.
        $this->perl5Lib = $mriRoot;

        return $this->perl5Lib;
    }

    /**
     * Parse one tab-separated get_dicom_info.pl output line.
     *
     * Expected columns: filename, then the requested tags in the order passed.
     *
     * @param string $line Raw output line.
     *
     * @return array<string, string>|null Null when malformed or unusable.
     */
    private function _parseInfoLine(string $line): ?array
    {
        // Split on a SINGLE tab. A greedy /\t+/ collapses consecutive tabs,
        // so two adjacent empty values would silently shift every later
        // column by one - which looks like corrupt headers, not a parse bug.
        $parts = explode("\t", rtrim($line, "\r\n"));

        if (count($parts) < count(self::TAGS) + 1) {
            return null;
        }

        $row = ['path' => array_shift($parts)];

        foreach (self::TAGS as $i => [$_g, $_e, $label]) {
            $value = trim($parts[$i] ?? '');
            // get_dicom_info.pl emits a literal dash for absent values.
            $row[$label] = ($value === '-') ? '' : $value;
        }

        if ($row['study_instance_uid'] === '' || $row['series_instance_uid'] === '') {
            return null;
        }

        return $row;
    }

    // =========================================================================
    //  GROUPING
    // =========================================================================

    /**
     * Group rows by StudyInstanceUID, then SeriesInstanceUID.
     *
     * @param array $headers Rows from _readHeaders().
     *
     * @return array<string, array<string, array>>
     */
    private function _groupByStudy(array $headers): array
    {
        $studies = [];

        foreach ($headers as $row) {
            $studies[$row['study_instance_uid']][$row['series_instance_uid']][] = $row;
        }

        foreach ($studies as &$seriesMap) {
            $this->stats['series_found'] += count($seriesMap);

            uasort(
                $seriesMap,
                function (array $a, array $b) {
                    return intval($a[0]['series_number'] ?? 0)
                        <=> intval($b[0]['series_number'] ?? 0);
                }
            );
        }
        unset($seriesMap);

        ksort($studies);

        return $studies;
    }

    // =========================================================================
    //  VALIDATION
    // =========================================================================

    /**
     * Build the manifest entry for one study, running all delivery checks.
     *
     * @param string $studyUid  StudyInstanceUID.
     * @param array  $seriesMap SeriesInstanceUID => rows.
     * @param array  $usedDirs  Directory names already claimed, by reference.
     *
     * @return array|null Null when the study fails a check and is skipped.
     */
    private function _buildEntry(
        string $studyUid,
        array $seriesMap,
        array &$usedDirs,
        ?string $fixedDirName = null
    ): ?array {
        $allRows = [];
        foreach ($seriesMap as $rows) {
            foreach ($rows as $row) {
                $allRows[] = $row;
            }
        }

        $sample  = $allRows[0];
        $subject = $sample['subject'];
        $session = $sample['session'];

        // One examination belongs to one participant and one visit. Files of
        // the same StudyInstanceUID under several sub-/ses- folders mean a
        // copy or a misfiling; taking the first file's folder would ingest it
        // under a participant chosen by sort order. Not organized - the error
        // stops the project before anything is ingested.
        $places = [];
        foreach ($allRows as $row) {
            $places[sprintf('sub-%s/ses-%s', $row['subject'] ?? '?', $row['session'] ?? '?')] = true;
        }
        if (count($places) > 1) {
            $this->_problem('error', $studyUid, sprintf(
                'StudyInstanceUID %s is in more than one participant/visit folder (%s) - '
                . 'one examination must be in one place; not organized. Remove the extra copy.',
                $studyUid,
                implode(', ', array_keys($places))
            ));
            return null;
        }

        // Two states:
        //   linked   - resolvable to a participant and visit
        //   unlinked - no matching participant/visit: reported, never ingested
        $linkStatus = 'linked';
        $linkReason = null;

        $where = sprintf(
            'sub-%s/ses-%s',
            $subject ?? '?',
            $session ?? '?'
        );

        // --- Subject derivable from the path ---------------------------------
        if ($subject === null) {
            $degraded = $this->_degrade(
                $studyUid,
                'upload is not organized into sub- folders'
            );
            if ($degraded === null) {
                return null;
            }
            $linkStatus = 'unlinked';
            $linkReason = 'not_organized';
        }

        // --- Session derivable from the path ----------------------------------
        if ($session === null && $linkStatus === 'linked') {
            $degraded = $this->_degrade(
                $studyUid,
                "sub-{$subject}: no ses- folder; visit cannot be determined"
            );
            if ($degraded === null) {
                return null;
            }
            $linkStatus = 'unlinked';
            $linkReason = 'no_session_folder';
        }

        // --- participants.tsv -------------------------------------------------
        if ($linkStatus === 'linked') {
            if (empty($this->participants)) {
                $degraded = $this->_degrade(
                    $studyUid,
                    "{$where}: no participants.tsv in the upload"
                );
                if ($degraded === null) {
                    return null;
                }
                $linkStatus = 'unlinked';
                $linkReason = 'no_participants_tsv';
            } elseif (!isset($this->participants['sub-' . $subject])) {
                $degraded = $this->_degrade(
                    $studyUid,
                    "{$where}: no row in participants.tsv for sub-{$subject}"
                );
                if ($degraded === null) {
                    return null;
                }
                $linkStatus = 'unlinked';
                $linkReason = 'not_in_participants_tsv';
            }
        }

        // --- sub-<ID>_sessions.tsv ---------------------------------------------
        // The guide asks for one row per visit. A ses- folder the site has not
        // listed is not ingested until it is: like a participant folder without
        // its participants.tsv row, it is reported and picked up on the run
        // after the row is added. (Shared rule: Shared\SessionsTsv.)
        if ($linkStatus === 'linked' && !$this->ignoreParticipants) {
            $listed = SessionsTsv::listed($this->sourceRoot, (string) $subject);
            $file   = "sub-{$subject}/" . basename(SessionsTsv::path($this->sourceRoot, (string) $subject));

            if ($listed === null) {
                $degraded = $this->_degrade(
                    $studyUid,
                    "{$where}: no sessions file (sessions.tsv or sub-{$subject}_sessions.tsv) in sub-{$subject}/"
                );
                if ($degraded === null) {
                    return null;
                }
                $linkStatus = 'unlinked';
                $linkReason = 'no_sessions_tsv';
            } elseif (!in_array((string) $session, $listed, true)) {
                $degraded = $this->_degrade(
                    $studyUid,
                    "{$where}: ses-{$session} is not listed in {$file}"
                );
                if ($degraded === null) {
                    return null;
                }
                $linkStatus = 'unlinked';
                $linkReason = 'not_in_sessions_tsv';
            }
        }

        // --- PatientName: informational only ------------------------------------
        // Never affects link status on its own; a mismatch is an error below.
        $names = [];
        foreach ($allRows as $row) {
            if ($row['patient_name'] !== '') {
                $names[$row['patient_name']] = true;
            }
        }

        $patientName = empty($names) ? '' : (string) array_key_first($names);

        // PatientName is how the guide checks that files belong to the
        // participant whose folder they are in. For a participant study a
        // mismatch means misfiled or mixed data: flagged as an error, the study
        // is not organized and the project stops before anything is ingested.
        // Compared ignoring case and DICOM PN component separators (^).
        $norm = static fn (string $v): string => strtolower(trim(rtrim(trim($v), '^')));

        if (count($names) > 1 && $linkStatus === 'linked'
            && count(array_unique(array_map($norm, array_keys($names)))) > 1
        ) {
            $this->_problem('error', $studyUid, sprintf(
                '%s: PATIENTNAME MISMATCH - more than one PatientName within a single study (%s); '
                . 'not organized, nothing is ingested until fixed',
                $where,
                implode(', ', array_keys($names))
            ));
            return null;
        } elseif (count($names) > 1 && $linkStatus !== 'linked') {
            $this->_problem('warning', $studyUid, sprintf(
                '%s: more than one PatientName within a single study (%s)',
                $where,
                implode(', ', array_keys($names))
            ));
        } elseif (empty($names) && $linkStatus === 'linked') {
            $this->_problem(
                'warning',
                $studyUid,
                "{$where}: PatientName is empty; using the sub- folder as external ID (cannot be checked)"
            );
        } elseif ($linkStatus === 'linked' && $subject !== null && $norm($patientName) !== $norm($subject)) {
            $this->_problem('error', $studyUid, sprintf(
                '%s: PATIENTNAME MISMATCH - PatientName "%s" does not match the folder sub-%s; '
                . 'misfiled or mixed data? Not organized, nothing is ingested until fixed',
                $where,
                $patientName,
                $subject
            ));
            return null;
        }

        // For a linked subject the folder wins; otherwise keep the header value.
        $externalId = $linkStatus === 'linked'
            ? ($subject ?? $patientName)
            : ($patientName !== '' ? $patientName : ($subject ?? ''));

        // --- Build the entry ---------------------------------------------------
        $modalities = [];
        foreach ($allRows as $row) {
            if ($row['modality'] !== '') {
                $modalities[$row['modality']] = true;
            }
        }
        $modalities = array_keys($modalities);
        sort($modalities);

        // If every file of this study was delivered in one directory, that
        // directory is the site's own name for the study - keep it. Renaming
        // TST02_ROM_00000001_02_SE01_MR to unlinked-20190311_1730_MR loses
        // the identifier the site will refer to when something is queried,
        // and breaks the correspondence between what arrived and what we
        // produced.
        $sourceDirs = [];

        foreach ($allRows as $row) {
            if (($row['srcdir'] ?? '') !== '') {
                $sourceDirs[$row['srcdir']] = true;
            }
        }

        $preservedName = null;

        // Only outside a sub-/ses- layout: inside one, the leaf folder is the
        // site's grouping (MR/, PET/, ...), not its name for the study, and
        // keeping it produces MR, MR_2, MR_3 across subjects and visits.
        if ($this->preserveSourceNames && count($sourceDirs) === 1 && $subject === null) {
            $only = (string) array_key_first($sourceDirs);
            $base = basename($only);

            if ($base !== '' && $base !== '.') {
                $preservedName = $base;
            }
        }

        // Keep the tracked folder name across runs, unless the link status it
        // spells out changed: sub-ARCHI0003 added to participants.tsv must not
        // stay in unlinked-sub-ARCHI0003_ses-V01_MR.
        if ($fixedDirName !== null && $subject !== null
            && $this->_statusPrefix($fixedDirName) !== ($linkStatus === 'linked' ? '' : $linkStatus . '-')
        ) {
            $fixedDirName = null;
        }

        $dirName = $fixedDirName !== null && $fixedDirName !== ''
            ? $fixedDirName
            : ($preservedName !== null
            ? $this->_uniqueName($this->_slugPart($preservedName), $usedDirs)
            : $this->_studyDirName(
                $subject,
                $session,
                $modalities,
                $usedDirs,
                $linkStatus,
                $sample['study_date'],
                $studyUid
            ));
        $usedDirs[$dirName] = true;

        if ($linkStatus === 'unlinked') {
            $this->stats['unlinked_studies']++;
        }

        $seriesEntries = [];
        foreach ($seriesMap as $seriesUid => $rows) {
            $s               = $rows[0];
            $seriesEntries[] = [
                'series_instance_uid' => $seriesUid,
                'series_number'       => intval($s['series_number'] ?? 0),
                'modality'            => $s['modality'],
                'description'         => $s['series_description'],
                'directory'           => $this->_seriesDirName($s),
                'file_count'          => count($rows),
            ];
        }

        return [
            'study_instance_uid' => $studyUid,
            'directory'          => $this->_studyRoot() . '/' . $dirName,
            'directory_name'     => $dirName,
            'subject'            => $subject,
            'session'            => $session,
            'patient_name'       => $patientName,
            'external_id'        => $externalId,
            'patient_id'         => $sample['patient_id'],
            'study_date'         => $sample['study_date'],
            'modalities'         => $modalities,
            'link_status'        => $linkStatus,
            'link_reason'        => $linkReason,
            'series_count'       => count($seriesEntries),
            'file_count'         => count($allRows),
            'organized_at'       => date('c'),
            'series'             => $seriesEntries,
        ];
    }

    /**
     * Apply a freshly computed link status to a tracked (unchanged) study.
     *
     * @param string $studyUid StudyInstanceUID.
     * @param array  $tracked  Stored entry.
     * @param array  $current  Entry rebuilt from the current participants.tsv.
     *
     * @return array Entry to store and hand downstream.
     */
    private function _refreshLink(string $studyUid, array $tracked, array $current): array
    {
        $fields = ['link_status', 'link_reason', 'external_id',
                   'subject', 'session', 'patient_name'];

        $before = $tracked['link_status'] ?? '?';
        $entry  = array_merge($tracked, array_intersect_key($current, array_flip($fields)));

        if ($before !== $entry['link_status']
            || ($tracked['external_id'] ?? null) !== ($entry['external_id'] ?? null)
        ) {
            $entry['relinked_at'] = date('c');
            $this->stats['studies_relinked'] = ($this->stats['studies_relinked'] ?? 0) + 1;
            $this->_info(sprintf(
                '  %s: link updated %s -> %s%s',
                $studyUid,
                $before,
                $entry['link_status'],
                $entry['link_reason'] ? " ({$entry['link_reason']})" : ''
            ));
        }

        return $entry;
    }

    /**
     * Delete one organized study folder (processed/ only).
     *
     * @param string $dirName Study directory name.
     *
     * @return void
     */
    /**
     * Studies organized by an earlier run that are no longer in the delivery.
     *
     * The delivery is the source of truth for the working copies: the
     * processed/ folder is removed so later steps cannot import it again.
     * The tracking entry is KEPT (marked removed_at) so a re-deposit of the
     * same study is recognised instead of treated as new. LORIS is never
     * touched here - the import step reports what LORIS still holds.
     *
     * @param array $tracking Tracking state, updated in place.
     * @param array $studies  Studies found in this delivery, keyed by UID.
     */
    private function _handleRemovedStudies(array &$tracking, array $studies): void
    {
        foreach ($tracking['studies'] ?? [] as $uid => $tracked) {
            if (isset($studies[$uid]) || !empty($tracked['removed_at'])) {
                continue;
            }

            $dir = $tracked['directory_name'] ?? '?';
            $this->stats['studies_removed'] = ($this->stats['studies_removed'] ?? 0) + 1;
            $this->_problem('warning', $uid, sprintf(
                'REMOVED from delivery: %s (%s) - %s; LORIS is not changed',
                $uid,
                $dir,
                $this->dryRun ? 'would remove processed copy' : 'processed copy removed'
            ));

            if ($this->dryRun) {
                continue;
            }

            if ($dir !== '?') {
                $this->_removeStudyDir($dir);
            }
            $tracking['studies'][$uid]['removed_at'] = date('c');
        }

        if (!$this->dryRun) {
            $this->_writeTracking($tracking);
        }
    }

    /**
     * Files that must not be archived silently.
     *
     *   - A file that looks like DICOM (DICM marker, or a .dcm/.ima name) but
     *     could not be read is truncated or corrupt. Archiving the study without
     *     it would lose a slice without anyone knowing: error.
     *   - The same SOPInstanceUID twice: a byte-identical copy (a folder copied
     *     twice) is dropped with a warning; different bytes under one instance
     *     UID is a conflict: error.
     *
     * Errors stop the project before anything is ingested.
     *
     * @param array $candidates Path => context, every file considered.
     * @param array $headers    Readable rows.
     *
     * @return array Rows with byte-identical duplicate instances removed.
     */
    private function _checkInstances(array $candidates, array $headers): array
    {
        $read = [];
        foreach ($headers as $row) {
            $read[$row['path']] = true;
        }

        // Grouped by the sub-/ses- folder they are in, so the studies of that
        // folder (only) can be excluded if the operator continues without them.
        $corrupt = [];
        foreach ($candidates as $path => $context) {
            if (isset($read[$path])) {
                continue;
            }
            $ext  = strtolower(pathinfo($path, PATHINFO_EXTENSION));
            $head = @file_get_contents($path, false, null, 128, 4);
            if ($head === 'DICM' || in_array($ext, ['dcm', 'ima', 'dicom'], true)) {
                $key = ($context['subject'] ?? '') . '|' . ($context['session'] ?? '');
                $corrupt[$key]['subject'] = $context['subject'] ?? null;
                $corrupt[$key]['session'] = $context['session'] ?? null;
                $corrupt[$key]['files'][] = $path;
            }
        }

        foreach ($corrupt as $place) {
            $where = $place['subject'] !== null
                ? sprintf('sub-%s/ses-%s', $place['subject'], $place['session'] ?? '?')
                : 'outside sub-/ses- folders';
            $this->_problem('error', null, sprintf(
                '%s: %d file(s) look like DICOM but could not be read (truncated, empty or corrupt) - '
                . 'fix or remove them: %s%s',
                $where,
                count($place['files']),
                implode(', ', array_map('basename', array_slice($place['files'], 0, 5))),
                count($place['files']) > 5 ? ' ...' : ''
            ), $place['subject'] !== null
                ? ['place_subject' => $place['subject'], 'place_session' => $place['session']]
                : []);
        }

        $seen  = [];
        $place = [];
        $kept  = [];
        foreach ($headers as $row) {
            $sop = $row['sop_instance_uid'] ?? '';
            if ($sop === '') {
                $kept[] = $row;
                continue;
            }
            if (!isset($seen[$sop])) {
                $seen[$sop]  = $row['path'];
                $place[$sop] = ($row['subject'] ?? '') . '/' . ($row['session'] ?? '');
                $kept[]      = $row;
                continue;
            }
            // A copy under ANOTHER participant or visit is not a harmless
            // duplicate: which one is right is unknown. Keep both rows so the
            // one-study-one-place check rejects it with both locations.
            $samePlace = $place[$sop] === (($row['subject'] ?? '') . '/' . ($row['session'] ?? ''));
            if ($samePlace && @hash_file('sha256', $seen[$sop]) === @hash_file('sha256', $row['path'])) {
                $this->_problem('warning', $row['study_instance_uid'] ?? null, sprintf(
                    'duplicate copy of instance %s ignored: %s (same as %s)',
                    $sop,
                    $row['path'],
                    $seen[$sop]
                ));
                $this->stats['duplicate_files'] = ($this->stats['duplicate_files'] ?? 0) + 1;
                continue;
            }
            if (!$samePlace) {
                $kept[] = $row;
                continue;
            }
            $this->_problem('error', $row['study_instance_uid'] ?? null, sprintf(
                'SOPInstanceUID %s is used by two different files (%s, %s) - conflicting '
                . 'instances; fix the delivery',
                $sop,
                $seen[$sop],
                $row['path']
            ));
            $kept[] = $row;
        }

        return $kept;
    }

    /**
     * Link-status prefix a study folder name carries ('unlinked-' or '').
     */
    private function _statusPrefix(string $dirName): string
    {
        foreach (['unlinked-'] as $prefix) {
            if (str_starts_with($dirName, $prefix)) {
                return $prefix;
            }
        }

        return '';
    }

    private function _removeStudyDir(string $dirName): void
    {
        $root = realpath($this->_studyRoot());
        $dir  = realpath($this->_studyRoot() . '/' . $dirName);

        if ($root === false || $dir === false || !str_starts_with($dir, $root . '/')) {
            return;
        }

        $it = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST
        );

        foreach ($it as $file) {
            $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        }

        rmdir($dir);
    }

    /**
     * Warn about visit folders that contain no readable DICOM.
     *
     * @param string $sourceDir Upload root.
     * @param array  $headers   Parsed header rows.
     *
     * @return void
     */
    private function _checkEmptyVisitFolders(string $sourceDir, array $headers): void
    {
        $withData = [];
        foreach ($headers as $row) {
            if ($row['subject'] !== null && $row['session'] !== null) {
                $withData[$row['subject'] . '/' . $row['session']] = true;
            }
        }

        foreach (glob($sourceDir . '/sub-*', GLOB_ONLYDIR) ?: [] as $subDir) {
            $subject = substr(basename($subDir), 4);

            foreach (glob($subDir . '/ses-*', GLOB_ONLYDIR) ?: [] as $sesDir) {
                $session = substr(basename($sesDir), 4);

                if (!isset($withData[$subject . '/' . $session])) {
                    $this->_problem(
                        'warning',
                        null,
                        "sub-{$subject}/ses-{$session}: no readable DICOM files"
                    );
                }
            }
        }
    }

    /**
     * Record a study-level error and skip the study.
     *
     * @param string $studyUid StudyInstanceUID.
     * @param string $message  What went wrong.
     *
     * @return null
     *
     * @throws \RuntimeException in strict mode.
     */
    private function _reject(string $studyUid, string $message)
    {
        $this->_problem('error', $studyUid, $message);

        if ($this->strict) {
            throw new \RuntimeException("Strict mode: {$message}");
        }

        return null;
    }

    /**
     * Handle a failed resolution check.
     *
     * With the unlinked fallback on (the default), this records a warning and
     * returns true so the caller carries the study forward as unlinked. With
     * it off, the study is rejected outright.
     *
     * @param string $studyUid StudyInstanceUID.
     * @param string $message  What could not be resolved.
     *
     * @return bool|null True to degrade, null to skip the study.
     *
     * @throws \RuntimeException in strict mode.
     */
    private function _degrade(string $studyUid, string $message)
    {
        if ($this->strict) {
            $this->_problem('error', $studyUid, $message);
            throw new \RuntimeException("Strict mode: {$message}");
        }

        if (!$this->fallbackToUnlinked) {
            $this->_problem('error', $studyUid, $message);
            return null;
        }

        $this->stats['studies_degraded']++;
        $this->_problem(
            'warning',
            $studyUid,
            $message . ' - UNLINKED: not processed or ingested until it matches a participant'
        );

        return true;
    }

    private function _problem(string $level, ?string $studyUid, string $message, array $extra = []): void
    {
        $this->problems[] = [
            'level'              => $level,
            'study_instance_uid' => $studyUid,
            'message'            => $message,
        ] + $extra;

        if ($level === 'error') {
            $this->_error($message);
        } else {
            $this->_warn($message);
        }
    }

    // =========================================================================
    //  WRITING
    // =========================================================================

    /**
     * Copy or move a study's files into its organized layout.
     *
     * @param array $entry     Manifest entry from _buildEntry().
     * @param array $seriesMap SeriesInstanceUID => rows.
     *
     * @return void
     */
    private function _writeStudy(array $entry, array $seriesMap): void
    {
        $this->_info("  {$entry['study_instance_uid']}");
        $this->_info(sprintf(
            '    -> %s',
            $entry['directory_name']
        ));

        foreach ($entry['series'] as $seriesEntry) {
            $this->_info(sprintf(
                '       %s (%s, %d file(s))',
                $seriesEntry['directory'],
                $seriesEntry['modality'] ?: 'NA',
                $seriesEntry['file_count']
            ));
        }

        if ($this->dryRun) {
            $this->_info('');
            return;
        }

        foreach ($seriesMap as $rows) {
            $seriesDir = $entry['directory'] . '/' . $this->_seriesDirName($rows[0]);
            $this->_transferSeries($rows, $seriesDir);
        }

        $this->stats['studies_written']++;
        $this->stats['files_organized'] += $entry['file_count'];

        $this->_info('');
    }

    /**
     * Copy or move one series' files, resolving basename collisions.
     *
     * @param array  $rows      Header rows.
     * @param string $seriesDir Destination directory.
     *
     * @return void
     */
    private function _transferSeries(array $rows, string $seriesDir): void
    {
        if (!is_dir($seriesDir)
            && !@mkdir($seriesDir, 0775, true)
            && !is_dir($seriesDir)
        ) {
            throw new \RuntimeException("Could not create {$seriesDir}");
        }

        foreach ($rows as $row) {
            $source = $row['path'];
            $dest   = $seriesDir . '/' . basename($source);

            if (file_exists($dest)) {
                $info = pathinfo($source);
                $stem = $info['filename'];
                $ext  = isset($info['extension']) ? '.' . $info['extension'] : '';
                $n    = 1;
                while (file_exists($dest)) {
                    $dest = $seriesDir . '/' . $stem . '_' . $n . $ext;
                    $n++;
                }
            }

            $ok = $this->move ? rename($source, $dest) : copy($source, $dest);

            if (!$ok) {
                throw new \RuntimeException(sprintf(
                    'Failed to %s %s',
                    $this->move ? 'move' : 'copy',
                    $source
                ));
            }
        }
    }

    // =========================================================================
    //  NAMING
    // =========================================================================

    /**
     * Study directory name: sub-<id>_ses-<visit>_<modalities>[_N]
     *
     * Modalities are joined rather than reduced to one, because a PET/CT is a
     * single StudyInstanceUID carrying both and no single suffix is correct.
     * Stage 2 replaces sub-/ses- with PSCID_CandID_VisitLabel; the shape is
     * kept identical so that rename is mechanical.
     *
     * @param string   $subject    Subject identifier.
     * @param string   $session    Session identifier.
     * @param string[] $modalities Sorted, unique modalities in the study.
     * @param array    $usedDirs   Names already claimed this run.
     *
     * @return string
     */
    private function _studyDirName(
        ?string $subject,
        ?string $session,
        array $modalities,
        array $usedDirs,
        string $linkStatus = 'linked',
        string $studyDate = '',
        string $studyUid = ''
    ): string {
        $modalityPart = empty($modalities)
            ? 'NA'
            : implode('-', array_map([$this, '_slugPart'], $modalities));

        $prefix = $linkStatus === 'unlinked' ? 'unlinked-' : '';

        if ($subject === null) {
            // Unorganized upload: nothing in the path identifies it, so fall
            // back to the study's own attributes.
            $parts = explode('.', $studyUid);
            $tail  = substr(end($parts), 0, 12);

            $base = sprintf(
                '%s%s_%s_%s',
                $prefix ?: 'unlinked-',
                $this->_slugPart($studyDate ?: 'nodate'),
                $this->_slugPart($tail ?: 'nouid'),
                $modalityPart
            );
        } else {
            $base = sprintf(
                '%ssub-%s_ses-%s_%s',
                $prefix,
                $this->_slugPart($subject),
                $this->_slugPart($session ?? 'nosession'),
                $modalityPart
            );
        }

        // Two studies can share subject, session and modality set.
        if (!isset($usedDirs[$base]) && !is_dir($this->_studyRoot() . '/' . $base)) {
            return $base;
        }

        $n = 2;
        while (isset($usedDirs["{$base}_{$n}"])
            || is_dir($this->_studyRoot() . "/{$base}_{$n}")
        ) {
            $n++;
        }

        return "{$base}_{$n}";
    }

    /**
     * Series directory name: series-NNNN_<modality>_<description>
     *
     * @param array $sample One header row from the series.
     *
     * @return string
     */
    /**
     * Append a counter only when the name is already taken.
     *
     * @param string $base     Desired name.
     * @param array  $usedDirs Names claimed this run.
     *
     * @return string
     */
    private function _uniqueName(string $base, array $usedDirs): string
    {
        if (!isset($usedDirs[$base]) && !is_dir($this->_studyRoot() . '/' . $base)) {
            return $base;
        }

        $n = 2;

        while (isset($usedDirs["{$base}_{$n}"])
            || is_dir($this->_studyRoot() . "/{$base}_{$n}")
        ) {
            $n++;
        }

        return "{$base}_{$n}";
    }

    private function _seriesDirName(array $sample): string
    {
        return sprintf(
            'series-%04d_%s_%s',
            intval($sample['series_number'] ?? 0),
            $this->_slugPart($sample['modality'] ?: 'NA'),
            $this->_slugPart($sample['series_description'] ?: 'nodescr')
        );
    }

    private function _slugPart(string $value): string
    {
        // Underscores are kept: site identifiers like
        // TST02_ROM_00000001_02_SE01_MR are built from them, and replacing
        // them would defeat preserving the name in the first place.
        $clean = preg_replace('/[^A-Za-z0-9._-]+/', '-', $value);
        $clean = trim($clean, '-');
        $clean = substr($clean, 0, 96);

        return $clean !== '' ? $clean : 'na';
    }

    // =========================================================================
    //  PARTICIPANTS
    // =========================================================================

    /**
     * Load participants.tsv from the upload root.
     *
     * Absence is not fatal here - it is reported per study instead (every
     * study is then unlinked).
     *
     * @param string $sourceDir Upload root.
     *
     * @return array<string, array> participant_id => row.
     */
    private function _loadParticipants(string $sourceDir): array
    {
        $path = $sourceDir . '/participants.tsv';

        if (!file_exists($path)) {
            $this->_warn("participants.tsv not found at {$path}");
            return [];
        }

        $handle = fopen($path, 'r');
        if ($handle === false) {
            throw new \RuntimeException("Could not open {$path}");
        }

        $header = fgetcsv($handle, 0, "\t");
        if ($header === false) {
            fclose($handle);
            throw new \RuntimeException("participants.tsv is empty: {$path}");
        }

        $header = array_map('trim', $header);
        $idCol  = array_search('participant_id', $header, true);

        if ($idCol === false) {
            fclose($handle);
            throw new \RuntimeException(
                "participants.tsv has no participant_id column: {$path}"
            );
        }

        $rows = [];

        while (($line = fgetcsv($handle, 0, "\t")) !== false) {
            if ($line === [null]) {
                continue;
            }

            $values = array_map('trim', $line);
            $values = array_pad(array_slice($values, 0, count($header)), count($header), '');

            $row = array_combine($header, $values);
            $id  = $row['participant_id'] ?? '';

            if ($id === '') {
                continue;
            }

            $rows[$id] = $row;
        }

        fclose($handle);

        return $rows;
    }

    // =========================================================================
    //  STATE
    // =========================================================================

    private function _studyRoot(): string
    {
        return $this->targetDir . '/' . self::STUDY_SUBDIR;
    }

    private function _trackingFilePath(): string
    {
        return $this->targetDir . '/' . self::TRACK_FILE;
    }

    /**
     * Create the output directories, tolerating a concurrent creator.
     *
     * @return void
     */
    private function _ensureTargetDir(): void
    {
        // In a dry run only the target root is created, for the manifest -
        // the study directory tree is not.
        $dirs = $this->dryRun
            ? [$this->targetDir]
            : [$this->targetDir, $this->_studyRoot()];

        foreach ($dirs as $dir) {
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
        $path = $this->_trackingFilePath();

        if (!file_exists($path)) {
            return ['studies' => []];
        }

        $raw = file_get_contents($path);
        if ($raw === false) {
            throw new \RuntimeException("Could not read tracking file: {$path}");
        }

        $data = json_decode($raw, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \RuntimeException(
                "Corrupt tracking file {$path}: " . json_last_error_msg()
            );
        }

        if (!isset($data['studies']) || !is_array($data['studies'])) {
            $data['studies'] = [];
        }

        return $data;
    }

    private function _writeTracking(array $tracking): void
    {
        $tracking['updated_at'] = date('c');
        $this->_atomicWriteJson($this->_trackingFilePath(), $tracking);
    }

    private function _writeManifest(array $manifest): void
    {
        $this->_atomicWriteJson(
            $this->targetDir . '/' . self::MANIFEST_FILE,
            [
                'generated_at' => date('c'),
                'dry_run'      => $this->dryRun,
                'study_count'  => count($manifest),
                'stats'        => $this->stats,
                'problems'     => $this->problems,
                'studies'      => $manifest,
            ]
        );
    }

    /**
     * Write JSON via temp file and rename, so readers never see a partial file.
     *
     * @param string $path Destination.
     * @param array  $data Payload.
     *
     * @return void
     */
    private function _atomicWriteJson(string $path, array $data): void
    {
        // Header text in another character set (SpecificCharacterSet ISO_IR 100,
        // a Latin-1 SeriesDescription) is not valid UTF-8 and would make
        // json_encode fail, stopping the whole delivery. Substitute instead.
        $json = json_encode(
            $data,
            JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE
        );

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
    //  LOGGING
    // =========================================================================

    private function _info(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->info($message);
        }
    }

    private function _warn(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->warning($message);
        }
    }

    private function _error(string $message): void
    {
        if ($this->logger === null) {
            return;
        }

        if (method_exists($this->logger, 'error')) {
            $this->logger->error($message);
        } else {
            $this->logger->warning($message);
        }
    }

    // =========================================================================
    //  Project selection, shared by every DICOM script and the bundle:
    //  enabled in the config + Imaging in project.json; upload quiet period.
    // =========================================================================

    /**
     * Why this project must not be processed for DICOM, or null if it may.
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
     * A run that starts while a site is uploading would organize half a study
     * and read a half-written participants.tsv. If anything under the raw
     * delivery changed within the quiet period, the project is left for the
     * next run. Stat calls only.
     *
     * @param string $rawDir  deidentified-raw/imaging/dicoms
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
}
