<?php
/**
 * DicomStudyOrganizer
 *
 * Stage 1 of the ARCHIMEDES DICOM pipeline.
 *
 * Walks a delivered upload laid out as sub-<id>/ses-<visit>/..., groups the
 * DICOMs by StudyInstanceUID, validates the delivery against the submission
 * guide, and writes an organised copy plus a manifest.
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

namespace LORIS\Pipelines;

class DicomStudyOrganizer
{
    /** Studies are written under <target>/dicoms/ */
    const STUDY_SUBDIR = 'dicoms';

    /** Idempotency state, keyed on StudyInstanceUID. */
    const TRACK_FILE = '.dicom_organize_tracking.json';

    /** Handoff to the next pipeline stage. */
    const MANIFEST_FILE = 'dicom_studies.json';

    /** Subject folders starting with this are treated as phantoms. */
    const PHANTOM_PREFIX = 'phantom';

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
     * pydicom is what import_dicom_study.py uses, so the organiser and the
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

    /** @var bool Treat every subject in this run as a phantom. */
    private $phantomRun = false;

    /** @var bool Abort on the first study-level error rather than skipping. */
    private $strict = false;

    /** @var bool Ignore the tracking file and reprocess every study. */
    private $force = false;

    /**
     * When true (the default), a study that cannot be resolved to a
     * participant and visit is carried forward as a phantom rather than
     * skipped. The reason is recorded in the manifest as phantom_reason.
     *
     * @var bool
     */
    private $fallbackToPhantom = true;

    /** @var array<string, array> participant_id => row from participants.tsv */
    private $participants = [];

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
        'phantom_studies' => 0,
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

    public function setPhantomRun(bool $v): void
    {
        $this->phantomRun = $v;
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

    /**
     * Carry unresolvable studies forward as unlinked instead of skipping them.
     *
     * Unlinked means archived with no participant link - not phantom.
     *
     * @param bool $v Whether to degrade rather than skip.
     *
     * @return void
     */
    public function setFallbackToUnlinked(bool $v): void
    {
        $this->fallbackToPhantom = $v;
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
     * Organise every DICOM under $sourceDir.
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

        $this->participants = $this->_loadParticipants($sourceDir);
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

        $this->_checkEmptyVisitFolders($sourceDir, $headers);

        $studies = $this->_groupByStudy($headers);
        $this->stats['studies_found'] = count($studies);
        $this->_info(sprintf('Found %d DICOM study/studies', count($studies)));
        $this->_info('');

        $tracking = $this->_readTracking();
        $manifest = [];
        $usedDirs = [];

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

            $comparison = StudyFingerprint::compare(
                $this->force ? null : $tracked,
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

                // Refresh the stored hashes even when skipping, so a renamed
                // copy does not force a byte read on every future run.
                $tracking['studies'][$studyUid] = array_merge(
                    $tracked,
                    StudyFingerprint::toTracking($comparison, $studyFiles, $sourceDir)
                );

                if (!$this->dryRun) {
                    $this->_writeTracking($tracking);
                }

                $manifest[] = $tracked;
                continue;
            }

            if ($tracked !== null) {
                $this->stats['studies_rechanged']++;
                $this->_warn(sprintf(
                    '%s: %s - re-organising',
                    $studyUid,
                    $comparison['reason']
                ));
            }

            $entry = $this->_buildEntry($studyUid, $seriesMap, $usedDirs);

            if ($entry === null) {
                $this->stats['studies_skipped']++;
                continue;
            }

            $this->_writeStudy($entry, $seriesMap);

            // Hashes are stored only after a successful write, so a stored
            // fingerprint always means "this study was organised".
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
        array &$usedDirs
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

        // Three states, not two:
        //   linked   - resolvable to a participant and visit
        //   unlinked - archived with no participant link (the default today)
        //   phantom  - genuinely a phantom acquisition, IsPhantom = 1
        // An unresolvable subject scan is unlinked, never phantom: setting
        // IsPhantom on real patient data is far harder to undo later.
        $isPhantom  = $this->_isPhantom($subject);
        $linkStatus = $isPhantom ? 'phantom' : 'linked';
        $linkReason = null;

        $where = sprintf(
            'sub-%s/ses-%s',
            $subject ?? '?',
            $session ?? '?'
        );

        // Phantom status is decided only by declaration. Everything below can
        // downgrade a study to unlinked, never to phantom.

        // --- Subject derivable from the path ---------------------------------
        if ($subject === null && !$isPhantom) {
            $degraded = $this->_degrade(
                $studyUid,
                'upload is not organised into sub- folders'
            );
            if ($degraded === null) {
                return null;
            }
            $linkStatus = 'unlinked';
            $linkReason = 'not_organised';
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

        // --- PatientName: informational only ------------------------------------
        // Never affects link status. A phantom may legitimately carry a site
        // external ID here.
        $names = [];
        foreach ($allRows as $row) {
            if ($row['patient_name'] !== '') {
                $names[$row['patient_name']] = true;
            }
        }

        $patientName = empty($names) ? '' : (string) array_key_first($names);

        if (count($names) > 1) {
            $this->_problem('warning', $studyUid, sprintf(
                '%s: more than one PatientName within a single study (%s)',
                $where,
                implode(', ', array_keys($names))
            ));
        } elseif (empty($names) && $linkStatus === 'linked') {
            $this->_problem(
                'warning',
                $studyUid,
                "{$where}: PatientName is empty; using the sub- folder as external ID"
            );
        } elseif ($linkStatus === 'linked' && $subject !== null && $patientName !== $subject) {
            $this->_problem('warning', $studyUid, sprintf(
                '%s: PatientName "%s" differs from the sub- folder; using the folder',
                $where,
                $patientName
            ));
        }

        // For a linked subject the folder wins; otherwise keep the header value.
        $externalId = $linkStatus === 'linked'
            ? ($subject ?? $patientName)
            : ($patientName !== '' ? $patientName : ($subject ?? ''));

        $isPhantom = ($linkStatus === 'phantom');

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

        if ($this->preserveSourceNames && count($sourceDirs) === 1) {
            $only = (string) array_key_first($sourceDirs);
            $base = basename($only);

            if ($base !== '' && $base !== '.') {
                $preservedName = $base;
            }
        }

        $dirName = $preservedName !== null
            ? $this->_uniqueName($this->_slugPart($preservedName), $usedDirs)
            : $this->_studyDirName(
                $subject,
                $session,
                $modalities,
                $usedDirs,
                $linkStatus,
                $sample['study_date'],
                $studyUid
            );
        $usedDirs[$dirName] = true;

        if ($linkStatus === 'phantom') {
            $this->stats['phantom_studies']++;
        } elseif ($linkStatus === 'unlinked') {
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
            'is_phantom'         => $isPhantom,
            'series_count'       => count($seriesEntries),
            'file_count'         => count($allRows),
            'organized_at'       => date('c'),
            'series'             => $seriesEntries,
        ];
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
     * Is this subject a phantom?
     *
     * @param string|null $subject Subject identifier.
     *
     * @return bool
     */
    private function _isPhantom(?string $subject): bool
    {
        if ($this->phantomRun) {
            return true;
        }

        if ($subject === null) {
            return false;
        }

        return stripos($subject, self::PHANTOM_PREFIX) === 0;
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
     * With the phantom fallback on, this records a warning and returns true so
     * the caller carries the study forward as a phantom. With it off, the
     * study is rejected outright.
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

        if (!$this->fallbackToPhantom) {
            $this->_problem('error', $studyUid, $message);
            return null;
        }

        $this->stats['studies_degraded']++;
        $this->_problem(
            'warning',
            $studyUid,
            $message . ' - archiving UNLINKED (no participant link)'
        );

        return true;
    }

    private function _problem(string $level, ?string $studyUid, string $message): void
    {
        $this->problems[] = [
            'level'              => $level,
            'study_instance_uid' => $studyUid,
            'message'            => $message,
        ];

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
     * Copy or move a study's files into its organised layout.
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
            '    -> %s%s',
            $entry['directory_name'],
            $entry['is_phantom'] ? '   [phantom]' : ''
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

        $prefix = '';
        if ($linkStatus === 'phantom') {
            $prefix = 'phantom-';
        } elseif ($linkStatus === 'unlinked') {
            $prefix = 'unlinked-';
        }

        if ($subject === null) {
            // Unorganised upload: nothing in the path identifies it, so fall
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
     * Absence is not fatal here - it is reported per study instead, so a
     * phantom-only delivery works without one.
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
}