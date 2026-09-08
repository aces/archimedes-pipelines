<?php
/**
 * DicomHeaderWriter
 *
 * Thin wrapper around DCMTK's dcmodify for the header rewrites ARCHIMEDES
 * performs: replacing PatientName with the LORIS conventional identifier and
 * preserving the site's original value in OtherPatientNames.
 *
 * Nothing else in the headers is modified.
 *
 * PHP Version 8.1
 *
 * @category Pipeline
 * @package  Archimedes
 */

namespace LORIS\Pipelines;

class DicomHeaderWriter
{
    /**
     * Patient's Name.
     *
     * Numeric tags are used throughout rather than dictionary keywords, so
     * this does not depend on the DCMTK dictionary spelling being what we
     * expect. dcmodify accepts (gggg,eeee) directly.
     */
    const TAG_PATIENT_NAME = '(0010,0010)';

    /**
     * Other Patient Names. VR is PN, VM is 1-n, so multiple values are
     * separated by a backslash - which is why backslashes are rejected in
     * the values below.
     */
    const TAG_OTHER_PATIENT_NAMES = '(0010,1001)';

    /**
     * Files per dcmodify invocation, to stay under ARG_MAX.
     */
    const CHUNK_SIZE = 200;

    /** @var string Absolute path to dcmodify. */
    private $dcmodifyPath;

    /** @var object|null */
    private $logger;

    /**
     * @param string      $dcmodifyPath Absolute path to dcmodify.
     * @param object|null $logger       Optional logger.
     */
    public function __construct(string $dcmodifyPath = '/usr/bin/dcmodify', $logger = null)
    {
        $this->dcmodifyPath = $dcmodifyPath;
        $this->logger       = $logger;
    }

    /**
     * Confirm dcmodify is present and runnable.
     *
     * @return void
     *
     * @throws \RuntimeException when it is not.
     */
    public function assertAvailable(): void
    {
        if (!is_executable($this->dcmodifyPath)) {
            throw new \RuntimeException(
                "dcmodify not executable: {$this->dcmodifyPath} "
                . '(install the dcmtk package)'
            );
        }
    }

    /**
     * Rewrite patient identity headers across a study directory.
     *
     * Operates in place, so the caller must have already copied the files.
     * Originals are never touched by this class.
     *
     * @param string $studyDir     Directory holding the copied DICOMs.
     * @param string $patientName  New PatientName (LORIS conventional form).
     * @param string $originalName Value to preserve in OtherPatientNames.
     *
     * @return int Number of files modified.
     *
     * @throws \RuntimeException if dcmodify fails on any chunk.
     */
    public function rewritePatientIdentity(
        string $studyDir,
        string $patientName,
        string $originalName
    ): int {
        $this->assertAvailable();

        $files = $this->_collectFiles($studyDir);

        if (empty($files)) {
            throw new \RuntimeException("No files to modify under {$studyDir}");
        }

        // dcmodify splits on backslash for multi-valued fields, and caret is
        // the DICOM person-name component separator. Neither belongs in an
        // identifier, so reject rather than silently mangling.
        foreach (['patientName' => $patientName, 'originalName' => $originalName] as $label => $value) {
            if (strpbrk($value, "\\^=") !== false) {
                throw new \RuntimeException(
                    "{$label} contains a character dcmodify cannot carry safely: {$value}"
                );
            }
        }

        $modified = 0;

        foreach (array_chunk($files, self::CHUNK_SIZE) as $chunk) {
            $cmd = sprintf(
                // -nb: no backup files. Without this dcmodify writes a .bak
                // beside every file, and those end up inside the tarchive.
                // -i inserts the tag when absent and overwrites when present,
                // which matters because OtherPatientNames is usually absent.
                '%s -nb -i %s -i %s %s 2>&1',
                escapeshellarg($this->dcmodifyPath),
                escapeshellarg(self::TAG_PATIENT_NAME . '=' . $patientName),
                escapeshellarg(self::TAG_OTHER_PATIENT_NAMES . '=' . $originalName),
                implode(' ', array_map('escapeshellarg', $chunk))
            );

            $output   = [];
            $exitCode = 0;
            exec($cmd, $output, $exitCode);

            if ($exitCode !== 0) {
                throw new \RuntimeException(sprintf(
                    'dcmodify failed (exit %d) on %d file(s) in %s: %s',
                    $exitCode,
                    count($chunk),
                    $studyDir,
                    implode(' | ', array_slice($output, 0, 3))
                ));
            }

            $modified += count($chunk);
        }

        $this->_info(sprintf(
            '       rewrote PatientName on %d file(s) -> %s',
            $modified,
            $patientName
        ));

        return $modified;
    }

    /**
     * Read back PatientName and OtherPatientNames from one file, for verification.
     *
     * @param string $file Absolute path to a DICOM file.
     *
     * @return array{patient_name: string, other_patient_names: string}
     */
    public function readPatientIdentity(string $file): array
    {
        $dcmdump = dirname($this->dcmodifyPath) . '/dcmdump';

        if (!is_executable($dcmdump)) {
            throw new \RuntimeException("dcmdump not executable: {$dcmdump}");
        }

        $cmd = sprintf(
            '%s +P %s +P %s %s 2>/dev/null',
            escapeshellarg($dcmdump),
            escapeshellarg('0010,0010'),
            escapeshellarg('0010,1001'),
            escapeshellarg($file)
        );

        $output = [];
        exec($cmd, $output);

        $result = ['patient_name' => '', 'other_patient_names' => ''];

        foreach ($output as $line) {
            if (preg_match('/\(0010,0010\).*?\[([^\]]*)\]/', $line, $m)) {
                $result['patient_name'] = $m[1];
            }
            if (preg_match('/\(0010,1001\).*?\[([^\]]*)\]/', $line, $m)) {
                $result['other_patient_names'] = $m[1];
            }
        }

        return $result;
    }

    /**
     * Collect every regular file under a directory.
     *
     * @param string $dir Study directory.
     *
     * @return string[] Absolute paths.
     */
    private function _collectFiles(string $dir): array
    {
        if (!is_dir($dir)) {
            throw new \RuntimeException("Not a directory: {$dir}");
        }

        $files    = [];
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::LEAVES_ONLY
        );

        foreach ($iterator as $fileInfo) {
            if ($fileInfo->isFile()) {
                $files[] = $fileInfo->getPathname();
            }
        }

        sort($files);

        return $files;
    }

    private function _info(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->info($message);
        }
    }
}
