<?php

/**
 * DicomEviDataExtract
 *
 * Builds a tabular CSV of DICOM header values for EviData privacy-risk
 * validation, and reports which columns are quasi-identifiers.
 *
 * EviData scores re-identification risk over a table: rows are records,
 * columns are attributes, and quasi-identifiers are the columns an adversary
 * could combine. So the extract is a wide table - one row per study, one
 * column per DICOM tag - not a list of findings.
 *
 * Read-only. Nothing in the delivery is modified.
 *
 * -----------------------------------------------------------------------------
 * CONFIGURATION - uses the existing evidata blocks, not a new one.
 *
 * Global, config/evidata_config.json:
 *     "evidata": { "enabled": true, "api_base_url": ..., "population_size": ... }
 *
 * Per project, project.json:
 *     "evidata": {
 *         "qis": [],
 *         "exclude_qis": ["Modality", "SeriesDescription"],
 *         "imaging":  { "enabled": true, "scan_mode": "full" },
 *         "clinical": { "enabled": true }
 *     }
 *
 * QI resolution follows the same precedence the clinical pipeline uses:
 * project qis -> global qis -> all-columns default. exclude_qis is subtracted
 * from whichever set wins.
 *
 * Each modality is enabled separately: the global service switch must be on,
 * the project must not be disabled, and evidata.imaging.enabled must be
 * explicitly true. Enabling EviData for clinical never enables it for imaging.
 * See isEnabled().
 * -----------------------------------------------------------------------------
 *
 * PHP Version 8.1
 *
 * @category Pipeline
 * @package  Archimedes
 */

declare(strict_types=1);

namespace LORIS\Pipelines;

use RuntimeException;

class DicomEviDataExtract
{
    /**
     * Columns extracted from every study, as [group, element, label].
     *
     * Direct and indirect identifiers plus the free-text fields that commonly
     * carry notes typed at the scanner. Everything here becomes a CSV column;
     * which of them count as quasi-identifiers is decided by config.
     */
    public const COLUMNS = [
        // Record key - never a QI, used to trace a finding back
        ['0020', '000d', 'StudyInstanceUID'],

        // Direct identifiers
        ['0010', '0010', 'PatientName'],
        ['0010', '0020', 'PatientID'],
        ['0010', '1000', 'OtherPatientIDs'],
        ['0010', '1001', 'OtherPatientNames'],
        ['0010', '1040', 'PatientAddress'],
        ['0010', '2154', 'PatientTelephoneNumbers'],
        ['0008', '0050', 'AccessionNumber'],
        ['0008', '0090', 'ReferringPhysicianName'],
        ['0008', '1048', 'PhysiciansOfRecord'],
        ['0008', '1050', 'PerformingPhysicianName'],
        ['0008', '1070', 'OperatorsName'],
        ['0032', '1032', 'RequestingPhysician'],

        // Quasi-identifiers: individually weak, jointly identifying
        ['0010', '0030', 'PatientBirthDate'],
        ['0010', '0040', 'PatientSex'],
        ['0010', '1010', 'PatientAge'],
        ['0010', '1020', 'PatientSize'],
        ['0010', '1030', 'PatientWeight'],
        ['0008', '0020', 'StudyDate'],
        ['0008', '0030', 'StudyTime'],
        ['0008', '0080', 'InstitutionName'],
        ['0008', '0081', 'InstitutionAddress'],
        ['0008', '1010', 'StationName'],
        ['0008', '1040', 'InstitutionalDepartmentName'],
        ['0008', '0060', 'Modality'],
        ['0008', '0070', 'Manufacturer'],
        ['0008', '1090', 'ManufacturerModelName'],
        ['0018', '1000', 'DeviceSerialNumber'],

        // Free text
        ['0008', '1030', 'StudyDescription'],
        ['0008', '103E', 'SeriesDescription'],
        ['0010', '4000', 'PatientComments'],
        ['0010', '21B0', 'AdditionalPatientHistory'],
        ['0032', '1060', 'RequestedProcedureDescription'],
        ['0038', '0300', 'CurrentPatientLocation'],

        // De-identification declarations
        ['0012', '0062', 'PatientIdentityRemoved'],
        ['0012', '0063', 'DeidentificationMethod'],
    ];

    /**
     * Never a quasi-identifier: a record key, not an attribute.
     */
    private const NEVER_QI = ['StudyInstanceUID'];

    private string $dcmdumpPath;

    private ?object $logger;

    /** @var string[] Column labels, in output order. */
    private array $columns;

    /** @var string[] Columns treated as quasi-identifiers. */
    private array $qis = [];

    /** @var string[] Columns excluded from the QI set. */
    private array $excludeQis = [];

    /** 'full' dumps every file; 'sample' one per leaf directory. */
    private string $scanMode = 'full';

    private int $filesScanned = 0;

    private int $filesUnreadable = 0;

    private int $studiesExtracted = 0;

    /** @var array<int, array<string, string>> One row per study. */
    private array $rows = [];

    /**
     * @param string      $dcmdumpPath Absolute path to dcmdump.
     * @param array       $evidata     Merged global + project evidata config.
     * @param object|null $logger      Optional logger.
     */
    public function __construct(
        string $dcmdumpPath,
        array $evidata = [],
        ?object $logger = null
    ) {
        $this->dcmdumpPath = $dcmdumpPath;
        $this->logger      = $logger;

        $this->columns = array_map(
            static fn (array $c): string => $c[2],
            self::COLUMNS
        );

        $this->qis        = array_values((array) ($evidata['qis'] ?? []));
        $this->excludeQis = array_values((array) ($evidata['exclude_qis'] ?? []));

        $mode = $evidata['imaging']['scan_mode'] ?? $evidata['scan_mode'] ?? 'full';

        if ($mode === 'sample') {
            $this->scanMode = 'sample';
        }
    }

    /**
     * Is the EviData check enabled for this project and modality?
     *
     * Each modality must be enabled explicitly. There is no inheritance from
     * the project switch to the modalities: turning EviData on for clinical
     * must never silently start dumping DICOM headers, and vice versa. The
     * two pipelines send different data to the service and are signed off
     * separately.
     *
     *   1. Global, config/evidata_config.json -> evidata.enabled
     *      The service switch. False here means off everywhere.
     *
     *   2. Project, project.json -> evidata.enabled
     *      A kill switch for the whole project. False here means off for both
     *      modalities regardless of what the modality blocks say. Absent means
     *      "not disabled", not "enabled".
     *
     *   3. Project, project.json -> evidata.<modality>.enabled
     *      The actual switch. Must be explicitly true. Absent means off.
     *
     * @param array  $projectConfig Decoded project.json.
     * @param array  $globalConfig  Decoded evidata_config.json.
     * @param string $modality      'imaging' or 'clinical'.
     */
    public static function isEnabled(
        array $projectConfig,
        array $globalConfig,
        string $modality = 'imaging'
    ): bool {
        return self::explainEnabled($projectConfig, $globalConfig, $modality)['enabled'];
    }

    /**
     * Resolve the state and say which rule decided it.
     *
     * Reported on every run, so a check that did not happen is never silent.
     *
     * @return array{enabled: bool, decided_by: string}
     */
    public static function explainEnabled(
        array $projectConfig,
        array $globalConfig,
        string $modality = 'imaging'
    ): array {
        if (empty($globalConfig['evidata']['enabled'])) {
            return [
                'enabled'    => false,
                'decided_by' => 'global evidata.enabled is false',
            ];
        }

        $project = $projectConfig['evidata'] ?? [];

        // Project kill switch. Only an explicit false disables; absent means
        // "carry on and check the modality", not "enabled".
        if (array_key_exists('enabled', $project) && !$project['enabled']) {
            return [
                'enabled'    => false,
                'decided_by' => 'project evidata.enabled is false',
            ];
        }

        $block = $project[$modality] ?? null;

        if (!is_array($block) || !array_key_exists('enabled', $block)) {
            return [
                'enabled'    => false,
                'decided_by' => "project evidata.{$modality}.enabled is not set",
            ];
        }

        return [
            'enabled'    => (bool) $block['enabled'],
            'decided_by' => "project evidata.{$modality}.enabled",
        ];
    }

    /**
     * Merge global and project evidata config, project taking precedence.
     *
     * The same precedence the clinical pipeline uses.
     */
    public static function mergeConfig(array $projectConfig, array $globalConfig): array
    {
        return array_merge(
            $globalConfig['evidata'] ?? [],
            $projectConfig['evidata'] ?? []
        );
    }

    /**
     * Columns treated as quasi-identifiers.
     *
     * Precedence: project/global qis, else all columns; exclude_qis is then
     * subtracted, and the record key is never included.
     *
     * @return string[]
     */
    public function quasiIdentifiers(): array
    {
        $base = !empty($this->qis) ? $this->qis : $this->columns;

        $qis = array_values(array_diff($base, $this->excludeQis, self::NEVER_QI));

        return $qis;
    }

    /**
     * @return array{files: int, unreadable: int, studies: int, scan_mode: string,
     *               columns: int, qis: int}
     */
    public function summary(): array
    {
        return [
            'files'      => $this->filesScanned,
            'unreadable' => $this->filesUnreadable,
            'studies'    => $this->studiesExtracted,
            'scan_mode'  => $this->scanMode,
            'columns'    => count($this->columns),
            'qis'        => count($this->quasiIdentifiers()),
        ];
    }

    /**
     * Columns that are populated in at least one row.
     *
     * Useful for reporting: an absent identifier is the desired state.
     *
     * @return string[]
     */
    public function populatedColumns(): array
    {
        $populated = [];

        foreach ($this->rows as $row) {
            foreach ($row as $column => $value) {
                if ($value !== '') {
                    $populated[$column] = true;
                }
            }
        }

        unset($populated['StudyInstanceUID']);

        return array_keys($populated);
    }

    // =========================================================================
    //  EXTRACTION
    // =========================================================================

    /**
     * Extract a table from the original delivery.
     *
     * @param string $sourceDir  Raw delivery root.
     * @param string $outputPath Where to write the CSV.
     *
     * @return string Path to the CSV.
     *
     * @throws RuntimeException when dcmdump is unavailable or the write fails.
     */
    public function extract(string $sourceDir, string $outputPath): string
    {
        if (!is_executable($this->dcmdumpPath)) {
            throw new RuntimeException(
                "dcmdump not executable: {$this->dcmdumpPath} (install dcmtk)"
            );
        }

        if (!is_dir($sourceDir)) {
            throw new RuntimeException("Source not a directory: {$sourceDir}");
        }

        $sourceDir = rtrim($sourceDir, '/');
        $files     = $this->collectFiles($sourceDir);

        if (empty($files)) {
            throw new RuntimeException("No readable files under {$sourceDir}");
        }

        // One row per study. Header values are consistent within a study for
        // the identifying tags, so the first readable file wins and later
        // files only fill blanks.
        $byStudy = [];

        foreach ($files as $file) {
            $this->filesScanned++;

            if (($this->filesScanned % 1000) === 0) {
                $this->info(sprintf('    dumped %d files', $this->filesScanned));
            }

            $dump = $this->dump($file);

            if ($dump === null) {
                $this->filesUnreadable++;
                continue;
            }

            $values   = $this->parseDump($dump);
            $studyUid = $values['StudyInstanceUID'] ?? '';

            if ($studyUid === '') {
                $this->filesUnreadable++;
                continue;
            }

            if (!isset($byStudy[$studyUid])) {
                $byStudy[$studyUid] = array_fill_keys($this->columns, '');
            }

            foreach ($values as $column => $value) {
                if ($value !== '' && ($byStudy[$studyUid][$column] ?? '') === '') {
                    $byStudy[$studyUid][$column] = $value;
                }
            }

            // In sample mode one file per directory is enough.
            if ($this->scanMode === 'sample') {
                continue;
            }
        }

        ksort($byStudy);

        $this->rows             = array_values($byStudy);
        $this->studiesExtracted = count($this->rows);

        $this->write($outputPath);

        return $outputPath;
    }

    /**
     * Candidate files, honouring scan mode.
     *
     * @return string[]
     */
    private function collectFiles(string $root): array
    {
        $files    = [];
        $seenDirs = [];

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($root, \FilesystemIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::LEAVES_ONLY
        );

        foreach ($iterator as $fileInfo) {
            if (!$fileInfo->isFile() || !$fileInfo->isReadable()) {
                continue;
            }

            $name = $fileInfo->getFilename();

            if (strtoupper($name) === 'DICOMDIR'
                || substr($name, -4) === '.tsv'
                || substr($name, -5) === '.json'
                || substr($name, -4) === '.csv'
            ) {
                continue;
            }

            $path = $fileInfo->getPathname();

            if ($this->scanMode === 'sample') {
                $dir = dirname($path);

                if (isset($seenDirs[$dir])) {
                    continue;
                }

                $seenDirs[$dir] = true;
            }

            $files[] = $path;
        }

        sort($files);

        return $files;
    }

    /**
     * Run dcmdump, printing only the columns we extract.
     *
     * @return string[]|null Null when dcmdump failed.
     */
    private function dump(string $file): ?array
    {
        $filter = '';

        foreach (self::COLUMNS as [$group, $element, $_label]) {
            $filter .= ' +P ' . escapeshellarg(strtolower($group) . ',' . strtolower($element));
        }

        $cmd = sprintf(
            '%s --quiet --print-short%s %s 2>/dev/null',
            escapeshellarg($this->dcmdumpPath),
            $filter,
            escapeshellarg($file)
        );

        $output   = [];
        $exitCode = 0;
        exec($cmd, $output, $exitCode);

        return $exitCode === 0 ? $output : null;
    }

    /**
     * Map a dump to column label => value.
     *
     * dcmdump lines look like:
     *   (0008,0080) LO [Some Hospital]   #  14, 1 InstitutionName
     *
     * @return array<string, string>
     */
    private function parseDump(array $dump): array
    {
        $byTag = [];

        foreach ($dump as $line) {
            if (!preg_match(
                '/^\s*\(([0-9a-fA-F]{4}),([0-9a-fA-F]{4})\)\s+\w\w\s+(.*)$/',
                $line,
                $m
            )) {
                continue;
            }

            $key   = strtolower($m[1]) . ',' . strtolower($m[2]);
            $value = '';

            if (preg_match('/\[([^\]]*)\]/', $m[3], $vm)) {
                $value = trim($vm[1]);
            }

            $byTag[$key] = $value;
        }

        $values = [];

        foreach (self::COLUMNS as [$group, $element, $label]) {
            $key           = strtolower($group) . ',' . strtolower($element);
            $values[$label] = $byTag[$key] ?? '';
        }

        return $values;
    }

    // =========================================================================
    //  OUTPUT
    // =========================================================================

    /**
     * Write the CSV: header row, then one row per study.
     *
     * Excluded QI columns are still written - excluding a column from the QI
     * set is not the same as withholding it, and EviData needs to see the
     * data to score it. Drop a column entirely by removing it from COLUMNS.
     */
    private function write(string $path): void
    {
        $dir = dirname($path);

        if (!is_dir($dir) && !@mkdir($dir, 0775, true) && !is_dir($dir)) {
            throw new RuntimeException("Could not create {$dir}");
        }

        $handle = fopen($path, 'w');

        if ($handle === false) {
            throw new RuntimeException("Could not write {$path}");
        }

        fputcsv($handle, $this->columns);

        foreach ($this->rows as $row) {
            fputcsv($handle, array_map(
                static fn (string $c): string => (string) ($row[$c] ?? ''),
                $this->columns
            ));
        }

        fclose($handle);
    }

    private function info(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->info($message);
        }
    }
}
