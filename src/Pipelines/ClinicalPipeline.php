<?php
declare(strict_types=1);

namespace LORIS\Pipelines;

use LORIS\Endpoints\ClinicalClient;
use LORIS\Database\Database;
use LORIS\Utils\{Notification, CleanLogFormatter};
use Monolog\Logger;
use Monolog\Handler\{StreamHandler, RotatingFileHandler};
use Psr\Log\LoggerInterface;

/**
 * Clinical Data Ingestion Pipeline
 *
 * Uses ClinicalClient (loris-php-api-client) for:
 * - Authentication
 * - Instrument CSV upload
 * - Auto-install instruments from data dictionary
 */
class ClinicalPipeline
{
    private array $config;
    private LoggerInterface $logger;
    private ClinicalClient $client;
    private Database $db;
    private Notification $notification;
    private bool $dryRun;
    private bool $verbose;

    private array $stats = [
        'total' => 0,
        'success' => 0,
        'failed' => 0,
        'skipped' => 0,
        'rows_uploaded' => 0,
        'rows_skipped' => 0,
        'candidates_created' => 0,
        'instruments_installed' => 0,
        'errors' => []
    ];

    public function __construct(array $config, bool $dryRun = false, bool $verbose = false)
    {
        $this->config = $config;
        $this->dryRun = $dryRun;
        $this->verbose = $verbose;

        // Initialize logger
        $logLevel = $verbose ? Logger::DEBUG : Logger::INFO;
        $this->logger = new Logger('clinical');

        // Create formatter for clean output
        $formatter = new CleanLogFormatter();

        // Console handler
        $consoleHandler = new StreamHandler('php://stdout', $logLevel);
        $consoleHandler->setFormatter($formatter);
        $this->logger->pushHandler($consoleHandler);

        // Add file handler if log directory configured
        if (isset($config['logging']['log_dir'])) {
            $logFile = $config['logging']['log_dir'] . '/clinical_' . date('Y-m-d') . '.log';
            $fileHandler = new RotatingFileHandler($logFile, 30, Logger::DEBUG);
            $fileHandler->setFormatter($formatter);
            $this->logger->pushHandler($fileHandler);
        }

        // Use ClinicalClient (wraps loris-php-api-client)
        $this->client = new ClinicalClient(
            $config['api']['base_url'],
            $config['api']['username'],
            $config['api']['password'],
            $config['api']['token_expiry_minutes'] ?? 55,
            $this->logger
        );

        // Initialize Database
        $this->db = new Database($config, $this->logger);

        $this->notification = new Notification();
    }

    /**
     * Run clinical ingestion pipeline
     */
    public function run(array $filters = []): int
    {
        $this->logger->info("=== CLINICAL DATA INGESTION PIPELINE ===");
        $this->logger->info("Started: " . date('Y-m-d H:i:s'));

        if ($this->dryRun) {
            $this->logger->info("DRY RUN MODE - No uploads will be performed");
        }

        try {
            // Authenticate via loris-php-api-client
            $this->client->authenticate();

            // Discover projects
            $projects = $this->discoverProjects($filters);

            if (empty($projects)) {
                $this->logger->warning("No projects found to process");
                return 0;
            }

            $this->logger->info("Found " . count($projects) . " project(s) to process");

            // Process each project
            foreach ($projects as $project) {
                $this->processProject($project);
            }

            // Print summary
            $this->printSummary();
            $this->logger->info("Completed: " . date('Y-m-d H:i:s'));
            $this->logger->info("=== Complete ===");

            return $this->stats['failed'] > 0 ? 1 : 0;

        } catch (\Exception $e) {
            $this->logger->error("FATAL: " . $e->getMessage());
            return 1;
        }
    }

    /**
     * Discover projects from configuration
     */
    private function discoverProjects(array $filters): array
    {
        $projects = [];
        $collections = $this->config['collections'] ?? [];

        foreach ($collections as $collection) {
            if (!($collection['enabled'] ?? true)) {
                continue;
            }

            if (isset($filters['collection']) && $collection['name'] !== $filters['collection']) {
                continue;
            }

            foreach ($collection['projects'] ?? [] as $projectConfig) {
                if (!($projectConfig['enabled'] ?? true)) {
                    continue;
                }

                if (isset($filters['project']) && $projectConfig['name'] !== $filters['project']) {
                    continue;
                }

                // Load project.json
                $projectPath = $collection['base_path'] . '/' . $projectConfig['name'];
                $projectJsonPath = $projectPath . '/project.json';

                if (!file_exists($projectJsonPath)) {
                    $this->logger->warning("Project config not found: {$projectJsonPath}");
                    continue;
                }

                $projectData = json_decode(file_get_contents($projectJsonPath), true);
                if (json_last_error() !== JSON_ERROR_NONE) {
                    $this->logger->warning("Invalid JSON in: {$projectJsonPath}");
                    continue;
                }

                $projectData['_collection'] = $collection['name'];
                $projectData['_projectPath'] = $projectPath;
                $projects[] = $projectData;
            }
        }

        return $projects;
    }

    /**
     * Process single project
     */
    private function processProject(array $project): void
    {
        $this->logger->info("\n========================================");
        $this->logger->info("Project: {$project['project_common_name']}");

        // Get clinical data directory
        $clinicalDir = $project['data_access']['mount_path'] . '/deidentified-lorisid/clinical';

        if (!is_dir($clinicalDir)) {
            $this->logger->warning("Clinical directory not found: {$clinicalDir}");
            return;
        }

        // Get instruments
        $instruments = $project['clinical_instruments'] ?? [];

        if (empty($instruments)) {
            $this->logger->warning("No clinical instruments defined in project.json");
            return;
        }

        $this->logger->info("Instruments: " . implode(', ', $instruments));

        // Setup project-specific logging
        if (isset($project['logging']['log_path'])) {
            $projectLogFile = $project['logging']['log_path'] . '/clinical.log';
            $this->logger->pushHandler(new RotatingFileHandler($projectLogFile, 30));
        }

        // Process each instrument
        $projectStats = [
            'total'   => 0,
            'success' => 0,
            'failed'  => 0,
            'skipped' => 0
        ];

        foreach ($instruments as $instrument) {
            $result = $this->processInstrument($project, $instrument, $clinicalDir);

            $projectStats['total']++;
            $projectStats[$result]++;
        }

        // Send notification
        $this->sendProjectNotification($project, $projectStats);
    }

    /**
     * Process single instrument CSV file
     */
    private function processInstrument(array $project, string $instrument, string $clinicalDir): string
    {
        $csvFile = "{$clinicalDir}/{$instrument}.csv";

        if (!file_exists($csvFile)) {
            $this->logger->info("\n  ⚠ {$instrument}.csv not found (skipping)");
            return 'skipped';
        }

        $filename = "{$instrument}.csv";
        if ($this->isAlreadyProcessed($project, $filename, 'clinical')) {
            $this->logger->info("  ⚠ Already processed earlier → SKIPPING {$filename}");
            return 'skipped';
        }

        $this->logger->info("\n  Processing: {$instrument}");
        $this->stats['total']++;

        // Log file info
        $fileSize = filesize($csvFile);
        $fileSizeMB = round($fileSize / 1024 / 1024, 2);
        $rowCount = $this->countCSVRows($csvFile);
        $this->logger->info("    File: {$instrument}.csv ({$fileSizeMB} MB, {$rowCount} rows)");

        // Validate CSV
        if (!$this->validateCSV($csvFile)) {
            $this->logger->error("  ✗ Invalid CSV file");
            $this->stats['failed']++;
            $this->stats['errors'][] = [
                'instrument' => $instrument,
                'file' => $csvFile,
                'errors' => ['Invalid CSV format - missing required columns']
            ];
            return 'failed';
        }

        // Check instrument exists in LORIS, try to install if not
        if (!$this->ensureInstrumentExists($project, $instrument)) {
            $this->stats['failed']++;
            return 'failed';
        }

        // Dry run
        if ($this->dryRun) {
            $this->logger->info("  DRY RUN - Would upload {$rowCount} row(s)");
            $this->stats['success']++;
            return 'success';
        }

        // Upload via ClinicalClient
        try {
            $this->logger->info("  Uploading to LORIS via API (CREATE_SESSIONS mode)...");
            $startTime = microtime(true);

            $result = $this->client->uploadInstrumentCSV(
                $instrument,
                $csvFile,
                'CREATE_SESSIONS'
            );

            $duration = round(microtime(true) - $startTime, 2);
            $this->logger->info("  Upload completed in {$duration}s");

            if ($result['success'] ?? false) {
                $this->logger->info("  ✓ Upload successful");
                $this->stats['success']++;

                // Parse message for statistics
                $this->parseUploadResult($result);

                // Archive
                $this->archiveFile($project, $csvFile, 'clinical');
                return 'success';

            } else {
                $this->logger->error("  ✗ Upload failed");
                $this->stats['failed']++;

                $this->logUploadErrors($instrument, $csvFile, $result);
                return 'failed';
            }

        } catch (\Exception $e) {
            $this->logger->error("  ✗ Upload exception: " . $e->getMessage());
            $this->stats['failed']++;
            $this->stats['errors'][] = [
                'instrument' => $instrument,
                'file' => $csvFile,
                'errors' => [$e->getMessage()]
            ];
            return 'failed';
        }
    }

    /**
     * Ensure instrument exists in LORIS, try to install if not
     */
    private function ensureInstrumentExists(array $project, string $instrument): bool
    {
        // Check if instrument exists
        if ($this->client->instrumentExists($instrument)) {
            $this->logger->debug("    Instrument '{$instrument}' exists in LORIS");
            return true;
        }

        $this->logger->warning("  ⚠ Instrument '{$instrument}' not found in LORIS");

        // Try to find definition in data dictionary
        $ddPath = $project['data_access']['mount_path'] . '/documentation/data_dictionary';

        $linstFile = "{$ddPath}/{$instrument}.linst";
        $csvFile = "{$ddPath}/{$instrument}.csv";

        $installFile = null;
        $fileType = null;

        if (file_exists($linstFile)) {
            $installFile = $linstFile;
            $fileType = 'LINST';
            $this->logger->info("    Found data dictionary: {$instrument}.linst");
        } elseif (file_exists($csvFile)) {
            $installFile = $csvFile;
            $fileType = 'REDCap CSV';
            $this->logger->info("    Found data dictionary: {$instrument}.csv");
        }

        if ($installFile === null) {
            $this->logger->error("  ✗ Instrument definition not found in data dictionary");
            $this->logger->error("    Please install '{$instrument}' manually or add definition to:");
            $this->logger->error("      {$ddPath}/{$instrument}.linst (LORIS format)");
            $this->logger->error("      {$ddPath}/{$instrument}.csv (REDCap format)");

            $this->stats['errors'][] = [
                'instrument' => $instrument,
                'file' => '',
                'errors' => [
                    "Instrument '{$instrument}' not installed in LORIS",
                    "No definition found in {$ddPath}/",
                    "Please install manually or add {$instrument}.linst or {$instrument}.csv to data dictionary"
                ]
            ];
            return false;
        }

        // Try to install
        if ($this->dryRun) {
            $this->logger->info("    DRY RUN - Would install instrument from {$fileType}");
            return true;
        }

        $this->logger->info("    Installing instrument from {$fileType}...");

        try {
            $success = $this->client->installInstrument($installFile);

            if ($success) {
                $this->logger->info("    ✓ Instrument '{$instrument}' installed successfully");
                $this->stats['instruments_installed']++;
                return true;
            } else {
                $this->logger->error("    ✗ Failed to install instrument");
                $this->stats['errors'][] = [
                    'instrument' => $instrument,
                    'file' => $installFile,
                    'errors' => ["Failed to install instrument from {$fileType}"]
                ];
                return false;
            }

        } catch (\Exception $e) {
            $this->logger->error("    ✗ Install exception: " . $e->getMessage());
            $this->stats['errors'][] = [
                'instrument' => $instrument,
                'file' => $installFile,
                'errors' => ["Install failed: " . $e->getMessage()]
            ];
            return false;
        }
    }

    /**
     * Parse upload result and update stats
     */
    private function parseUploadResult(array $result): void
    {
        $saved = 0;
        $total = 0;

        if (isset($result['message']) && is_string($result['message'])) {
            if (preg_match('/Saved (\d+) out of (\d+)/', $result['message'], $matches)) {
                $saved = (int)$matches[1];
                $total = (int)$matches[2];
                $skipped = $total - $saved;

                $this->stats['rows_uploaded'] += $saved;
                $this->stats['rows_skipped'] += $skipped;

                $this->logger->info("    Rows saved: {$saved}/{$total}");
                if ($skipped > 0) {
                    $this->logger->info("    Rows skipped: {$skipped} (already exist)");
                }
            } else {
                $this->logger->info("    " . $result['message']);
            }
        }

        // Log new candidates created
        if (isset($result['idMapping']) && !empty($result['idMapping']) && $saved > 0) {
            $newCandidates = count($result['idMapping']);
            $this->stats['candidates_created'] += $newCandidates;
            $this->logger->info("    New Records created: {$newCandidates}");

            if ($this->verbose) {
                foreach ($result['idMapping'] as $mapping) {
                    $extId = $mapping['ExtStudyID'] ?? 'N/A';
                    $candId = $mapping['CandID'] ?? 'N/A';
                    $this->logger->debug("      StudyID {$extId} → CandID {$candId}");
                }
            }
        }
    }

    /**
     * Log upload errors
     */
    private function logUploadErrors(string $instrument, string $csvFile, array $result): void
    {
        $errorEntry = [
            'instrument' => $instrument,
            'file' => $csvFile,
            'errors' => []
        ];

        if (isset($result['message'])) {
            if (is_array($result['message'])) {
                $errorCount = count($result['message']);
                $this->logger->error("    Errors: {$errorCount}");

                $errorEntry['errors'] = $result['message'];

                $maxErrors = 5;
                foreach (array_slice($result['message'], 0, $maxErrors) as $i => $error) {
                    if (is_array($error)) {
                        $msg = $error['message'] ?? json_encode($error);
                        $this->logger->error("      " . ($i + 1) . ". " . $msg);
                    } else {
                        $this->logger->error("      " . ($i + 1) . ". " . $error);
                    }
                }

                if ($errorCount > $maxErrors) {
                    $remaining = $errorCount - $maxErrors;
                    $this->logger->error("      ... and {$remaining} more error(s)");
                }
            } else {
                $errorEntry['errors'][] = $result['message'];
                $this->logger->error("    " . $result['message']);
            }
        }

        $this->stats['errors'][] = $errorEntry;
    }

    /**
     * Count rows in CSV file (excluding header)
     */
    private function countCSVRows(string $csvFile): int
    {
        $count = 0;
        $handle = fopen($csvFile, 'r');
        if ($handle) {
            fgetcsv($handle);
            while (fgetcsv($handle) !== false) {
                $count++;
            }
            fclose($handle);
        }
        return $count;
    }

    /**
     * Validate CSV file
     */
    private function validateCSV(string $csvFile): bool
    {
        if (!is_readable($csvFile)) {
            return false;
        }

        $handle = fopen($csvFile, 'r');
        $headers = fgetcsv($handle);
        fclose($handle);

        $required = ['Visit_label'];
        foreach ($required as $col) {
            if (!in_array($col, $headers)) {
                $this->logger->error("  Missing required column: {$col}");
                return false;
            }
        }

        return true;
    }

    /**
     * Archive processed file
     */
    private function archiveFile(array $project, string $sourceFile, string $modality): bool
    {
        $archiveDir = rtrim($project['data_access']['mount_path'], '/')
            . "/processed/{$modality}/"
            . date('Y-m-d');

        if (!is_dir($archiveDir)) {
            if (!mkdir($archiveDir, 0755, true)) {
                $this->logger->error("  ✗ Failed to create archive directory: {$archiveDir}");
                return false;
            }
        }

        $filename = basename($sourceFile);
        $destFile = "{$archiveDir}/{$filename}";

        if (file_exists($destFile)) {
            $uniqueName = time() . "_" . $filename;
            $destFile   = "{$archiveDir}/{$uniqueName}";
            $this->logger->warning("  ⚠ File exists, copying as: {$uniqueName}");
        }

        if (copy($sourceFile, $destFile)) {
            $this->logger->info("  ✓ Copied to archive: {$destFile}");
            return true;
        }

        $this->logger->error("  ✗ Failed to copy file: {$sourceFile} → {$destFile}");
        return false;
    }

    /**
     * Send project notification for clinical modality
     */
    private function sendProjectNotification(array $project, array $projectStats): void
    {
        $modality = 'clinical';
        $projectName = $project['project_common_name'];

        $hasFailures = ($projectStats['failed'] ?? 0) > 0;

        $successEmails = $project['notification_emails'][$modality]['on_success'] ?? [];
        $errorEmails   = $project['notification_emails'][$modality]['on_error'] ?? [];

        $emailsToSend = $hasFailures ? $errorEmails : $successEmails;

        if (empty($emailsToSend)) {
            return;
        }

        $subject = $hasFailures
            ? "FAILED: $projectName Clinical Ingestion"
            : "SUCCESS: $projectName Clinical Ingestion";

        $body  = "Project: $projectName\n";
        $body .= "Modality: $modality\n\n";
        $body .= "Files Processed: {$projectStats['total']}\n";
        $body .= "Successfully Uploaded: {$projectStats['success']}\n";
        $body .= "Failed: {$projectStats['failed']}\n";
        $body .= "Skipped: {$projectStats['skipped']}\n\n";

        if ($hasFailures) {
            $body .= "⚠ Some files failed to ingest.\n";
            $body .= "Check logs for details.\n";
        } else {
            $body .= "✔ Ingestion completed successfully.\n";
        }

        foreach ($emailsToSend as $emailTo) {
            $this->notification->send($emailTo, $subject, $body);
        }
    }

    /**
     * Print summary
     */
    private function printSummary(): void
    {
        $this->logger->info("\n========================================");
        $this->logger->info("Pipeline Summary:");
        $this->logger->info("----------------------------------------");
        $this->logger->info("Files:");
        $this->logger->info("  Total processed: {$this->stats['total']}");
        $this->logger->info("  Successfully uploaded: {$this->stats['success']}");
        $this->logger->info("  Failed: {$this->stats['failed']}");
        $this->logger->info("  Skipped: {$this->stats['skipped']}");

        if ($this->stats['instruments_installed'] > 0) {
            $this->logger->info("----------------------------------------");
            $this->logger->info("Instruments auto-installed: {$this->stats['instruments_installed']}");
        }

        if ($this->stats['rows_uploaded'] > 0 || $this->stats['rows_skipped'] > 0) {
            $this->logger->info("----------------------------------------");
            $this->logger->info("Data Rows:");
            $this->logger->info("  Uploaded: {$this->stats['rows_uploaded']}");
            if ($this->stats['rows_skipped'] > 0) {
                $this->logger->info("  Skipped (already exist): {$this->stats['rows_skipped']}");
            }
            $totalRows = $this->stats['rows_uploaded'] + $this->stats['rows_skipped'];
            $this->logger->info("  Total processed: {$totalRows}");
        }

        if ($this->stats['candidates_created'] > 0) {
            $this->logger->info("----------------------------------------");
            $this->logger->info("New Records Created: {$this->stats['candidates_created']}");
        }

        if ($this->stats['total'] > 0) {
            $successRate = round(($this->stats['success'] / $this->stats['total']) * 100, 1);
            $this->logger->info("----------------------------------------");
            $this->logger->info("Success Rate: {$successRate}%");
        }

        if (!empty($this->stats['errors'])) {
            $this->logger->info("----------------------------------------");
            $this->logger->error("Errors Encountered:");
            foreach ($this->stats['errors'] as $errorEntry) {
                $this->logger->error("  Instrument: {$errorEntry['instrument']}");
                if (is_array($errorEntry['errors'])) {
                    foreach (array_slice($errorEntry['errors'], 0, 3) as $error) {
                        if (is_array($error)) {
                            $msg = $error['message'] ?? json_encode($error);
                            $this->logger->error("    - {$msg}");
                        } else {
                            $this->logger->error("    - {$error}");
                        }
                    }
                    if (count($errorEntry['errors']) > 3) {
                        $remaining = count($errorEntry['errors']) - 3;
                        $this->logger->error("    ... and {$remaining} more");
                    }
                }
            }
        }

        $this->logger->info("========================================");
    }

    /**
     * Check if file already exists in ANY processed/<modality>/<date> folder
     */
    private function isAlreadyProcessed(array $project, string $filename, string $modality): bool
    {
        $base = rtrim($project['data_access']['mount_path'], '/') . "/processed/{$modality}";

        if (!is_dir($base)) {
            return false;
        }

        $folders = glob($base . "/*", GLOB_ONLYDIR);

        foreach ($folders as $folder) {
            if (file_exists($folder . '/' . $filename)) {
                return true;
            }
        }

        return false;
    }
}