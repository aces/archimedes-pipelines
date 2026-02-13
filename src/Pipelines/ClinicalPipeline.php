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
            $this->logger->info("MODE: DRY RUN - No uploads will be performed");
        }

        if ($this->verbose) {
            $this->logger->info("MODE: VERBOSE - Debug logging enabled");
        }

        // Log filters if any
        if (!empty($filters)) {
            $filterStr = [];
            if (isset($filters['collection'])) {
                $filterStr[] = "collection={$filters['collection']}";
            }
            if (isset($filters['project'])) {
                $filterStr[] = "project={$filters['project']}";
            }
            $this->logger->info("Filters: " . implode(', ', $filterStr));
        }

        try {
            // Authenticate via loris-php-api-client
            $this->client->authenticate();

            // Discover projects
            $projects = $this->discoverProjects($filters);

            if (empty($projects)) {
                $this->logger->warning("No projects found to process");
                $this->logger->info("  Check your filters or project configuration");
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
            $this->logger->error("FATAL ERROR: " . $e->getMessage());
            $this->logger->debug("Stack trace: " . $e->getTraceAsString());
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

        $this->logger->debug("Discovering projects from " . count($collections) . " collection(s)");

        foreach ($collections as $collection) {
            if (!($collection['enabled'] ?? true)) {
                $this->logger->debug("  Skipping disabled collection: {$collection['name']}");
                continue;
            }

            if (isset($filters['collection']) && $collection['name'] !== $filters['collection']) {
                continue;
            }

            $this->logger->debug("  Scanning collection: {$collection['name']}");

            foreach ($collection['projects'] ?? [] as $projectConfig) {
                if (!($projectConfig['enabled'] ?? true)) {
                    $this->logger->debug("    Skipping disabled project: {$projectConfig['name']}");
                    continue;
                }

                if (isset($filters['project']) && $projectConfig['name'] !== $filters['project']) {
                    continue;
                }

                // Load project.json
                $projectPath = $collection['base_path'] . '/' . $projectConfig['name'];
                $projectJsonPath = $projectPath . '/project.json';

                if (!file_exists($projectJsonPath)) {
                    $this->logger->warning("    Project config not found: {$projectJsonPath}");
                    continue;
                }

                $projectData = json_decode(file_get_contents($projectJsonPath), true);
                if (json_last_error() !== JSON_ERROR_NONE) {
                    $this->logger->warning("    Invalid JSON in: {$projectJsonPath}");
                    continue;
                }

                $projectData['_collection'] = $collection['name'];
                $projectData['_projectPath'] = $projectPath;
                $projects[] = $projectData;

                $this->logger->debug("    ✓ Loaded: {$projectConfig['name']}");
            }
        }

        return $projects;
    }

    /**
     * Process single project
     */
    private function processProject(array $project): void
    {
        $projectName = $project['project_common_name'] ?? 'Unknown';

        $this->logger->info("\n========================================");
        $this->logger->info("Project: {$projectName}");
        $this->logger->info("========================================");

        // Get clinical data directory
        $clinicalDir = $project['data_access']['mount_path'] . '/deidentified-raw/clinical';

        $this->logger->debug("Clinical data directory: {$clinicalDir}");

        if (!is_dir($clinicalDir)) {
            $this->logger->warning("  ✗ Clinical directory not found: {$clinicalDir}");
            $this->logger->info("    Expected path: {$clinicalDir}");
            $this->logger->info("    Ensure deidentified data has been placed in the correct location");
            return;
        }

        // Get instruments from project.json
        $instruments = $project['clinical_instruments'] ?? [];

        if (empty($instruments)) {
            $this->logger->warning("  ✗ No clinical_instruments defined in project.json");
            $this->logger->info("    Add instruments to project.json under 'clinical_instruments' array");
            return;
        }

        $this->logger->info("Instruments defined in project.json: " . count($instruments));
        $this->logger->info("  → " . implode(', ', $instruments));

        // Setup project-specific logging
        if (isset($project['logging']['log_path'])) {
            $projectLogFile = $project['logging']['log_path'] . '/clinical.log';
            $this->logger->debug("Project log: {$projectLogFile}");
            $this->logger->pushHandler(new RotatingFileHandler($projectLogFile, 30));
        }

        // Process each instrument
        $projectStats = [
            'total'   => 0,
            'success' => 0,
            'failed'  => 0,
            'skipped' => 0,
            'instruments' => [
                'success' => [],
                'failed'  => [],
                'skipped' => []
            ],
            'reasons' => [
                'failed'  => [],
                'skipped' => []
            ]
        ];

        $this->logger->info("\nProcessing instruments:");
        $this->logger->info("----------------------------------------");

        foreach ($instruments as $instrument) {
            $result = $this->processInstrument($project, $instrument, $clinicalDir);

            // Result can be 'success', 'failed', 'skipped' or array with reason
            if (is_array($result)) {
                $status = $result['status'];
                $reason = $result['reason'] ?? '';
                $projectStats[$status]++;
                $projectStats['instruments'][$status][] = $instrument;
                if ($reason && isset($projectStats['reasons'][$status])) {
                    $projectStats['reasons'][$status][$instrument] = $reason;
                }
            } else {
                $projectStats[$result]++;
                $projectStats['instruments'][$result][] = $instrument;
            }
            $projectStats['total']++;
        }

        // Project summary
        $this->logger->info("\n----------------------------------------");
        $this->logger->info("Project '{$projectName}' completed:");
        $this->logger->info("  Total: {$projectStats['total']}, Success: {$projectStats['success']}, Failed: {$projectStats['failed']}, Skipped: {$projectStats['skipped']}");

        // Send notification
        $this->sendProjectNotification($project, $projectStats);
    }

    /**
     * Process single instrument CSV file
     */
    private function processInstrument(array $project, string $instrument, string $clinicalDir): string|array
    {
        $csvFile = "{$clinicalDir}/{$instrument}.csv";

        // ─────────────────────────────────────────────────────────────
        // CHECK 1: Does the CSV data file exist?
        // ─────────────────────────────────────────────────────────────
        if (!file_exists($csvFile)) {
            // Check if instrument definition exists in data_dictionary
            $ddPath = $project['data_access']['mount_path'] . '/documentation/data_dictionary';
            $linstFile = "{$ddPath}/{$instrument}.linst";
            $ddCsvFile = "{$ddPath}/{$instrument}.csv";

            $linstExists = file_exists($linstFile);
            $ddCsvExists = file_exists($ddCsvFile);

            if ($linstExists || $ddCsvExists) {
                // DD exists but no data to ingest - this is OK, just skip
                $ddType = $linstExists ? '.linst' : '.csv';
                $this->logger->info("\n  ⚠ {$instrument}:");
                $this->logger->info("      Status: SKIPPED - No data to ingest");
                $this->logger->info("      DD definition: ✓ Found ({$ddType})");
                $this->logger->info("      Data CSV: ✗ Not found in deidentified folder");
                $this->logger->debug("      Expected: {$csvFile}");

                $this->stats['skipped']++;
                return ['status' => 'skipped', 'reason' => 'no data'];
            } else {
                // Neither DD nor data exists - this is a FAILURE
                $this->logger->info("\n  ✗ {$instrument}:");
                $this->logger->info("      Status: FAILED - No DD found");
                $this->logger->info("      DD definition: ✗ Not found");
                $this->logger->info("      Data CSV: ✗ Not found");
                $this->logger->info("      Note: Defined in project.json but no files exist");
                $this->logger->debug("      DD path checked: {$ddPath}");
                $this->logger->debug("      Data path checked: {$csvFile}");

                $this->stats['failed']++;
                $this->stats['errors'][] = [
                    'instrument' => $instrument,
                    'file' => '',
                    'errors' => ["No data dictionary (.linst or .csv) found in {$ddPath}/"]
                ];
                return ['status' => 'failed', 'reason' => 'no DD found'];
            }
        }

        // ─────────────────────────────────────────────────────────────
        // CHECK 2: Has this file already been processed?
        // ─────────────────────────────────────────────────────────────
        $filename = "{$instrument}.csv";
        if ($this->isAlreadyProcessed($project, $filename, 'clinical')) {
            $this->logger->info("\n  ⚠ {$instrument}:");
            $this->logger->info("      Status: SKIPPED - Already processed");
            $this->logger->info("      File found in processed/ archive folder");
            $this->logger->debug("      Source: {$csvFile}");

            $this->stats['skipped']++;
            return ['status' => 'skipped', 'reason' => 'already processed'];
        }

        // ─────────────────────────────────────────────────────────────
        // FILE FOUND - Process it
        // ─────────────────────────────────────────────────────────────
        $this->logger->info("\n  → {$instrument}:");
        $this->stats['total']++;

        // Log file info
        $fileSize = filesize($csvFile);
        $fileSizeMB = round($fileSize / 1024 / 1024, 2);
        $fileSizeKB = round($fileSize / 1024, 2);
        $rowCount = $this->countCSVRows($csvFile);

        $sizeStr = $fileSizeMB >= 1 ? "{$fileSizeMB} MB" : "{$fileSizeKB} KB";
        $this->logger->info("      File: {$instrument}.csv ({$sizeStr}, {$rowCount} rows)");

        // ─────────────────────────────────────────────────────────────
        // CHECK 3: Validate CSV structure
        // ─────────────────────────────────────────────────────────────
        if (!$this->validateCSV($csvFile)) {
            $this->logger->error("      Status: FAILED - Invalid CSV format");
            $this->stats['failed']++;
            $this->stats['errors'][] = [
                'instrument' => $instrument,
                'file' => $csvFile,
                'errors' => ['Invalid CSV format - missing required columns (Visit_label)']
            ];
            return ['status' => 'failed', 'reason' => 'invalid CSV'];
        }

        $this->logger->debug("      CSV validation: ✓ Passed");

        // ─────────────────────────────────────────────────────────────
        // CHECK 4: Ensure instrument exists in LORIS (auto-install if needed)
        // ─────────────────────────────────────────────────────────────
        if (!$this->ensureInstrumentExists($project, $instrument)) {
            $this->logger->error("      Status: FAILED - Instrument not installed in LORIS");
            $this->stats['failed']++;
            return ['status' => 'failed', 'reason' => 'not in LORIS'];
        }

        // ─────────────────────────────────────────────────────────────
        // DRY RUN - Don't actually upload
        // ─────────────────────────────────────────────────────────────
        if ($this->dryRun) {
            $this->logger->info("      Status: DRY RUN - Would upload {$rowCount} row(s)");
            $this->stats['success']++;
            return ['status' => 'success', 'reason' => 'dry run'];
        }

        // ─────────────────────────────────────────────────────────────
        // UPLOAD via ClinicalClient
        // ─────────────────────────────────────────────────────────────
        try {
            $this->logger->info("      Uploading to LORIS (CREATE_SESSIONS mode)...");
            $startTime = microtime(true);

            $result = $this->client->uploadInstrumentCSV(
                $instrument,
                $csvFile,
                'CREATE_SESSIONS'
            );

            $duration = round(microtime(true) - $startTime, 2);

            if ($result['success'] ?? false) {
                $this->logger->info("      Status: SUCCESS ({$duration}s)");
                $this->stats['success']++;

                // Parse message for statistics
                $this->parseUploadResult($result, $instrument);

                // Archive the processed file
                $this->archiveFile($project, $csvFile, 'clinical');
                return ['status' => 'success', 'reason' => 'uploaded'];

            } else {
                $this->logger->error("      Status: FAILED ({$duration}s)");
                $this->stats['failed']++;
                $this->logUploadErrors($instrument, $csvFile, $result);
                return ['status' => 'failed', 'reason' => 'upload error'];
            }

        } catch (\Exception $e) {
            $this->logger->error("      Status: FAILED - Exception");
            $this->logger->error("      Error: " . $e->getMessage());
            $this->stats['failed']++;
            $this->stats['errors'][] = [
                'instrument' => $instrument,
                'file' => $csvFile,
                'errors' => [$e->getMessage()]
            ];
            return ['status' => 'failed', 'reason' => 'exception'];
        }
    }

    /**
     * Ensure instrument exists in LORIS, try to install if not
     */
    private function ensureInstrumentExists(array $project, string $instrument): bool
    {
        // Check if instrument exists
        if ($this->client->instrumentExists($instrument)) {
            $this->logger->debug("      LORIS instrument: ✓ Exists");
            return true;
        }

        $this->logger->warning("      LORIS instrument: ✗ Not found - attempting auto-install");

        // Try to find definition in data dictionary
        $ddPath = $project['data_access']['mount_path'] . '/documentation/data_dictionary';

        $linstFile = "{$ddPath}/{$instrument}.linst";
        $csvFile = "{$ddPath}/{$instrument}.csv";

        $installFile = null;
        $fileType = null;

        if (file_exists($linstFile)) {
            $installFile = $linstFile;
            $fileType = 'LINST';
            $this->logger->info("      DD found: {$instrument}.linst (LORIS format)");
        } elseif (file_exists($csvFile)) {
            $installFile = $csvFile;
            $fileType = 'REDCap CSV';
            $this->logger->info("      DD found: {$instrument}.csv (REDCap format)");
        }

        if ($installFile === null) {
            $this->logger->error("      ✗ Cannot auto-install: No instrument definition found");
            $this->logger->error("        Checked locations:");
            $this->logger->error("          - {$ddPath}/{$instrument}.linst");
            $this->logger->error("          - {$ddPath}/{$instrument}.csv");
            $this->logger->info("        Action required: Install '{$instrument}' manually in LORIS");
            $this->logger->info("        Or add definition file to data_dictionary folder");

            $this->stats['errors'][] = [
                'instrument' => $instrument,
                'file' => '',
                'errors' => [
                    "Instrument '{$instrument}' not installed in LORIS",
                    "No definition found in {$ddPath}/",
                    "Add {$instrument}.linst or {$instrument}.csv to data_dictionary, or install manually"
                ]
            ];
            return false;
        }

        // Dry run - don't actually install
        if ($this->dryRun) {
            $this->logger->info("      DRY RUN - Would install instrument from {$fileType}");
            return true;
        }

        // Try to install
        $this->logger->info("      Installing instrument from {$fileType}...");

        try {
            $success = $this->client->installInstrument($installFile);

            if ($success) {
                $this->logger->info("      ✓ Instrument '{$instrument}' installed successfully");
                $this->stats['instruments_installed']++;
                return true;
            } else {
                $this->logger->error("      ✗ Failed to install instrument");
                $this->stats['errors'][] = [
                    'instrument' => $instrument,
                    'file' => $installFile,
                    'errors' => ["Failed to install instrument from {$fileType}"]
                ];
                return false;
            }

        } catch (\Exception $e) {
            $this->logger->error("      ✗ Install exception: " . $e->getMessage());
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
    private function parseUploadResult(array $result, string $instrument): void
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

                $this->logger->info("      Rows: {$saved}/{$total} saved" . ($skipped > 0 ? " ({$skipped} already exist)" : ""));
            } else {
                $this->logger->info("      Result: " . $result['message']);
            }
        }

        // Log new candidates created
        if (isset($result['idMapping']) && !empty($result['idMapping']) && $saved > 0) {
            $newCandidates = count($result['idMapping']);
            $this->stats['candidates_created'] += $newCandidates;
            $this->logger->info("      New records created: {$newCandidates}");

            if ($this->verbose) {
                foreach ($result['idMapping'] as $mapping) {
                    $extId = $mapping['ExtStudyID'] ?? 'N/A';
                    $candId = $mapping['CandID'] ?? 'N/A';
                    $this->logger->debug("        StudyID {$extId} → CandID {$candId}");
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
                $this->logger->error("      Errors: {$errorCount} issue(s)");

                $errorEntry['errors'] = $result['message'];

                $maxErrors = 5;
                foreach (array_slice($result['message'], 0, $maxErrors) as $i => $error) {
                    if (is_array($error)) {
                        $msg = $error['message'] ?? json_encode($error);
                        $this->logger->error("        " . ($i + 1) . ". " . $msg);
                    } else {
                        $this->logger->error("        " . ($i + 1) . ". " . $error);
                    }
                }

                if ($errorCount > $maxErrors) {
                    $remaining = $errorCount - $maxErrors;
                    $this->logger->error("        ... and {$remaining} more error(s)");
                }
            } else {
                $errorEntry['errors'][] = $result['message'];
                $this->logger->error("      Error: " . $result['message']);
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
            fgetcsv($handle); // Skip header
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
            $this->logger->error("      CSV not readable: {$csvFile}");
            return false;
        }

        $handle = fopen($csvFile, 'r');
        $headers = fgetcsv($handle);
        fclose($handle);

        if (empty($headers)) {
            $this->logger->error("      CSV has no headers");
            return false;
        }

        $required = ['Visit_label'];
        $missing = [];

        foreach ($required as $col) {
            if (!in_array($col, $headers)) {
                $missing[] = $col;
            }
        }

        if (!empty($missing)) {
            $this->logger->error("      Missing required column(s): " . implode(', ', $missing));
            $this->logger->debug("      Found columns: " . implode(', ', $headers));
            return false;
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
                $this->logger->error("      ✗ Failed to create archive directory: {$archiveDir}");
                return false;
            }
            $this->logger->debug("      Created archive directory: {$archiveDir}");
        }

        $filename = basename($sourceFile);
        $destFile = "{$archiveDir}/{$filename}";

        if (file_exists($destFile)) {
            $uniqueName = time() . "_" . $filename;
            $destFile   = "{$archiveDir}/{$uniqueName}";
            $this->logger->warning("      Archive file exists, using: {$uniqueName}");
        }

        if (copy($sourceFile, $destFile)) {
            $this->logger->info("      Archived: {$filename} → processed/{$modality}/" . date('Y-m-d') . "/");
            return true;
        }

        $this->logger->error("      ✗ Failed to archive file");
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
            $this->logger->debug("  No notification emails configured for {$modality}");
            return;
        }

        $subject = $hasFailures
            ? "FAILED: $projectName Clinical Ingestion"
            : "SUCCESS: $projectName Clinical Ingestion";

        $body  = "Project: $projectName\n";
        $body .= "Modality: $modality\n";
        $body .= "Timestamp: " . date('Y-m-d H:i:s') . "\n\n";

        $body .= "Instruments Processed: {$projectStats['total']}\n";

        // Uploaded
        $body .= "  ✔ Uploaded: {$projectStats['success']}";
        if (!empty($projectStats['instruments']['success'])) {
            $body .= " (" . implode(', ', $projectStats['instruments']['success']) . ")";
        }
        $body .= "\n";

        // Failed - group by reason
        $body .= "  ✗ Failed: {$projectStats['failed']}";
        if (!empty($projectStats['instruments']['failed'])) {
            $failedByReason = [];
            foreach ($projectStats['instruments']['failed'] as $inst) {
                $reason = $projectStats['reasons']['failed'][$inst] ?? 'error';
                $failedByReason[$reason][] = $inst;
            }
            $failedParts = [];
            foreach ($failedByReason as $reason => $insts) {
                $failedParts[] = implode(', ', $insts) . " [{$reason}]";
            }
            $body .= " (" . implode('; ', $failedParts) . ")";
        }
        $body .= "\n";

        // Skipped - group by reason
        $body .= "  ⚠ Skipped: {$projectStats['skipped']}";
        if (!empty($projectStats['instruments']['skipped'])) {
            $skippedByReason = [];
            foreach ($projectStats['instruments']['skipped'] as $inst) {
                $reason = $projectStats['reasons']['skipped'][$inst] ?? 'unknown';
                $skippedByReason[$reason][] = $inst;
            }
            $skippedParts = [];
            foreach ($skippedByReason as $reason => $insts) {
                $skippedParts[] = implode(', ', $insts) . " [{$reason}]";
            }
            $body .= " (" . implode('; ', $skippedParts) . ")";
        }
        $body .= "\n\n";

        // Outlook / Status message
        if ($hasFailures) {
            $body .= "⚠ Some instruments failed to ingest.\n";
            $body .= "Check logs for details.\n";
        } elseif ($projectStats['success'] > 0) {
            $body .= "✔ Ingestion completed successfully.\n";
        } elseif ($projectStats['skipped'] > 0 && $projectStats['success'] === 0) {
            $body .= "✔ Ingestion completed. All instruments were skipped\n";
            $body .= "   (already processed or no new data available).\n";
        } else {
            $body .= "✔ Ingestion completed. No instruments to process.\n";
        }

        $this->logger->debug("  Sending notification to: " . implode(', ', $emailsToSend));

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
        $this->logger->info("PIPELINE SUMMARY");
        $this->logger->info("========================================");

        $this->logger->info("\nFiles:");
        $this->logger->info("  Total processed: {$this->stats['total']}");
        $this->logger->info("  Successfully uploaded: {$this->stats['success']}");
        $this->logger->info("  Failed: {$this->stats['failed']}");
        $this->logger->info("  Skipped: {$this->stats['skipped']}");

        if ($this->stats['instruments_installed'] > 0) {
            $this->logger->info("\nInstruments:");
            $this->logger->info("  Auto-installed: {$this->stats['instruments_installed']}");
        }

        if ($this->stats['rows_uploaded'] > 0 || $this->stats['rows_skipped'] > 0) {
            $this->logger->info("\nData Rows:");
            $this->logger->info("  Uploaded: {$this->stats['rows_uploaded']}");
            if ($this->stats['rows_skipped'] > 0) {
                $this->logger->info("  Skipped (already exist): {$this->stats['rows_skipped']}");
            }
            $totalRows = $this->stats['rows_uploaded'] + $this->stats['rows_skipped'];
            $this->logger->info("  Total processed: {$totalRows}");
        }

        if ($this->stats['candidates_created'] > 0) {
            $this->logger->info("\nNew Records:");
            $this->logger->info("  Created: {$this->stats['candidates_created']}");
        }

        if ($this->stats['total'] > 0) {
            $successRate = round(($this->stats['success'] / $this->stats['total']) * 100, 1);
            $this->logger->info("\nSuccess Rate: {$successRate}%");
        }

        if (!empty($this->stats['errors'])) {
            $this->logger->info("\n----------------------------------------");
            $this->logger->error("ERRORS ENCOUNTERED:");
            foreach ($this->stats['errors'] as $errorEntry) {
                $this->logger->error("\n  Instrument: {$errorEntry['instrument']}");
                if (!empty($errorEntry['file'])) {
                    $this->logger->error("  File: " . basename($errorEntry['file']));
                }
                if (is_array($errorEntry['errors'])) {
                    foreach (array_slice($errorEntry['errors'], 0, 3) as $error) {
                        if (is_array($error)) {
                            $msg = $error['message'] ?? json_encode($error);
                            $this->logger->error("    • {$msg}");
                        } else {
                            $this->logger->error("    • {$error}");
                        }
                    }
                    if (count($errorEntry['errors']) > 3) {
                        $remaining = count($errorEntry['errors']) - 3;
                        $this->logger->error("    ... and {$remaining} more");
                    }
                }
            }
        }

        $this->logger->info("\n========================================");
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
                $this->logger->debug("      Found in archive: {$folder}/{$filename}");
                return true;
            }
        }

        return false;
    }
}