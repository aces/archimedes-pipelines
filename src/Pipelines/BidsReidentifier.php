<?php

declare(strict_types=1);

namespace LORIS\Pipelines;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;
use Psr\Log\LoggerInterface;
use GuzzleHttp\Client as GuzzleClient;

/**
 * BIDS Reidentifier
 *
 * Re-labels BIDS data from external IDs to LORIS PSCIDs by:
 *   1. Querying CBIGR mapper for ExternalID → PSCID mappings
 *   2. Renaming sub-{ExternalID}/ → sub-{PSCID}/
 *   3. Updating participants.tsv with PSCIDs
 *   4. Copying data to target directory
 *
 * Uses CBIGR API endpoint if available, falls back to direct HTTP.
 *
 * @package LORIS\Pipelines
 */
class BidsReidentifier
{
    private LoggerInterface $logger;
    private array           $config;
    private ?string         $token = null;
    private GuzzleClient    $httpClient;
    private ?array          $projectConfig = null;  // From project.json

    private array $stats = [
        'subjects_found'    => 0,
        'subjects_mapped'   => 0,
        'subjects_unmapped' => 0,
        'files_copied'      => 0,
        'errors'            => [],
        'unmapped_subjects' => [],
    ];

    private array $idMapping = [];  // ExternalID => PSCID

    public function __construct(array $config, bool $verbose = false)
    {
        $this->config = $config;
        $this->logger = $this->initLogger($verbose);
        $this->httpClient = new GuzzleClient([
            'verify'  => false,
            'timeout' => 30,
        ]);
    }

    private function initLogger(bool $verbose): Logger
    {
        $logger = new Logger('bids-reidentifier');
        $formatter = new LineFormatter(
            "[%datetime%] %level_name%: %message%\n",
            'Y-m-d H:i:s'
        );

        // Console handler (always enabled)
        $level = $verbose ? Logger::DEBUG : Logger::INFO;
        $console = new StreamHandler('php://stdout', $level);
        $console->setFormatter($formatter);
        $logger->pushHandler($console);

        // File handler will be added later after project.json is loaded
        return $logger;
    }

    /**
     * Setup file logging after project.json is loaded.
     * Creates date-stamped log files: bids_reidentifier_2026-02-06.log
     * 
     * Priority:
     *   1. project.json -> logging.log_path
     *   2. config -> logging.directory  
     *   3. default -> /tmp/archimedes-logs
     */
    private function setupFileLogging(): void
    {
        // Determine log directory
        $logDir = null;
        
        // Priority 1: project.json
        if ($this->projectConfig && isset($this->projectConfig['logging']['log_path'])) {
            $logDir = $this->projectConfig['logging']['log_path'];
            $this->logger->debug("  Using log directory from project.json: {$logDir}");
        }
        
        // Priority 2: config
        if (!$logDir && isset($this->config['logging']['directory'])) {
            $logDir = $this->config['logging']['directory'];
            $this->logger->debug("  Using log directory from config: {$logDir}");
        }
        
        // Priority 3: default
        if (!$logDir) {
            $logDir = '/tmp/archimedes-logs';
            $this->logger->debug("  Using default log directory: {$logDir}");
        }
        
        // Create directory if needed
        if (!is_dir($logDir)) {
            if (!@mkdir($logDir, 0755, true)) {
                $this->logger->warning("  Failed to create log directory: {$logDir}, using /tmp");
                $logDir = '/tmp/archimedes-logs';
                if (!is_dir($logDir)) {
                    @mkdir($logDir, 0755, true);
                }
            }
        }
        
        // Create date-stamped log file: bids_reidentifier_2026-02-06.log
        $dateStamp = date('Y-m-d');
        $logFile = "{$logDir}/bids_reidentifier_{$dateStamp}.log";
        
        $formatter = new LineFormatter(
            "[%datetime%] %level_name%: %message%\n",
            'Y-m-d H:i:s'
        );
        
        // Append to today's log file
        $file = new StreamHandler($logFile, Logger::DEBUG);
        $file->setFormatter($formatter);
        $this->logger->pushHandler($file);
        
        $this->logger->info("  Log file: {$logFile}");
    }

    /**
     * Main reidentification workflow
     */
    public function run(string $sourceDir, string $targetDir, array $options = []): array
    {
        $dryRun = $options['dry_run'] ?? false;
        $force = $options['force'] ?? false;
        $projectName = $options['project'] ?? null;

        $this->logger->info("═══════════════════════════════════════════════════════════");
        $this->logger->info("  BIDS Reidentifier");
        $this->logger->info("═══════════════════════════════════════════════════════════");
        $this->logger->info("  Source: {$sourceDir}");
        $this->logger->info("  Target: {$targetDir}");
        $this->logger->info("  Dry run: " . ($dryRun ? 'YES' : 'NO'));
        $this->logger->info("───────────────────────────────────────────────────────────");

        // Validate source
        if (!$this->validateBidsDirectory($sourceDir)) {
            $this->stats['errors'][] = "Invalid BIDS directory: {$sourceDir}";
            return $this->stats;
        }

        // Load project.json from parent directories
        $this->projectConfig = $this->loadProjectConfig($sourceDir);
        
        // Setup file logging now that project.json is loaded
        $this->setupFileLogging();
        
        // Get project name from: CLI arg > project.json > error
        if (!$projectName && $this->projectConfig) {
            $projectName = $this->projectConfig['project_full_name'] 
                        ?? $this->projectConfig['project'] 
                        ?? null;
        }
        
        if (!$projectName) {
            $this->stats['errors'][] = "Project name required (use --project or add to project.json)";
            return $this->stats;
        }

        $this->logger->info("  Project: {$projectName}");

        // Authenticate with LORIS
        $this->authenticate();

        // Build ID pattern from participants.tsv
        $idPattern = $this->extractIdPattern($sourceDir);
        if (!$idPattern) {
            $this->stats['errors'][] = "Could not determine ID pattern from participants.tsv";
            return $this->stats;
        }

        $this->logger->info("  ID Pattern: {$idPattern}");

        if ($dryRun) {
            $this->logger->info("[DRY-RUN] Would execute BIDS reidentification:");
            $this->logger->info("  API Endpoint: /cbigr_api/script/bidsreidentifier");
            $this->logger->info("  Mode: INTERNAL (ExternalID → PSCID)");
            $this->logger->info("  Project: '{$projectName}'");
            return $this->stats;
        }

        // Execute via CBIGR Script API
        $result = $this->executeReidentificationAPI(
            $sourceDir,
            $targetDir,
            $idPattern,
            $projectName
        );

        if ($result['success']) {
            $this->logger->info("✓ Reidentification completed successfully");
            $this->logger->info("  Output: {$targetDir}");
            $this->parseScriptOutput($result['output']);
        } else {
            $this->logger->error("✗ Reidentification failed");
            $this->stats['errors'][] = $result['error'] ?? 'Unknown error';
            
            if (!empty($result['output'])) {
                $this->logger->debug("Script output:");
                $this->logger->debug($result['output']);
            }
        }

        return $this->stats;
    }

    /**
     * Authenticate with LORIS
     */
    private function authenticate(): void
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $version = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';
        $username = $this->config['loris']['username'];
        $password = $this->config['loris']['password'];

        $this->logger->debug("Authenticating with LORIS at {$baseUrl}");

        try {
            $response = $this->httpClient->request('POST', "{$baseUrl}/api/{$version}/login", [
                'json' => [
                    'username' => $username,
                    'password' => $password,
                ],
            ]);

            $data = json_decode((string)$response->getBody(), true);
            $this->token = $data['token'] ?? null;

            if (!$this->token) {
                throw new \RuntimeException("No token in login response");
            }

            $this->logger->debug("  ✓ Authenticated successfully");
        } catch (\Exception $e) {
            throw new \RuntimeException("LORIS authentication failed: " . $e->getMessage());
        }
    }

    /**
     * Load project.json from BIDS directory or parent directories
     */
    private function loadProjectConfig(string $bidsDir): ?array
    {
        $searchPaths = [
            rtrim($bidsDir, '/') . '/project.json',                    // BIDS directory
            dirname(rtrim($bidsDir, '/')) . '/project.json',           // Parent (deidentified-raw)
            dirname(dirname(rtrim($bidsDir, '/'))) . '/project.json',  // Grandparent (project root)
        ];

        foreach ($searchPaths as $path) {
            if (file_exists($path)) {
                $content = file_get_contents($path);
                if ($content === false) {
                    continue;
                }

                $data = json_decode($content, true);
                if (json_last_error() === JSON_ERROR_NONE) {
                    $this->logger->debug("  Loaded project config: {$path}");
                    return $data;
                }
            }
        }

        $this->logger->debug("  No project.json found");
        return null;
    }

    /**
     * Extract ID pattern from participants.tsv external_id column
     * Generates regex pattern like "FDGP\d{6}" from sample IDs
     */
    private function extractIdPattern(string $bidsDir): ?string
    {
        $participants = $this->parseParticipantsTsv($bidsDir);
        if (empty($participants)) {
            return null;
        }

        // Get first external ID as sample
        $sampleId = $participants[0]['external_id'] ?? null;
        if (!$sampleId) {
            return null;
        }

        // Extract pattern: letters followed by digits
        // Example: "FDGP0001" → "FDGP\d{4}"
        //          "MRAC-NORM-003" → "MRAC-NORM-\d{3}"
        
        // Replace sequences of digits with \d{N}
        $pattern = preg_replace_callback('/\d+/', function($matches) {
            $length = strlen($matches[0]);
            return "\\d{{$length}}";
        }, $sampleId);

        $this->logger->debug("  Generated ID pattern from '{$sampleId}': {$pattern}");
        
        return $pattern;
    }

    /**
     * Execute BIDS reidentification via CBIGR Script API
     */
    private function executeReidentificationAPI(
        string $sourceDir,
        string $targetDir,
        string $idPattern,
        string $projectName
    ): array {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url = "{$baseUrl}/cbigr_api/script/bidsreidentifier";

        $this->logger->info("Executing BIDS reidentification via Script API...");

        // Format project_list as SQL string: "'ProjectName'"
        $projectList = "'{$projectName}'";

        $requestData = [
            'args' => [
                'source_dir' => $sourceDir,
                'target_dir' => $targetDir,
                'id_pattern' => $idPattern,
                'mode' => 'INTERNAL',  // ExternalID → PSCID
                'project_list' => $projectList,
            ],
        ];

        $this->logger->debug("Request payload:");
        $this->logger->debug(json_encode($requestData, JSON_PRETTY_PRINT));

        try {
            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => $requestData,
                'timeout' => 300,  // 5 minutes timeout for large datasets
            ]);

            $statusCode = $response->getStatusCode();
            $body = (string)$response->getBody();

            if ($statusCode === 200) {
                // Sync mode - script completed
                $data = json_decode($body, true);
                
                if ($data && isset($data['status']) && $data['status'] === 'success') {
                    return [
                        'success' => true,
                        'output' => $data['output'] ?? '',
                        'warnings' => $data['warnings'] ?? [],
                    ];
                }

                // Check for error status
                if ($data && isset($data['status']) && $data['status'] === 'error') {
                    return [
                        'success' => false,
                        'error' => implode("\n", $data['errors'] ?? [$data['error'] ?? 'Unknown error']),
                        'output' => $data['output'] ?? '',
                    ];
                }

                return [
                    'success' => false,
                    'error' => 'Unexpected response format',
                    'output' => $body,
                ];
            }

            if ($statusCode === 202) {
                // Async mode - script started
                $data = json_decode($body, true);
                $this->logger->info("Script started asynchronously");
                $this->logger->info("  Job ID: " . ($data['job_id'] ?? 'N/A'));
                $this->logger->info("  Log: " . ($data['log_file'] ?? 'N/A'));
                
                return [
                    'success' => true,
                    'async' => true,
                    'job_id' => $data['job_id'] ?? null,
                    'log_file' => $data['log_file'] ?? null,
                ];
            }

            return [
                'success' => false,
                'error' => "HTTP {$statusCode}: {$body}",
                'output' => $body,
            ];

        } catch (\Exception $e) {
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'output' => '',
            ];
        }
    }

    /**
     * Parse script output to extract stats
     */
    private function parseScriptOutput(string $output): void
    {
        // Extract mapped subjects count
        if (preg_match('/(\d+)\s+subjects?\s+mapped/i', $output, $matches)) {
            $this->stats['subjects_mapped'] = (int)$matches[1];
            $this->logger->info("  Subjects mapped: {$this->stats['subjects_mapped']}");
        }

        // Extract unmapped subjects count
        if (preg_match('/(\d+)\s+subjects?\s+unmapped/i', $output, $matches)) {
            $this->stats['subjects_unmapped'] = (int)$matches[1];
            if ($this->stats['subjects_unmapped'] > 0) {
                $this->logger->warning("  Subjects unmapped: {$this->stats['subjects_unmapped']}");
            }
        }

        // Extract files copied count
        if (preg_match('/(\d+)\s+files?\s+copied/i', $output, $matches)) {
            $this->stats['files_copied'] = (int)$matches[1];
            $this->logger->info("  Files copied: {$this->stats['files_copied']}");
        }

        // Check for warnings
        if (preg_match_all('/WARNING:?\s*(.+)/i', $output, $matches)) {
            foreach ($matches[1] as $warning) {
                $this->logger->warning("  " . trim($warning));
            }
        }

        // Check for errors
        if (preg_match_all('/ERROR:?\s*(.+)/i', $output, $matches)) {
            foreach ($matches[1] as $error) {
                $errorMsg = trim($error);
                $this->stats['errors'][] = $errorMsg;
                $this->logger->error("  " . $errorMsg);
            }
        }
    }

    /**
     * Validate BIDS directory structure
     */
    private function validateBidsDirectory(string $dir): bool
    {
        // Check for participants.tsv
        if (!file_exists("{$dir}/participants.tsv")) {
            $this->logger->error("Missing participants.tsv in {$dir}");
            return false;
        }

        // Check for at least one sub-* directory
        $subjects = glob("{$dir}/sub-*", GLOB_ONLYDIR);
        if (empty($subjects)) {
            $this->logger->error("No sub-* directories found in {$dir}");
            return false;
        }

        return true;
    }

    /**
     * Parse participants.tsv
     */
    private function parseParticipantsTsv(string $bidsDir): array
    {
        $tsvFile = "{$bidsDir}/participants.tsv";
        $participants = [];

        $handle = fopen($tsvFile, 'r');
        if ($handle === false) {
            $this->logger->error("Cannot open {$tsvFile}");
            return [];
        }

        // Read header
        $headerLine = fgets($handle);
        if ($headerLine === false) {
            fclose($handle);
            return [];
        }
        $headers = array_map('trim', explode("\t", $headerLine));

        // Read rows
        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if ($line === '') continue;

            $fields = array_map('trim', explode("\t", $line));
            $row = [];
            foreach ($headers as $i => $header) {
                $row[$header] = $fields[$i] ?? '';
            }

            $participantId = $row['participant_id'] ?? '';
            $externalId = $this->extractExternalID($row);

            if ($participantId && $externalId) {
                $participants[] = [
                    'participant_id' => $participantId,
                    'external_id' => $externalId,
                    'row_data' => $row,
                ];
                $this->stats['subjects_found']++;
            }
        }

        fclose($handle);
        return $participants;
    }

    /**
     * Extract ExternalID from participant row
     */
    private function extractExternalID(array $row): ?string
    {
        $idCols = [
            'external_id', 'ExternalID', 'externalid',
            'study_id', 'StudyID', 'studyid',
            'ext_study_id', 'ExtStudyID',
        ];

        foreach ($idCols as $col) {
            if (!empty($row[$col])) {
                return trim($row[$col]);
            }
        }

        return null;
    }

    /**
     * Query CBIGR mapper for all ExternalID → PSCID mappings
     */
    private function buildIdMappings(array $participants): void
    {
        $externalIds = array_column($participants, 'external_id');

        $this->logger->info("Querying CBIGR mapper for {$this->stats['subjects_found']} subjects...");

        // Try batch query first (more efficient)
        $batchMappings = $this->queryMapperBatch($externalIds);

        if (!empty($batchMappings)) {
            $this->idMapping = $batchMappings;
            $this->stats['subjects_mapped'] = count($batchMappings);
            $this->stats['subjects_unmapped'] = $this->stats['subjects_found'] - $this->stats['subjects_mapped'];

            // Track unmapped subjects
            foreach ($participants as $p) {
                if (!isset($this->idMapping[$p['external_id']])) {
                    $this->stats['unmapped_subjects'][] = $p['participant_id'];
                }
            }
            return;
        }

        // Fallback: Query individually
        $this->logger->debug("Batch query failed, querying individually...");
        foreach ($participants as $p) {
            $pscid = $this->queryMapperSingle($p['external_id']);
            if ($pscid) {
                $this->idMapping[$p['external_id']] = $pscid;
                $this->stats['subjects_mapped']++;
            } else {
                $this->stats['unmapped_subjects'][] = $p['participant_id'];
                $this->stats['subjects_unmapped']++;
            }
        }
    }

    /**
     * Query CBIGR mapper with batch of ExternalIDs
     */
    private function queryMapperBatch(array $externalIds): array
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url = "{$baseUrl}/cbigr_api/externalToInternalIdMapper";

        $mappings = [];

        try {
            // Send as comma-separated string (per API spec)
            $idsString = implode(',', $externalIds);

            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => [$idsString],
            ]);

            if ($response->getStatusCode() !== 200) {
                return [];
            }

            // Parse CSV response: ExtID,PSCID
            $body = (string)$response->getBody();
            $lines = explode("\n", trim($body));

            if (count($lines) < 2) {
                return [];
            }

            // Skip header row
            for ($i = 1; $i < count($lines); $i++) {
                $parts = str_getcsv($lines[$i]);
                if (count($parts) >= 2) {
                    $extID = trim($parts[0], '="');  // Remove Excel formatting
                    $pscid = trim($parts[1]);

                    if ($pscid && $pscid !== 'unauthorized_access') {
                        $mappings[$extID] = $pscid;
                        $this->logger->debug("  Mapped: {$extID} → {$pscid}");
                    }
                }
            }

            return $mappings;

        } catch (\Exception $e) {
            $this->logger->debug("Batch query error: " . $e->getMessage());
            return [];
        }
    }

    /**
     * Query CBIGR mapper for single ExternalID → PSCID
     */
    private function queryMapperSingle(string $externalId): ?string
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url = "{$baseUrl}/cbigr_api/externalToInternalIdMapper";

        try {
            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => [$externalId],
            ]);

            if ($response->getStatusCode() !== 200) {
                return null;
            }

            // Parse CSV response
            $body = (string)$response->getBody();
            $lines = explode("\n", trim($body));

            if (count($lines) < 2) {
                return null;
            }

            $parts = str_getcsv($lines[1]);
            if (count($parts) >= 2) {
                $pscid = trim($parts[1]);
                if ($pscid && $pscid !== 'unauthorized_access') {
                    $this->logger->debug("  Mapped: {$externalId} → {$pscid}");
                    return $pscid;
                }
            }

            return null;

        } catch (\Exception $e) {
            $this->logger->debug("Query error for {$externalId}: " . $e->getMessage());
            return null;
        }
    }

    /**
     * Create target directory
     */
    private function createTargetDirectory(string $targetDir, bool $force): bool
    {
        if (file_exists($targetDir)) {
            if (!$force) {
                $this->logger->error("Target directory exists (use --force to overwrite)");
                return false;
            }

            $this->logger->warning("Removing existing target directory");
            $this->rmdirRecursive($targetDir);
        }

        if (!mkdir($targetDir, 0755, true)) {
            $this->logger->error("Failed to create directory: {$targetDir}");
            return false;
        }

        $this->logger->info("Created target directory: {$targetDir}");
        return true;
    }

    /**
     * Copy BIDS data with renamed subject directories
     */
    private function copyBidsData(string $sourceDir, string $targetDir, array $participants): void
    {
        $this->logger->info("Copying and renaming BIDS data...");

        // Copy dataset-level files first
        $this->copyDatasetFiles($sourceDir, $targetDir);

        // Copy and rename each subject directory
        foreach ($participants as $p) {
            $externalId = $p['external_id'];
            $participantId = $p['participant_id'];

            // Check if we have mapping
            if (!isset($this->idMapping[$externalId])) {
                $this->logger->warning("  Skipping {$participantId} (no PSCID mapping)");
                continue;
            }

            $pscid = $this->idMapping[$externalId];
            $sourceSubDir = "{$sourceDir}/{$participantId}";
            $targetSubDir = "{$targetDir}/sub-{$pscid}";

            if (!is_dir($sourceSubDir)) {
                $this->logger->warning("  Subject directory not found: {$sourceSubDir}");
                continue;
            }

            $this->logger->info("  Copying: {$participantId} → sub-{$pscid}");
            $this->copyDirectoryRecursive($sourceSubDir, $targetSubDir);
        }

        // Update participants.tsv with PSCIDs
        $this->updateParticipantsTsv($targetDir, $participants);
    }

    /**
     * Copy dataset-level files (excluding participants.tsv, we'll generate new one)
     */
    private function copyDatasetFiles(string $sourceDir, string $targetDir): void
    {
        $files = [
            'dataset_description.json',
            'README',
            'README.md',
            'CHANGES',
            'LICENSE',
            '.bidsignore',
        ];

        foreach ($files as $file) {
            $source = "{$sourceDir}/{$file}";
            if (file_exists($source)) {
                $target = "{$targetDir}/{$file}";
                copy($source, $target);
                $this->stats['files_copied']++;
                $this->logger->debug("  Copied: {$file}");
            }
        }

        // Copy code/, derivatives/ if exist
        foreach (['code', 'derivatives', 'sourcedata'] as $dir) {
            $sourceSubDir = "{$sourceDir}/{$dir}";
            if (is_dir($sourceSubDir)) {
                $this->copyDirectoryRecursive($sourceSubDir, "{$targetDir}/{$dir}");
            }
        }
    }

    /**
     * Update participants.tsv with PSCIDs
     */
    private function updateParticipantsTsv(string $targetDir, array $participants): void
    {
        $tsvFile = "{$targetDir}/participants.tsv";
        $handle = fopen($tsvFile, 'w');

        if ($handle === false) {
            $this->logger->error("Cannot create participants.tsv");
            return;
        }

        // Write header (add PSCID column if not present)
        $firstRow = $participants[0]['row_data'] ?? [];
        $headers = array_keys($firstRow);
        if (!in_array('PSCID', $headers)) {
            array_splice($headers, 1, 0, ['PSCID']);  // Insert after participant_id
        }
        fwrite($handle, implode("\t", $headers) . "\n");

        // Write rows with updated participant_id and PSCID
        foreach ($participants as $p) {
            $externalId = $p['external_id'];
            if (!isset($this->idMapping[$externalId])) {
                continue;  // Skip unmapped
            }

            $pscid = $this->idMapping[$externalId];
            $row = $p['row_data'];

            // Update participant_id to PSCID format
            $row['participant_id'] = "sub-{$pscid}";

            // Add/update PSCID column
            if (!isset($row['PSCID'])) {
                $values = array_values($row);
                array_splice($values, 1, 0, [$pscid]);
            } else {
                $row['PSCID'] = $pscid;
                $values = array_values($row);
            }

            fwrite($handle, implode("\t", $values) . "\n");
        }

        fclose($handle);
        $this->stats['files_copied']++;
        $this->logger->info("  Updated participants.tsv with PSCIDs");
    }

    /**
     * Update dataset_description.json
     */
    private function updateDatasetDescription(string $sourceDir, string $targetDir): void
    {
        $sourceFile = "{$sourceDir}/dataset_description.json";
        $targetFile = "{$targetDir}/dataset_description.json";

        if (!file_exists($targetFile)) {
            return;
        }

        $data = json_decode(file_get_contents($targetFile), true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            return;
        }

        // Update description to note reidentification
        if (isset($data['Name'])) {
            $data['Name'] .= " (LORIS Reidentified)";
        }

        $data['GeneratedBy'] = [
            [
                'Name' => 'ARCHIMEDES BIDS Reidentifier',
                'Version' => '1.0.0',
                'Description' => 'Reidentified from external IDs to LORIS PSCIDs',
            ]
        ];

        file_put_contents($targetFile, json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        $this->logger->debug("  Updated dataset_description.json");
    }

    /**
     * Copy directory recursively
     */
    private function copyDirectoryRecursive(string $source, string $target): void
    {
        if (!is_dir($source)) {
            return;
        }

        if (!is_dir($target)) {
            mkdir($target, 0755, true);
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($source, \RecursiveDirectoryIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::SELF_FIRST
        );

        foreach ($iterator as $item) {
            $targetPath = $target . DIRECTORY_SEPARATOR . $iterator->getSubPathName();

            if ($item->isDir()) {
                if (!is_dir($targetPath)) {
                    mkdir($targetPath, 0755, true);
                }
            } else {
                copy($item->getPathname(), $targetPath);
                $this->stats['files_copied']++;
            }
        }
    }

    /**
     * Remove directory recursively
     */
    private function rmdirRecursive(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \RecursiveDirectoryIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST
        );

        foreach ($iterator as $item) {
            if ($item->isDir()) {
                rmdir($item->getPathname());
            } else {
                unlink($item->getPathname());
            }
        }

        rmdir($dir);
    }

    public function getStats(): array
    {
        return $this->stats;
    }
}
