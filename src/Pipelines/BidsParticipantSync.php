<?php

declare(strict_types=1);

namespace LORIS\Pipelines;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Handler\RotatingFileHandler;
use Monolog\Formatter\LineFormatter;
use Psr\Log\LoggerInterface;
use GuzzleHttp\Client as GuzzleClient;

/**
 * BIDS Participant Sync Pipeline
 *
 * Scans a BIDS directory, resolves participants against LORIS,
 * creates missing candidates via the PHP API client, and appends
 * ExternalIDs via the LORIS formHandler.php endpoint.
 *
 * Guarantees: every participant in the BIDS dataset will have
 *   1. An existing LORIS CandID
 *   2. An entry in candidate_project_extid_rel (ExternalID)
 *
 * NEW FEATURES (v2):
 *   - Reads project.json from BIDS directory for project defaults
 *   - project_external_id NO LONGER REQUIRED in CSV (mapped via config)
 *   - project field OPTIONAL in CSV (falls back to project.json)
 *   - Only creates candidates if they don't already exist in LORIS
 *
 * Usage:
 *   - Candidate creation:   Direct LORIS API (POST /api/v{version}/candidates)
 *   - ExternalID append:    POST to candidate_parameters/ajax/formHandler.php
 *   - StudyUID update:      Same formHandler.php with tab=candidateEDC
 *
 * @package LORIS\Pipelines
 */
class BidsParticipantSync
{
    private LoggerInterface $logger;
    private array           $config;
    private ?string         $token       = null;
    private GuzzleClient    $httpClient;
    private ?array          $projectDefaults = null;  // From project.json

    // ── Stats ────────────────────────────────────────────────────────────
    private array $stats = [
        'total_participants'   => 0,
        'total_bids_dirs'      => 0,
        'already_exists'       => 0,
        'created'              => 0,
        'external_id_linked'   => 0,
        'external_id_skipped'  => 0,
        'orphan_directories'   => [],  // sub-* dirs not in participants.tsv
        'missing_directories'  => [],  // participants.tsv entries without sub-* dir
        'errors'               => [],
    ];

    /**
     * @param array $config Loaded loris_client_config.json
     */
    public function __construct(array $config)
    {
        // Support both 'loris' and 'api' as the LORIS config key
        if (!isset($config['loris']) && isset($config['api'])) {
            $config['loris'] = $config['api'];
        }

        // Validate required config
        if (empty($config['loris']['base_url'])) {
            throw new \RuntimeException(
                "Missing required config: loris.base_url\n"
                . "Check your loris_client_config.json has a 'loris' (or 'api') section with 'base_url'."
            );
        }
        if (empty($config['loris']['username']) || empty($config['loris']['password'])) {
            throw new \RuntimeException(
                "Missing required config: loris.username and/or loris.password"
            );
        }

        $this->config     = $config;
        $this->logger     = $this->initLogger();
        $this->httpClient = new GuzzleClient([
            'verify'  => false,
            'timeout' => 30,
        ]);
    }

    // =====================================================================
    //  LOGGING
    // =====================================================================

    private function initLogger(): Logger
    {
        $logger    = new Logger('bids-participant-sync');
        $formatter = new LineFormatter(
            "[%datetime%] %channel%.%level_name%: %message%\n",
            'Y-m-d H:i:s'
        );

        // Console handler (always enabled)
        $console = new StreamHandler('php://stdout', Logger::INFO);
        $console->setFormatter($formatter);
        $logger->pushHandler($console);

        // File handler will be added after project.json is loaded
        return $logger;
    }

    /**
     * Setup file logging after project.json is loaded.
     * Creates date-stamped log files: bids_participant_sync_2026-02-06.log
     */
    private function setupFileLogging(): void
    {
        // Priority 1: project.json
        $logDir = null;
        if ($this->projectDefaults && isset($this->projectDefaults['logging']['log_path'])) {
            $logDir = $this->projectDefaults['logging']['log_path'];
            $this->logger->debug("  Using log path from project.json: {$logDir}");
        }

        // Priority 2: config
        if (!$logDir && isset($this->config['logging']['directory'])) {
            $logDir = $this->config['logging']['directory'];
            $this->logger->debug("  Using log path from config: {$logDir}");
        }

        // Priority 3: default
        if (!$logDir) {
            $logDir = '/tmp/archimedes-logs';
        }

        // Create directory
        if (!is_dir($logDir)) {
            if (!@mkdir($logDir, 0755, true)) {
                $logDir = '/tmp/archimedes-logs';
                @mkdir($logDir, 0755, true);
            }
        }

        // Date-stamped filename
        $dateStamp = date('Y-m-d');
        $logFile = "{$logDir}/bids_participant_sync_{$dateStamp}.log";

        $formatter = new LineFormatter(
            "[%datetime%] %channel%.%level_name%: %message%\n",
            'Y-m-d H:i:s'
        );

        $file = new StreamHandler($logFile, Logger::DEBUG);
        $file->setFormatter($formatter);
        $this->logger->pushHandler($file);

        $this->logger->info("  Log file: {$logFile}");
    }

    // =====================================================================
    //  AUTHENTICATION
    // =====================================================================

    /**
     * Authenticate with LORIS and obtain JWT token.
     * Uses the loris-php-api-client AuthenticationApi.
     */
    /**
     * Authenticate with LORIS and obtain a JWT token.
     * Uses direct HTTP POST to /api/{version}/login
     */
    private function authenticate(): void
    {
        $baseUrl  = rtrim($this->config['loris']['base_url'], '/');
        $version  = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';
        $username = $this->config['loris']['username'];
        $password = $this->config['loris']['password'];

        $this->logger->info("Authenticating with LORIS at {$baseUrl}");

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

            $this->logger->info("  ✓ Authenticated successfully");
        } catch (\Exception $e) {
            $this->logger->error("  ✗ Authentication failed: " . $e->getMessage());
            throw new \RuntimeException("LORIS authentication failed: " . $e->getMessage());
        }
    }

    // =====================================================================
    //  PROJECT.JSON PARSING
    // =====================================================================

    /**
     * Load project.json from BIDS directory for project defaults.
     * Expected fields:
     *   - project: LORIS project name (e.g., "archimedes")
     *   - project_full_name: Full project name (e.g., "ARCHIMEDES")
     *   - site: Default site (e.g., "Montreal")
     *
     * @param string $bidsDir BIDS root directory
     * @return array|null Project defaults or null if file doesn't exist
     */
    private function loadProjectDefaults(string $bidsDir): ?array
    {
        // Check multiple locations for project.json
        $searchPaths = [
            rtrim($bidsDir, '/') . '/project.json',                    // BIDS directory
            dirname(rtrim($bidsDir, '/')) . '/project.json',           // Parent (deidentified-raw)
            dirname(dirname(rtrim($bidsDir, '/'))) . '/project.json',  // Grandparent (project root)
        ];

        $projectFile = null;
        foreach ($searchPaths as $path) {
            if (file_exists($path)) {
                $projectFile = $path;
                break;
            }
        }

        if (!$projectFile) {
            $this->logger->debug("No project.json found in:");
            foreach ($searchPaths as $path) {
                $this->logger->debug("  - {$path}");
            }
            return null;
        }

        $content = file_get_contents($projectFile);
        if ($content === false) {
            $this->logger->warning("Cannot read project.json at {$projectFile}");
            return null;
        }

        $data = json_decode($content, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->logger->warning("Invalid JSON in project.json: " . json_last_error_msg());
            return null;
        }

        $this->logger->info("  ✓ Loaded project defaults from: {$projectFile}");
        $this->logger->debug("    Project: " . ($data['project'] ?? 'N/A'));
        $this->logger->debug("    Site: " . ($data['site'] ?? 'N/A'));
        if (isset($data['project_mappings'])) {
            $this->logger->debug("    Found project_mappings in project.json");
        }

        return $data;
    }

    /**
     * Get ProjectExternalID from candidate_parameters mapping.
     * Uses config mappings or defaults.
     *
     * @param string $projectName Project name from CSV or project.json
     * @return string|null ProjectExternalID (numeric) or null if not found
     */
    private function getProjectExternalID(string $projectName): ?string
    {
        $this->logger->info("  → Looking up ProjectExternalID for: {$projectName}");

        // PRIORITY 1: Query database for ProjectExternalID from project_external table
        // SECURITY: Database credentials MUST be in config file, never hardcoded
        $this->logger->debug("  Step 1: Querying project_external table...");

        try {
            // Get database config - NO DEFAULTS, must be in config
            $dbConfig = $this->config['database'] ?? null;
            if (!$dbConfig) {
                throw new \RuntimeException("Missing 'database' section in config");
            }

            $host = $dbConfig['host'] ?? null;
            $dbname = $dbConfig['name'] ?? null;
            $user = $dbConfig['user'] ?? null;
            $pass = $dbConfig['password'] ?? null;

            if (!$host || !$dbname || !$user) {
                throw new \RuntimeException(
                    "Missing required database config: host, name, and user are required in loris_client_config.json"
                );
            }

            $this->logger->debug("    Connecting to: {$user}@{$host}/{$dbname}");

            // Connect to database
            $dsn = "mysql:host={$host};dbname={$dbname};charset=utf8mb4";
            $pdo = new \PDO($dsn, $user, $pass, [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            ]);

            // Query project_external table joined with Project table
            $stmt = $pdo->prepare("
                SELECT pe.ProjectExternalID, pe.Name as ExternalName, p.Name as ProjectName
                FROM project_external pe
                JOIN Project p ON pe.ProjectID = p.ProjectID
                WHERE p.Name = :name OR pe.Name = :name
            ");
            $stmt->execute(['name' => $projectName]);

            $result = $stmt->fetch();

            if ($result) {
                $projectExternalID = $result['ProjectExternalID'];
                $this->logger->info("  ✓ Found ProjectExternalID in database: {$projectExternalID} (project: {$result['ProjectName']})");
                return (string)$projectExternalID;
            }

            $this->logger->debug("  ✗ Project '{$projectName}' not found in project_external table");

            // List all available project_external entries for debugging
            $stmt = $pdo->query("
                SELECT pe.ProjectExternalID, pe.Name as ExternalName, p.Name as ProjectName
                FROM project_external pe
                JOIN Project p ON pe.ProjectID = p.ProjectID
                ORDER BY p.Name
            ");
            $allProjects = $stmt->fetchAll();

            if (!empty($allProjects)) {
                $projectNames = array_map(function($p) {
                    return "{$p['ProjectName']} (ExtID: {$p['ProjectExternalID']})";
                }, $allProjects);
                $this->logger->debug("    Available in project_external: " . implode(", ", $projectNames));
            }

        } catch (\Exception $e) {
            $this->logger->warning("  Database query failed: " . $e->getMessage());
            $this->logger->debug("    Exception type: " . get_class($e));
        }

        // PRIORITY 2: Check config mappings (fallback)
        $this->logger->debug("  Step 2: Checking config/project.json mappings...");

        $projectMappings = $this->config['project_mappings'] ?? [];
        if (isset($projectMappings[$projectName])) {
            $this->logger->info("  ✓ Found in config mapping: {$projectMappings[$projectName]}");
            return (string)$projectMappings[$projectName];
        }

        // Check project.json (projectDefaults) for mappings
        if ($this->projectDefaults && isset($this->projectDefaults['project_mappings'][$projectName])) {
            $id = $this->projectDefaults['project_mappings'][$projectName];
            $this->logger->info("  ✓ Found in project.json mapping: {$id}");
            return (string)$id;
        }

        // PRIORITY 3: Fallback to config default (last resort)
        $this->logger->debug("  Step 3: Checking config default...");
        $defaultProjectExternalID = $this->config['loris']['project_external_id']
            ?? $this->config['api']['project_external_id']
            ?? null;
        if ($defaultProjectExternalID) {
            $this->logger->info("  ✓ Using default ProjectExternalID from config: {$defaultProjectExternalID}");
            return (string)$defaultProjectExternalID;
        }

        $this->logger->error("  ✗ Could not determine ProjectExternalID for: {$projectName}");
        $this->logger->info("  Tried:");
        $this->logger->info("    1. Database query (project_external table)");
        $this->logger->info("    2. Config mappings (project_mappings in config/project.json)");
        $this->logger->info("    3. Config default (loris.project_external_id)");
        $this->logger->info("  Solution: Either add project to project_external table in LORIS,");
        $this->logger->info("            or add mapping: \"project_mappings\": {\"FDG PET\": \"<ExtID>\"}");
        return null;
    }

    // =====================================================================
    //  BIDS DIRECTORY SCANNING
    // =====================================================================

    /**
     * Parse participants.tsv from BIDS root directory.
     *
     * @return array<int, array{participant_id: string, external_id: string, ...}>
     */
    private function parseBidsParticipants(string $bidsDir): array
    {
        $tsvFile = rtrim($bidsDir, '/') . '/participants.tsv';

        if (!file_exists($tsvFile)) {
            $this->logger->error("participants.tsv not found in {$bidsDir}");
            return [];
        }

        $handle = fopen($tsvFile, 'r');
        if ($handle === false) {
            $this->logger->error("Cannot open {$tsvFile}");
            return [];
        }

        // Header row
        $headerLine = fgets($handle);
        if ($headerLine === false) {
            fclose($handle);
            return [];
        }
        $headers = array_map('trim', explode("\t", $headerLine));

        $participants = [];
        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if ($line === '') continue;

            $fields = array_map('trim', explode("\t", $line));
            $row    = [];
            foreach ($headers as $i => $header) {
                $row[$header] = $fields[$i] ?? '';
            }
            $participants[] = $row;
        }

        fclose($handle);

        $this->logger->info("  Parsed " . count($participants) . " participants from {$tsvFile}");
        return $participants;
    }

    /**
     * Discover all sub-* directories in the BIDS root.
     */
    private function discoverBidsSubjects(string $bidsDir): array
    {
        $subjects = [];
        $dirs     = glob(rtrim($bidsDir, '/') . '/sub-*', GLOB_ONLYDIR);

        foreach ($dirs as $dir) {
            $subjects[] = basename($dir);
        }

        sort($subjects);
        return $subjects;
    }

    /**
     * Extract the ExternalID from a participant row.
     * Checks multiple possible column names.
     */
    private function extractExternalID(array $row): ?string
    {
        $extIdCols = [
            'external_id', 'ExternalID', 'externalid',
            'study_id',    'StudyID',    'studyid',
            'pscid',       'PSCID',
            'ext_study_id','ExtStudyID',
        ];

        foreach ($extIdCols as $col) {
            if (!empty($row[$col])) {
                return trim($row[$col]);
            }
        }

        return null;
    }

    /**
     * Extract sex from participant row, normalised for LORIS API.
     * Returns null if sex is missing or unrecognised — caller must handle.
     */
    private function extractSex(array $row): ?string
    {
        $raw = strtolower(trim($row['sex'] ?? $row['Sex'] ?? $row['gender'] ?? ''));
        return match ($raw) {
            'm', 'male'   => 'Male',
            'f', 'female' => 'Female',
            default        => null,
        };
    }

    // =====================================================================
    //  CBIGR API: ExternalID → PSCID Lookup
    // =====================================================================

    /**
     * Look up ExternalID → PSCID mapping via CBIGR API.
     *
     * Endpoint: POST /cbigr_api/externalToInternalIdMapper
     * Body: JSON array with single comma-separated string: ["EXT001,EXT002"]
     * Returns: CSV with ExtID,PSCID columns
     *
     * When ExternalID doesn't exist, API returns:
     *   - 400 BadRequest: "Error: no PSCIDS returned"
     *   - 404 NotFound: "Number of returned IDs does not match"
     *
     * @param string $externalID  Single external ID to look up
     * @return array|null  ['PSCID' => '...', 'CandID' => '...'] or null if not found
     */
    private function lookupExternalIDViaCBIGR(string $externalID): ?array
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url     = "{$baseUrl}/cbigr_api/externalToInternalIdMapper";

        $this->logger->debug("  CBIGR lookup: {$externalID}");

        try {
            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json' => [$externalID],  // API expects ["EXT001"] or ["EXT001,EXT002"]
            ]);

            $statusCode = $response->getStatusCode();

            if ($statusCode === 200) {
                // Parse CSV response: ExtID,PSCID
                $body = (string)$response->getBody();
                $lines = explode("\n", trim($body));

                if (count($lines) >= 2) {
                    // Skip header row, parse data row
                    $dataLine = $lines[1];
                    $parts = str_getcsv($dataLine);

                    if (count($parts) >= 2) {
                        $extID = trim($parts[0], '="');  // Remove Excel formatting
                        $pscid = trim($parts[1]);

                        if ($pscid && $pscid !== 'unauthorized_access') {
                            $this->logger->info("  ✓ CBIGR found: ExtStudyID={$extID} → PSCID={$pscid}");

                            // Return PSCID - CandID not needed for skip logic
                            return [
                                'PSCID'  => $pscid,
                                'CandID' => 'exists',  // Just a marker that it exists
                            ];
                        }
                    }
                }
            }

            // 404 or other — not found
            $this->logger->debug("  CBIGR: ExternalID {$externalID} not mapped (status {$statusCode})");
            return null;

        } catch (\Exception $e) {
            $errMsg = $e->getMessage();

            // Handle expected "not found" responses from the mapper
            // 400 BadRequest: "Error: no PSCIDS returned"
            // 404 NotFound: "Number of returned IDs does not match"
            if (strpos($errMsg, 'no PSCIDS returned') !== false ||
                strpos($errMsg, 'Number of returned IDs') !== false ||
                (method_exists($e, 'getCode') && ($e->getCode() === 404 || $e->getCode() === 400))) {
                $this->logger->debug("  CBIGR: ExternalID {$externalID} not found in database");
                return null;
            }

            // Other errors - log as warning
            $this->logger->warning("  CBIGR lookup error: " . $errMsg);
            return null;
        }
    }

    // =====================================================================
    //  LORIS CANDIDATE LOOKUP
    // =====================================================================

    /**
     * Fetch all existing candidates from LORIS API.
     * Returns a map: PSCID => CandID
     *
     * @return array<string, string>
     */
    private function fetchExistingCandidates(): array
    {
        $this->logger->info("Fetching existing candidates from LORIS API...");
        $map = [];

        try {
            $result     = $this->candidatesApi->getCandidates();
            $candidates = $result->getCandidates ?? $result;

            // The API returns an object with a Candidates array
            // Handle both possible response structures
            if (is_object($result) && method_exists($result, 'getCandidates')) {
                $candidates = $result->getCandidates();
            } elseif (is_array($result)) {
                $candidates = $result['Candidates'] ?? $result;
            } else {
                // Decode from JSON if needed
                $decoded    = json_decode(json_encode($result), true);
                $candidates = $decoded['Candidates'] ?? $decoded;
            }

            if (is_array($candidates)) {
                foreach ($candidates as $c) {
                    $pscid  = is_array($c) ? ($c['PSCID'] ?? '') : (method_exists($c, 'getPscid') ? $c->getPscid() : '');
                    $candID = is_array($c) ? ($c['CandID'] ?? '') : (method_exists($c, 'getCandId') ? $c->getCandId() : '');
                    if ($pscid && $candID) {
                        $map[$pscid] = (string)$candID;
                    }
                }
            }

            $this->logger->info("  ✓ Found " . count($map) . " existing candidates");
        } catch (\Exception $e) {
            // API client may fail on invalid data (e.g., sex='Unknown')
            // This is not fatal - we can still proceed with CBIGR lookups
            $errMsg = $e->getMessage();
            if (strpos($errMsg, "Invalid value") !== false && strpos($errMsg, "sex") !== false) {
                $this->logger->warning("  ⚠ API validation warning: " . $errMsg);
                $this->logger->info("  → Continuing with CBIGR lookups (this is not an error)");
            } else {
                $this->logger->error("  ✗ Failed to fetch candidates: " . $errMsg);
            }
        }

        return $map;
    }

    /**
     * Check if a specific candidate has an ExternalID entry by querying
     * the candidate_parameters page (or a direct API call if available).
     *
     * Uses a GET request to check the candidate details.
     */
    private function candidateHasExternalID(string $candID, string $expectedExtId): bool
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');

        try {
            $result = $this->candidatesApi->getCandidate($candID);

            // Check if ExternalID fields are populated
            $decoded = json_decode(json_encode($result), true);

            // The LORIS API candidate response may contain ExternalID info
            // in various nested locations depending on LORIS version
            $extIds = $decoded['ExternalIDs'] ?? $decoded['Meta']['ExternalIDs'] ?? [];

            if (is_array($extIds)) {
                foreach ($extIds as $entry) {
                    $extStudyId = $entry['ExtStudyID'] ?? $entry['externalID'] ?? '';
                    if ($extStudyId === $expectedExtId) {
                        return true;
                    }
                }
            }
        } catch (\Exception $e) {
            $this->logger->debug("  Could not verify ExternalID via API for {$candID}: " . $e->getMessage());
        }

        return false;
    }

    // =====================================================================
    //  CANDIDATE CREATION  (via loris-php-api-client)
    // =====================================================================

    /**
     * Create a new candidate using direct LORIS API.
     *
     * @return string|null  CandID of the new candidate, or null on failure
     */
    private function createCandidate(string $pscid, string $sex, string $site, string $project, ?string $dob = null): ?string
    {
        $this->logger->info("  Creating candidate: PSCID={$pscid}, Sex={$sex}, Site={$site}, Project={$project}, DoB={$dob}");

        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $version = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';
        $url = "{$baseUrl}/api/{$version}/candidates";

        $candidateData = [
            'Candidate' => [
                'Project' => $project,
                'Site'    => $site,
                'Sex'     => $sex,
                'PSCID'   => $pscid,
            ]
        ];

        if ($dob) {
            $candidateData['Candidate']['DoB'] = $dob;
        }

        try {
            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type' => 'application/json',
                ],
                'json' => $candidateData,
            ]);

            $statusCode = $response->getStatusCode();

            if ($statusCode === 201 || $statusCode === 200) {
                $body = (string)$response->getBody();
                $data = json_decode($body, true);

                $candID = $data['Meta']['CandID'] ?? $data['CandID'] ?? null;

                if ($candID) {
                    $this->logger->info("  ✓ Created candidate: CandID={$candID}, PSCID={$pscid}");
                    return (string)$candID;
                }

                $this->logger->error("  ✗ CandID not found in response");
                $this->logger->debug("    Response: {$body}");
                return null;
            }

            $this->logger->error("  ✗ Unexpected status: {$statusCode}");
            return null;

        } catch (\Exception $e) {
            $this->logger->error("  ✗ Failed to create candidate: " . $e->getMessage());
            if ($errorBody) {
                $this->logger->debug("    Response body: {$errorBody}");
            }

            // If 409 Conflict (PSCID already exists), try to look it up
            if (method_exists($e, 'getCode') && $e->getCode() === 409) {
                $this->logger->info("  → PSCID {$pscid} already exists (409), attempting lookup...");
                return $this->lookupCandidateByPSCID($pscid);
            }

            $this->stats['errors'][] = "Create candidate {$pscid}: " . $e->getMessage();
            return null;
        }
    }

    /**
     * Lookup a candidate by PSCID using the API.
     */
    private function lookupCandidateByPSCID(string $pscid): ?string
    {
        try {
            $result     = $this->candidatesApi->getCandidates();
            $decoded    = json_decode(json_encode($result), true);
            $candidates = $decoded['Candidates'] ?? $decoded ?? [];

            foreach ($candidates as $c) {
                if (($c['PSCID'] ?? '') === $pscid) {
                    return (string)($c['CandID'] ?? '');
                }
            }
        } catch (\Exception $e) {
            $this->logger->error("  ✗ Lookup by PSCID failed: " . $e->getMessage());
        }

        return null;
    }

    // =====================================================================
    //  EXTERNAL ID APPEND  (via formHandler.php)
    // =====================================================================

    /**
     * Append ExternalID (ExtStudyID) to a candidate using the LORIS
     * candidate_parameters formHandler.php endpoint.
     *
     * Endpoint:  POST {baseUrl}/candidate_parameters/ajax/formHandler.php
     *
     * Required POST fields (from editExternalIdentifierFields in formHandler.php):
     *   - tab         = "externalIdentifier"
     *   - candID      = CandID
     *   - ProjectID   = ProjectExternalID (int from project_external table)
     *   - ExtStudyID  = The external study identifier string
     *   - Comment     = Optional comment
     *
     * Inserts into: candidate_project_extid_rel (CandID, ProjectExternalID, ExtStudyID, Comment)
     *
     * UI equivalent:
     *   {baseUrl}/candidate_parameters/?candID={candID}&identifier={candID}#externalIdentifier
     */
    private function appendExternalID(
        string  $candID,
        string  $extStudyID,
        string  $projectExternalID,
        ?string $comment = null
    ): bool {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url     = "{$baseUrl}/candidate_parameters/ajax/formHandler.php";

        $this->logger->info(
            "  Appending ExternalID: CandID={$candID}, "
            . "ProjectExternalID={$projectExternalID}, ExtStudyID={$extStudyID}"
        );

        $multipart = [
            ['name' => 'tab',       'contents' => 'externalIdentifier'],
            ['name' => 'candID',    'contents' => $candID],
            ['name' => 'ProjectID', 'contents' => $projectExternalID],
            ['name' => 'ExtStudyID','contents' => $extStudyID],
        ];

        if ($comment !== null && $comment !== '') {
            $multipart[] = ['name' => 'Comment', 'contents' => $comment];
        }

        try {
            $response = $this->httpClient->request('POST', $url, [
                'headers'   => [
                    'Authorization' => "Bearer {$this->token}",
                ],
                'multipart' => $multipart,
            ]);

            $statusCode = $response->getStatusCode();
            $body       = (string)$response->getBody();

            if ($statusCode >= 200 && $statusCode < 300) {
                $this->logger->info("  ✓ ExternalID appended: {$extStudyID} → CandID {$candID}");
                return true;
            }

            $this->logger->warning("  ⚠ Unexpected status {$statusCode}: {$body}");
            return false;

        } catch (\Exception $e) {
            $this->logger->error("  ✗ Failed to append ExternalID: " . $e->getMessage());

            // Try to get response body for 500 errors
            if (method_exists($e, 'getResponse')) {
                $response = $e->getResponse();
                if ($response) {
                    $errorBody = (string)$response->getBody();
                    $this->logger->error("    Server response: {$errorBody}");
                }
            }

            $this->stats['errors'][] = "ExternalID {$extStudyID} for CandID {$candID}: " . $e->getMessage();
            return false;
        }
    }

    // =====================================================================
    //  MAIN PIPELINE
    // =====================================================================

    /**
     * Run the full participant sync pipeline.
     *
     * 1. Parse participants.tsv from the BIDS directory
     * 2. Cross-reference sub-* directories
     * 3. Fetch existing candidates from LORIS
     * 4. Create missing candidates via PHP API client
     * 5. Append ExternalIDs via formHandler.php
     *
     * @param string $bidsDir  Path to BIDS root directory
     * @param bool   $dryRun   If true, log actions without executing
     */
    public function run(string $bidsDir, bool $dryRun = false): array
    {
        $this->logger->info("═══════════════════════════════════════════════════════════");
        $this->logger->info("  BIDS Participant Sync Pipeline");
        $this->logger->info("═══════════════════════════════════════════════════════════");
        $this->logger->info("  BIDS directory : {$bidsDir}");
        $this->logger->info("  Dry run        : " . ($dryRun ? 'YES' : 'NO'));
        $this->logger->info("───────────────────────────────────────────────────────────");

        // ── Validate BIDS directory ──────────────────────────────────────
        if (!is_dir($bidsDir)) {
            $this->logger->error("BIDS directory does not exist: {$bidsDir}");
            return $this->stats;
        }

        $descFile = $bidsDir . '/dataset_description.json';
        if (!file_exists($descFile)) {
            $this->logger->warning("Missing dataset_description.json — continuing anyway");
        }

        // ── Load project defaults from project.json ──────────────────────
        $this->projectDefaults = $this->loadProjectDefaults($bidsDir);

        // ── Setup file logging (uses project.json log_path if available) ─
        $this->setupFileLogging();

        // ── Authenticate ─────────────────────────────────────────────────
        $this->authenticate();

        // ── Parse participants ────────────────────────────────────────────
        $participants = $this->parseBidsParticipants($bidsDir);
        $bidsSubjects = $this->discoverBidsSubjects($bidsDir);

        $this->stats['total_bids_dirs'] = count($bidsSubjects);

        $this->logger->info("Found " . count($bidsSubjects) . " sub-* directories");
        $this->logger->info("Found " . count($participants) . " entries in participants.tsv");

        // Build participant lookup: participant_id => row
        $participantMap = [];
        foreach ($participants as $row) {
            $pid = $row['participant_id'] ?? '';
            if ($pid) {
                $participantMap[$pid] = $row;
            }
        }

        // ── Cross-reference: BIDS dirs vs participants.tsv ───────────────
        $this->logger->info("───────────────────────────────────────────────────────────");
        $this->logger->info("Validating BIDS structure...");

        // Find orphan directories (sub-* exists but not in participants.tsv)
        foreach ($bidsSubjects as $subDir) {
            if (!isset($participantMap[$subDir])) {
                $this->stats['orphan_directories'][] = $subDir;
                $this->logger->warning("  ⚠ ORPHAN DIRECTORY: {$subDir}");
                $this->logger->warning("     → Folder exists in BIDS but NOT listed in participants.tsv");
            }
        }

        // Find missing directories (in participants.tsv but no sub-* dir)
        foreach ($participantMap as $pid => $row) {
            if (!in_array($pid, $bidsSubjects)) {
                $this->stats['missing_directories'][] = $pid;
                $externalID = $this->extractExternalID($row);
                $this->logger->warning("  ⚠ MISSING DIRECTORY: {$pid}");
                $this->logger->warning("     → Listed in participants.tsv (ExternalID: {$externalID}) but NO folder exists");
            }
        }

        if (empty($this->stats['orphan_directories']) && empty($this->stats['missing_directories'])) {
            $this->logger->info("  ✓ All sub-* directories match participants.tsv");
        } else {
            $orphanCount = count($this->stats['orphan_directories']);
            $missingCount = count($this->stats['missing_directories']);
            $this->logger->warning("  ⚠ Found {$orphanCount} orphan directories and {$missingCount} missing directories");
        }

        // ── Only process participants that are in participants.tsv ───────
        // (We need external_id from there, orphan dirs can't be processed)
        $this->stats['total_participants'] = count($participantMap);
        $this->logger->info("───────────────────────────────────────────────────────────");

        // ── Process each participant ─────────────────────────────────────
        $this->logger->info("Processing participants from participants.tsv...");
        $this->logger->info("───────────────────────────────────────────────────────────");

        // Cache for newly created candidates (to avoid duplicate API calls)
        $existingCandidates = [];

        foreach ($participantMap as $subjectId => $row) {
            $this->logger->info("");
            $this->logger->info("▸ Participant: {$subjectId}");

            $externalID = $this->extractExternalID($row);
            $sex        = $this->extractSex($row);

            // Site: ONLY from CSV (required in participants.tsv)
            $site = trim($row['site'] ?? $row['Site'] ?? '');

            // Project priority: CSV > CLI argument > project.json > error
            $project = trim($row['project'] ?? $row['Project'] ?? '');
            if (!$project && !empty($this->config['cli_overrides']['project'])) {
                $project = trim($this->config['cli_overrides']['project']);
            }
            if (!$project && $this->projectDefaults) {
                $project = trim($this->projectDefaults['project'] ?? '');
            }

            // DoB: Extract from participants.tsv (optional)
            $dob = trim($row['dob'] ?? $row['DoB'] ?? $row['date_of_birth'] ?? '');

            // ─── Validate required fields ────────────────────────────────
            $missingFields = [];

            if (!$externalID) {
                $missingFields[] = 'external_id';
            }
            if (!$sex) {
                $missingFields[] = 'sex';
            }
            if (!$site) {
                $missingFields[] = 'site';
            }
            if (!$project) {
                $missingFields[] = 'project';
            }
            if (!$dob) {
                $missingFields[] = 'dob (Date of Birth)';
            }

            if (!empty($missingFields)) {
                $missing = implode(', ', $missingFields);
                $this->logger->error("  ✗ Missing required field(s) for {$subjectId}: {$missing} — skipping");
                $this->stats['errors'][] = "Missing required field(s) for {$subjectId}: {$missing}";
                $this->stats['external_id_skipped']++;
                continue;
            }

            // Get ProjectExternalID from mapping (not required in CSV anymore)
            $projectExternalID = $this->getProjectExternalID($project);
            if (!$projectExternalID) {
                $this->logger->error("  ✗ Could not determine ProjectExternalID for project: {$project} — skipping");
                $this->stats['errors'][] = "No ProjectExternalID mapping for project {$project}";
                $this->stats['external_id_skipped']++;
                continue;
            }

            // PSCID = external_id (no separate PSCID field)
            $pscid = $externalID;

            $this->logger->info("  external_id (PSCID) : {$externalID}");
            $this->logger->info("  ProjectExternalID   : {$projectExternalID}");
            $this->logger->info("  Sex                 : {$sex}");

            // ─── Step 1: Check CBIGR API for existing ExternalID mapping ───
            // If found, the candidate exists AND ExternalID is already linked
            $cbigrResult = $this->lookupExternalIDViaCBIGR($externalID);

            if ($cbigrResult && !empty($cbigrResult['CandID'])) {
                // ExternalID already mapped → candidate exists, ExternalID already linked
                $pscid  = $cbigrResult['PSCID'];

                $this->logger->info("  ✓ Already mapped in CBIGR: ExtStudyID={$externalID} → PSCID={$pscid}");
                $this->logger->info("  → Skipping (candidate already exists with ExternalID linked)");
                $this->stats['already_exists']++;
                $this->stats['external_id_skipped']++;  // Already linked via CBIGR

                continue;  // Nothing more to do
            }

            // ─── Step 2: ExternalID not found in CBIGR → check if PSCID exists ───
            // (PSCID = external_id, candidate might exist but not linked)
            $candID = $existingCandidates[$pscid] ?? null;

            if ($candID) {
                $this->logger->info("  ✓ Candidate exists (not linked): CandID={$candID}");
                $this->stats['already_exists']++;
            } else {
                // ─── Step 3: Create missing candidate ────────────────
                if ($dryRun) {
                    $this->logger->info("  [DRY-RUN] Would create candidate: PSCID={$pscid}, CandID=?");
                    $this->stats['created']++;
                    $this->logger->info("  [DRY-RUN] Would link ExternalID: {$externalID} → CandID ?");
                    $this->stats['external_id_linked']++;
                    continue;
                }

                $candID = $this->createCandidate($pscid, $sex, $site, $project, $dob);

                if (!$candID) {
                    $this->logger->error("  ✗ Could not create or find candidate for {$pscid}");
                    $this->stats['errors'][] = "Could not create or find candidate for {$pscid}";
                    continue;
                }

                $this->stats['created']++;
                // Update local cache
                $existingCandidates[$pscid] = $candID;
            }

            // ─── Step 4: Link ExternalID via formHandler ────────────────
            // (only reached if CBIGR didn't find a mapping)
            if ($dryRun) {
                $this->logger->info("  [DRY-RUN] Would link ExternalID: {$externalID} → CandID {$candID}");
                $this->stats['external_id_linked']++;
                continue;
            }

            // POST to formHandler.php with:
            //   tab=externalIdentifier, candID, ProjectID, ExtStudyID
            $success = $this->appendExternalID(
                $candID,
                $externalID,          // ExtStudyID = external_id
                $projectExternalID    // ProjectExternalID (int)
            );
            if ($success) {
                $this->stats['external_id_linked']++;
            }
        }

        // ── Summary ──────────────────────────────────────────────────────
        $this->printSummary();

        return $this->stats;
    }

    // =====================================================================
    //  SUMMARY
    // =====================================================================

    private function printSummary(): void
    {
        $this->logger->info("");
        $this->logger->info("═══════════════════════════════════════════════════════════");
        $this->logger->info("  SYNC SUMMARY");
        $this->logger->info("═══════════════════════════════════════════════════════════");
        $this->logger->info("  BIDS sub-* directories : {$this->stats['total_bids_dirs']}");
        $this->logger->info("  participants.tsv rows  : {$this->stats['total_participants']}");
        $this->logger->info("───────────────────────────────────────────────────────────");
        $this->logger->info("  Already mapped (CBIGR) : {$this->stats['already_exists']}");
        $this->logger->info("  Newly created          : {$this->stats['created']}");
        $this->logger->info("  ExternalIDs linked     : {$this->stats['external_id_linked']}");
        $this->logger->info("  ExternalIDs skipped    : {$this->stats['external_id_skipped']}");
        $this->logger->info("───────────────────────────────────────────────────────────");

        $orphanCount  = count($this->stats['orphan_directories']);
        $missingCount = count($this->stats['missing_directories']);

        if ($orphanCount > 0) {
            $this->logger->warning("  ⚠ ORPHAN DIRECTORIES   : {$orphanCount} (sub-* dir exists, NOT in participants.tsv)");
            foreach ($this->stats['orphan_directories'] as $dir) {
                $this->logger->warning("      • {$dir}");
            }
        } else {
            $this->logger->info("  Orphan directories     : 0");
        }

        if ($missingCount > 0) {
            $this->logger->warning("  ⚠ MISSING DIRECTORIES  : {$missingCount} (in participants.tsv, NO sub-* dir)");
            foreach ($this->stats['missing_directories'] as $dir) {
                $this->logger->warning("      • {$dir}");
            }
        } else {
            $this->logger->info("  Missing directories    : 0");
        }

        $this->logger->info("───────────────────────────────────────────────────────────");

        // Add counts to stats for programmatic access
        $this->stats['orphan_count'] = $orphanCount;
        $this->stats['missing_count'] = $missingCount;

        $this->logger->info("  Errors                 : " . count($this->stats['errors']));

        if (!empty($this->stats['errors'])) {
            foreach ($this->stats['errors'] as $err) {
                $this->logger->error("      • {$err}");
            }
        }

        $this->logger->info("═══════════════════════════════════════════════════════════");
    }

    // =====================================================================
    //  GETTERS
    // =====================================================================

    public function getStats(): array
    {
        return $this->stats;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }
}