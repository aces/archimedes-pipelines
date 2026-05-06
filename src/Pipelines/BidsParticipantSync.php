<?php

declare(strict_types=1);

namespace LORIS\Pipelines;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use LORIS\Utils\CleanLogFormatter;

/**
 * BIDS Participant Sync Pipeline
 *
 * Reads participants.tsv, creates missing LORIS candidates, and maps
 * ExtStudyIDs. Continues processing all participants even on individual
 * failures. All errors written to a dedicated error log.
 *
 * Log files in {projectPath}/logs/:
 *   bids_sync_run_{timestamp}.log    — full run log
 *   bids_sync_errors_{timestamp}.log — errors only (created on first error)
 *
 * Date normalization (DoB and DoD):
 *   Per ARCHIMEDES privacy policy, DoB and DoD values are jittered to
 *   YYYY-MM-01 before being sent to LORIS. Missing day -> 01, missing
 *   month -> 01, year-only inputs become YYYY-01-01. Empty values stay
 *   empty; unparseable values pass through unchanged so LORIS surfaces
 *   the validation error rather than the pipeline silently mangling it.
 *
 * Candidate creation strategy:
 *   1. Try CandidatesPlus endpoint first (POST /cbigr_api/candidatesPlus)
 *      — accepts ProjectExternalName + ExtStudyID directly, server-side
 *      handles candidate_project_extid_rel insert atomically.
 *   2. Fall back to legacy two-step (/api/v0.0.4-dev/candidates →
 *      candidate_parameters/ajax/formHandler.php) when CandidatesPlus
 *      returns 404 or other transport errors. Kept until CandidatesPlus
 *      ships everywhere.
 *
 * @package LORIS\Pipelines
 */
class BidsParticipantSync
{
    private Logger  $logger;
    private array   $config;
    private ?string $token           = null;
    private ?array  $projectDefaults = null;

    private string  $runTimestamp;
    private ?string $logDir       = null;
    private ?string $runLogPath   = null;
    private ?string $errorLogPath = null;

    /** @var resource|null */
    private $runLogFh = null;
    /** @var resource|null */
    private $errorFh  = null;

    /**
     * Tracks whether CandidatesPlus has been confirmed available on this
     * server during this run. Once it returns 404 we stop trying it for
     * subsequent participants and fall through to the legacy path
     * silently. Reset on each run() call.
     */
    private bool $candidatesPlusAvailable = true;

    private array $stats = [
        'total_participants'   => 0,
        'total_bids_dirs'      => 0,
        'already_exists'       => 0,
        'created'              => 0,
        'created_via_plus'     => 0,
        'created_via_legacy'   => 0,
        'external_id_linked'   => 0,
        'external_id_skipped'  => 0,
        'orphan_directories'   => [],
        'missing_directories'  => [],
        'errors'               => [],
    ];

    public function __construct(array $config)
    {
        if (!isset($config['loris']) && isset($config['api'])) {
            $config['loris'] = $config['api'];
        }
        if (empty($config['loris']['base_url'])) {
            throw new \RuntimeException("Missing required config: loris.base_url");
        }
        if (empty($config['loris']['username']) || empty($config['loris']['password'])) {
            throw new \RuntimeException("Missing required config: loris.username / loris.password");
        }
        $this->config       = $config;
        $this->runTimestamp = date('Y-m-d_H-i-s');
        $this->logger       = $this->_initLogger();
    }

    // =========================================================================
    //  LOGGING
    // =========================================================================

    private function _initLogger(): Logger
    {
        $logger    = new Logger('bids-sync');
        $formatter = new CleanLogFormatter();
        $console   = new StreamHandler('php://stdout', Logger::INFO);
        $console->setFormatter($formatter);
        $logger->pushHandler($console);
        return $logger;
    }

    private function _openLogs(string $projectPath): void
    {
        $this->logDir = "{$projectPath}/logs";
        if (!is_dir($this->logDir)) {
            @mkdir($this->logDir, 0755, true);
        }

        $this->runLogPath = "{$this->logDir}/bids_sync_run_{$this->runTimestamp}.log";
        $this->runLogFh   = @fopen($this->runLogPath, 'a');

        if ($this->runLogFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->runLogFh,
                "{$sep}\n BIDS Participant Sync — Run Log\n"
                . " Started : " . date('Y-m-d H:i:s T') . "\n"
                . " Project : {$projectPath}\n"
                . "{$sep}\n\n"
            );
        }

        $formatter      = new CleanLogFormatter();
        $runFileHandler = new StreamHandler($this->runLogPath, Logger::DEBUG);
        $runFileHandler->setFormatter($formatter);
        $this->logger->pushHandler($runFileHandler);

        $this->_log("  Run log  : {$this->runLogPath}");
    }

    private function _openErrorLog(): void
    {
        if ($this->errorFh !== null || $this->logDir === null) {
            return;
        }
        $this->errorLogPath = "{$this->logDir}/bids_sync_errors_{$this->runTimestamp}.log";
        $this->errorFh      = @fopen($this->errorLogPath, 'a');
        if ($this->errorFh) {
            $sep = str_repeat('=', 72);
            fwrite($this->errorFh,
                "{$sep}\n BIDS Participant Sync — Error Log\n"
                . " Run: {$this->runTimestamp}\n"
                . "{$sep}\n\n"
            );
        }
        $this->_log("  Error log: {$this->errorLogPath}");
    }

    private function _log(string $msg): void
    {
        $this->logger->info($msg);
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[" . date('H:i:s') . "] {$msg}\n");
        }
    }

    private function _warn(string $context, string $msg): void
    {
        $this->logger->warning("[{$context}] {$msg}");
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[" . date('H:i:s') . "] WARNING [{$context}] {$msg}\n");
        }
    }

    private function _error(string $context, string $msg): void
    {
        $this->logger->error("[{$context}] {$msg}");
        $this->_openErrorLog();

        $ts = date('H:i:s');
        if ($this->errorFh) {
            fwrite($this->errorFh, "[{$ts}] [{$context}] {$msg}\n");
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "[{$ts}] ERROR [{$context}] {$msg}\n");
        }
        $this->stats['errors'][] = "[{$context}] {$msg}";
    }

    private function _closeLogs(): void
    {
        $sep = str_repeat('=', 72);
        $ts  = date('Y-m-d H:i:s T');
        if ($this->errorFh) {
            fwrite($this->errorFh, "\n{$sep}\n Closed: {$ts}\n{$sep}\n");
            fclose($this->errorFh);
            $this->errorFh = null;
        }
        if ($this->runLogFh) {
            fwrite($this->runLogFh, "\n{$sep}\n Completed: {$ts}\n{$sep}\n");
            fclose($this->runLogFh);
            $this->runLogFh = null;
        }
    }

    // =========================================================================
    //  DATE NORMALIZATION (DoB and DoD)
    // =========================================================================

    private function _normalizeDateValue(string $value): string
    {
        $value = trim($value);
        if ($value === '') return $value;
        if (preg_match('/^(\d{4})-(\d{2})-\d{2}$/', $value, $m)) return "{$m[1]}-{$m[2]}-01";
        if (preg_match('/^(\d{4})-(\d{2})$/',      $value, $m)) return "{$m[1]}-{$m[2]}-01";
        if (preg_match('/^(\d{4})$/',              $value, $m)) return "{$m[1]}-01-01";
        return $value;
    }

    // =========================================================================
    //  HTTP
    // =========================================================================

    private function _http(): \GuzzleHttp\Client
    {
        return new \GuzzleHttp\Client(['verify' => false, 'timeout' => 30]);
    }

    // =========================================================================
    //  AUTHENTICATION
    // =========================================================================

    private function _authenticate(): bool
    {
        $baseUrl  = rtrim($this->config['loris']['base_url'], '/');
        $version  = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';
        $username = $this->config['loris']['username'];
        $password = $this->config['loris']['password'];

        $this->_log("Authenticating with LORIS at {$baseUrl}");

        try {
            $response    = $this->_http()->request('POST', "{$baseUrl}/api/{$version}/login", [
                'json' => compact('username', 'password'),
            ]);
            $data        = json_decode((string)$response->getBody(), true);
            $this->token = $data['token'] ?? null;

            if (!$this->token) {
                $this->_error("AUTH", "No token in login response — check credentials");
                return false;
            }
            $this->_log("  ✓ Authenticated");
            return true;
        } catch (\Exception $e) {
            $this->_error("AUTH", "Authentication failed: " . $e->getMessage());
            return false;
        }
    }

    // =========================================================================
    //  PROJECT CONFIG
    // =========================================================================

    private function _loadProjectDefaults(string $bidsDir): ?array
    {
        $searchPaths = [
            rtrim($bidsDir, '/') . '/project.json',
            dirname(rtrim($bidsDir, '/')) . '/project.json',
            dirname(dirname(rtrim($bidsDir, '/'))) . '/project.json',
        ];
        foreach ($searchPaths as $path) {
            if (!file_exists($path)) continue;
            $data = json_decode(file_get_contents($path), true);
            if (json_last_error() === JSON_ERROR_NONE) {
                $this->_log("  Loaded project.json: {$path}");
                return $data;
            }
            $this->_warn("CONFIG", "Invalid JSON in {$path}: " . json_last_error_msg());
        }
        return null;
    }

    /**
     * Resolve the value to send to CandidatesPlus as ProjectExternalName.
     *
     * Single source: project.json → candidate_defaults.project. The same
     * value is used for the LORIS Candidate.Project field; CandidatesPlus
     * uses it for the external-project lookup too. Returns null if the
     * field isn't set, and the caller falls back to the legacy path.
     */
    private function _getProjectExternalName(): ?string
    {
        $defaults = $this->projectDefaults['candidate_defaults'] ?? [];
        if (!empty($defaults['project'])) {
            return (string)$defaults['project'];
        }
        return null;
    }

    private function _getProjectExternalID(string $projectName): ?string
    {
        // Legacy resolver — used only on the legacy two-step candidate-creation
        // path. CandidatesPlus does the lookup server-side and doesn't need this.
        $defaults = $this->projectDefaults['candidate_defaults'] ?? [];
        if (!empty($defaults['project_external_id'])) {
            return (string)$defaults['project_external_id'];
        }

        if (!empty($this->projectDefaults['project_mappings'])) {
            $mappings = $this->projectDefaults['project_mappings'];
            if (isset($mappings[$projectName])) return (string)$mappings[$projectName];
            $normalise = fn($s) => str_replace([' ', '_'], '-', strtolower(trim($s)));
            foreach ($mappings as $key => $val) {
                if ($normalise($key) === $normalise($projectName)) {
                    $this->_log("  ProjectExternalID: {$val}"
                        . " (fuzzy matched '{$key}' → '{$projectName}')");
                    return (string)$val;
                }
            }
        }

        $configMappings = $this->config['project_mappings'] ?? [];
        if (isset($configMappings[$projectName])) return (string)$configMappings[$projectName];

        $default = $this->config['loris']['project_external_id']
            ?? $this->config['api']['project_external_id']
            ?? null;
        if ($default) return (string)$default;

        try {
            $dbConfig = $this->config['database'] ?? null;
            if ($dbConfig && !empty($dbConfig['host']) && !empty($dbConfig['name'])) {
                $dsn  = "mysql:host={$dbConfig['host']};dbname={$dbConfig['name']};charset=utf8mb4";
                $pdo  = new \PDO($dsn, $dbConfig['user'], $dbConfig['password'] ?? '', [
                    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                ]);
                $stmt = $pdo->prepare("
                    SELECT pe.ProjectExternalID
                    FROM project_external pe
                    JOIN Project p ON pe.ProjectID = p.ProjectID
                    WHERE p.Name = :name OR pe.Name = :name
                    LIMIT 1
                ");
                $stmt->execute(['name' => $projectName]);
                $result = $stmt->fetch(\PDO::FETCH_ASSOC);
                if ($result) return (string)$result['ProjectExternalID'];
            }
        } catch (\Exception $e) {
            $this->_warn("PROJECT_EXTID_DB",
                "DB lookup failed (pipeline may be on a different server than LORIS): "
                . $e->getMessage()
                . " — add project_mappings or candidate_defaults.project_external_id"
                . " to project.json instead."
            );
        }
        return null;
    }

    // =========================================================================
    //  LORIS PROJECT VALIDATION
    // =========================================================================

    private function _fetchLorisProjects(): ?array
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $version = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';

        try {
            $response = $this->_http()->request('GET', "{$baseUrl}/api/{$version}/projects", [
                'headers' => ['Authorization' => "Bearer {$this->token}"],
            ]);
            $data     = json_decode((string)$response->getBody(), true);
            $projects = $data['Projects'] ?? [];
            if (empty($projects)) {
                $this->_error('LORIS_PROJECTS',
                    'No projects returned from LORIS API. Response: ' . json_encode($data)
                );
                return null;
            }
            $names = array_keys($projects);
            $this->_log('  LORIS projects: ' . implode(', ', $names));
            return $names;
        } catch (\Exception $e) {
            $this->_error('LORIS_PROJECTS',
                'Failed to fetch projects from LORIS API: ' . $e->getMessage()
                . ' — check LORIS API is accessible and token is valid.'
            );
            return null;
        }
    }

    // =========================================================================
    //  BIDS PARSING
    // =========================================================================

    private function _parseBidsParticipants(string $bidsDir): array
    {
        $tsvFile = rtrim($bidsDir, '/') . '/participants.tsv';
        if (!file_exists($tsvFile)) {
            $this->_error("PARTICIPANTS_TSV", "File not found: {$tsvFile}");
            return [];
        }
        $handle = fopen($tsvFile, 'r');
        if ($handle === false) {
            $this->_error("PARTICIPANTS_TSV", "Cannot open: {$tsvFile}");
            return [];
        }
        $headerLine = fgets($handle);
        if ($headerLine === false) {
            fclose($handle);
            $this->_error("PARTICIPANTS_TSV", "Empty file: {$tsvFile}");
            return [];
        }
        $headers      = array_map('trim', explode("\t", $headerLine));
        $participants = [];
        $lineNum      = 1;

        while (($line = fgets($handle)) !== false) {
            $lineNum++;
            $line = trim($line);
            if ($line === '') continue;
            $fields = array_map('trim', explode("\t", $line));
            $row    = [];
            foreach ($headers as $i => $header) $row[$header] = $fields[$i] ?? '';
            if (empty($row['participant_id'])) {
                $this->_error("PARTICIPANTS_TSV",
                    "Line {$lineNum}: missing participant_id — skipping row"
                );
                continue;
            }
            $participants[] = $row;
        }
        fclose($handle);
        $this->_log("  Parsed " . count($participants) . " participants from participants.tsv");
        return $participants;
    }

    private function _discoverBidsSubjects(string $bidsDir): array
    {
        $dirs = glob(rtrim($bidsDir, '/') . '/sub-*', GLOB_ONLYDIR) ?: [];
        $subs = array_map('basename', $dirs);
        sort($subs);
        return $subs;
    }

    private function _extractExternalID(array $row): ?string
    {
        foreach (['external_id','ExternalID','externalid','study_id','StudyID',
                     'studyid','ext_study_id','ExtStudyID'] as $col) {
            if (!empty($row[$col])) return trim($row[$col]);
        }
        $pid = $row['participant_id'] ?? '';
        if (str_starts_with($pid, 'sub-')) return substr($pid, 4);
        return null;
    }

    private function _extractSex(array $row): ?string
    {
        $raw = strtolower(trim($row['sex'] ?? $row['Sex'] ?? $row['gender'] ?? ''));
        return match ($raw) {
            'm', 'male'                        => 'Male',
            'f', 'female'                      => 'Female',
            'o', 'other', 'nb', 'non-binary',
            'nonbinary', 'unknown', 'u', 'n/a' => 'Other',
            default                             => null,
        };
    }

    // =========================================================================
    //  CBIGR MAPPER
    // =========================================================================

    private function _lookupExternalIDViaCBIGR(string $externalID): ?array
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        try {
            $response = $this->_http()->request('POST',
                "{$baseUrl}/cbigr_api/externalToInternalIdMapper",
                [
                    'headers' => [
                        'Authorization' => "Bearer {$this->token}",
                        'Content-Type'  => 'application/json',
                    ],
                    'json' => [$externalID],
                ]
            );
            if ($response->getStatusCode() !== 200) return null;

            $lines = explode("\n", trim((string)$response->getBody()));
            if (count($lines) >= 2) {
                $parts = str_getcsv($lines[1]);
                if (count($parts) >= 2) {
                    $pscid = trim($parts[1]);
                    if ($pscid && $pscid !== 'unauthorized_access') {
                        return ['PSCID' => $pscid, 'CandID' => 'exists'];
                    }
                }
            }
            return null;
        } catch (\Exception $e) {
            $msg = $e->getMessage();
            if (str_contains($msg, 'no PSCIDS returned')
                || str_contains($msg, 'Number of returned IDs')
            ) {
                return null;
            }
            $this->_warn("CBIGR_LOOKUP", "Lookup error for {$externalID}: {$msg}");
            return null;
        }
    }

    // =========================================================================
    //  CANDIDATE CREATION — STRATEGY 1: CandidatesPlus (preferred)
    // =========================================================================

    /**
     * Try the CandidatesPlus endpoint, which atomically:
     *   - creates the candidate via the standard /api/{ver}/candidates POST
     *   - resolves ProjectExternalName → ProjectExternalID server-side
     *   - inserts the candidate_project_extid_rel row
     *
     * Returns:
     *   - CandID on success (string, numeric)
     *   - null with ['plus_unavailable' => true] sentinel via the second
     *     argument when the endpoint returns 404 — caller should fall
     *     back to the legacy two-step path
     *   - null with no sentinel for hard failures (bad project name,
     *     duplicate PSCID, malformed payload) — those are real errors
     *     the legacy path can't fix
     */
    private function _createCandidateViaPlus(
        string  $pscid,
        string  $sex,
        string  $site,
        string  $project,
        string  $dob,
        string  $extStudyID,
        string  $projectExternalName,
        array  &$flags
    ): ?string {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url     = "{$baseUrl}/cbigr_api/candidatesPlus";

        $body = ['Candidate' => [
            'PSCID'               => $pscid,
            'Project'             => $project,
            'Site'                => $site,
            'DoB'                 => $dob,
            'Sex'                 => $sex,
            'ExtStudyID'          => $extStudyID,
            'ProjectExternalName' => $projectExternalName,
        ]];

        try {
            $response   = $this->_http()->request('POST', $url, [
                'headers'     => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json'        => $body,
                'http_errors' => false,
            ]);
            $statusCode = $response->getStatusCode();
            $rawBody    = (string)$response->getBody();
            $data       = json_decode($rawBody, true);

            if ($statusCode === 404) {
                // Endpoint not deployed on this LORIS — fall back silently.
                $flags['plus_unavailable'] = true;
                return null;
            }

            if ($statusCode === 201 || $statusCode === 200) {
                $candID = $data['CandID'] ?? $data['Meta']['CandID'] ?? null;
                if ($candID) {
                    $this->_log("  ✓ Created via CandidatesPlus: CandID={$candID} PSCID={$pscid}"
                        . " ExtStudyID={$extStudyID} (relation linked atomically)"
                    );
                    return (string)$candID;
                }
                $this->_error("CREATE_CANDIDATE_PLUS",
                    "CandID missing in CandidatesPlus response for PSCID={$pscid}."
                    . " HTTP {$statusCode}. Body: " . json_encode($data)
                );
                return null;
            }

            // Real error (400, 409, 500, etc) — surface and DON'T fall back.
            // The legacy path would hit the same problem (bad project name,
            // duplicate PSCID, etc), so trying it would just produce two
            // identical errors in the log.
            $this->_error("CREATE_CANDIDATE_PLUS",
                "HTTP {$statusCode} for PSCID={$pscid} ExtStudyID={$extStudyID}."
                . " Body: " . substr($rawBody, 0, 500)
            );
            return null;

        } catch (\Exception $e) {
            // Transport-level error (connection refused, timeout, DNS).
            // Treat as endpoint-unavailable so we fall back rather than
            // double-error.
            $flags['plus_unavailable'] = true;
            $this->_warn("CREATE_CANDIDATE_PLUS",
                "Transport error contacting CandidatesPlus: " . $e->getMessage()
                . " — falling back to legacy path"
            );
            return null;
        }
    }

    // =========================================================================
    //  CANDIDATE CREATION — STRATEGY 2: legacy two-step (fallback)
    // =========================================================================

    private function _createCandidate(
        string  $pscid,
        string  $sex,
        string  $site,
        string  $project,
        string  $dob
    ): ?string {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $version = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';

        $body = ['Candidate' => [
            'PSCID'   => $pscid,
            'Project' => $project,
            'Site'    => $site,
            'DoB'     => $dob,
            'Sex'     => $sex,
        ]];

        try {
            $response   = $this->_http()->request('POST', "{$baseUrl}/api/{$version}/candidates", [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json' => $body,
            ]);
            $statusCode = $response->getStatusCode();
            $data       = json_decode((string)$response->getBody(), true);

            if ($statusCode === 201 || $statusCode === 200) {
                $candID = $data['Meta']['CandID'] ?? $data['CandID'] ?? null;
                if ($candID) {
                    $this->_log("  ✓ Created via legacy: CandID={$candID} PSCID={$pscid}");
                    return (string)$candID;
                }
                $this->_error("CREATE_CANDIDATE",
                    "CandID missing in response for PSCID={$pscid}."
                    . " HTTP {$statusCode}. Body: " . json_encode($data)
                );
                return null;
            }
            $this->_error("CREATE_CANDIDATE",
                "HTTP {$statusCode} for PSCID={$pscid}. Body: " . json_encode($data)
            );
            return null;
        } catch (\Exception $e) {
            $errorBody = '';
            if (method_exists($e, 'getResponse') && $e->getResponse()) {
                $errorBody = (string)$e->getResponse()->getBody();
            }
            $code = method_exists($e, 'getCode') ? $e->getCode() : 0;
            if ($code === 409 || str_contains($e->getMessage(), '409')) {
                $this->_warn("CREATE_CANDIDATE",
                    "PSCID={$pscid} already exists (409) — looking up CandID"
                );
                return $this->_lookupCandidateByPSCID($pscid);
            }
            $this->_error("CREATE_CANDIDATE",
                "Exception for PSCID={$pscid}: " . $e->getMessage()
                . ($errorBody ? " | Response: {$errorBody}" : "")
            );
            return null;
        }
    }

    private function _lookupCandidateByPSCID(string $pscid): ?string
    {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $version = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';
        try {
            $response   = $this->_http()->request('GET', "{$baseUrl}/api/{$version}/candidates", [
                'headers' => ['Authorization' => "Bearer {$this->token}"],
            ]);
            $candidates = json_decode((string)$response->getBody(), true)['Candidates'] ?? [];
            foreach ($candidates as $c) {
                if (($c['PSCID'] ?? '') === $pscid) return (string)($c['CandID'] ?? '');
            }
            $this->_error("LOOKUP_CANDIDATE",
                "PSCID={$pscid} not found in candidate list after 409"
            );
            return null;
        } catch (\Exception $e) {
            $this->_error("LOOKUP_CANDIDATE",
                "Exception looking up PSCID={$pscid}: " . $e->getMessage()
            );
            return null;
        }
    }

    private function _appendExternalID(
        string $candID,
        string $extStudyID,
        string $projectExternalID
    ): bool {
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        try {
            $response   = $this->_http()->request('POST',
                "{$baseUrl}/candidate_parameters/ajax/formHandler.php",
                [
                    'headers'   => ['Authorization' => "Bearer {$this->token}"],
                    'multipart' => [
                        ['name' => 'tab',        'contents' => 'externalIdentifier'],
                        ['name' => 'candID',     'contents' => $candID],
                        ['name' => 'ProjectID',  'contents' => $projectExternalID],
                        ['name' => 'ExtStudyID', 'contents' => $extStudyID],
                    ],
                ]
            );
            $statusCode = $response->getStatusCode();
            $body       = (string)$response->getBody();

            if ($statusCode >= 200 && $statusCode < 300) {
                $this->_log("  ✓ ExternalID linked: {$extStudyID} → CandID {$candID}");
                return true;
            }
            $this->_error("APPEND_EXTID",
                "HTTP {$statusCode} linking ExtStudyID={$extStudyID}"
                . " CandID={$candID}. Response: {$body}"
            );
            return false;
        } catch (\Exception $e) {
            $errorBody = '';
            if (method_exists($e, 'getResponse') && $e->getResponse()) {
                $errorBody = (string)$e->getResponse()->getBody();
            }
            $this->_error("APPEND_EXTID",
                "Exception linking ExtStudyID={$extStudyID} CandID={$candID}: "
                . $e->getMessage()
                . ($errorBody ? " | Response: {$errorBody}" : "")
            );
            return false;
        }
    }

    // =========================================================================
    //  MAIN
    // =========================================================================

    public function run(string $bidsDir, bool $dryRun = false, string $projectPath = ''): array
    {
        if (!$projectPath) $projectPath = dirname(dirname(rtrim($bidsDir, '/')));

        $this->_openLogs($projectPath);
        $this->candidatesPlusAvailable = true; // reset per run

        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  BIDS Participant Sync");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  BIDS dir : {$bidsDir}");
        $this->_log("  Dry run  : " . ($dryRun ? 'YES' : 'NO'));
        $this->_log("───────────────────────────────────────────────────────────");

        if (!is_dir($bidsDir)) {
            $this->_error("BIDS_DIR",
                "Directory not found: {$bidsDir}"
                . " — check deidentified-raw/bids/ exists in project"
            );
            $this->_closeLogs();
            return $this->stats;
        }

        $this->projectDefaults = $this->_loadProjectDefaults($bidsDir);

        if (!$dryRun && !$this->_authenticate()) {
            $this->_closeLogs();
            return $this->stats;
        }

        // ── Validate project exists in LORIS ──────────────────────────────────
        $projectNameForLoris = null;
        if ($this->projectDefaults) {
            $defaults = $this->projectDefaults['candidate_defaults'] ?? [];
            if (!empty($defaults['project'])) {
                $projectNameForLoris = trim($defaults['project']);
            } elseif (!empty($this->projectDefaults['loris_project_name'])) {
                $projectNameForLoris = trim($this->projectDefaults['loris_project_name']);
            } else {
                foreach (['project_common_name', 'project_full_name', 'project'] as $key) {
                    if (!empty($this->projectDefaults[$key])) {
                        $projectNameForLoris = trim($this->projectDefaults[$key]);
                        break;
                    }
                }
            }
        }
        if (!empty($this->config['cli_overrides']['project'])) {
            $projectNameForLoris = trim($this->config['cli_overrides']['project']);
        }

        if ($projectNameForLoris && !$dryRun) {
            $this->_log("  Validating project '{$projectNameForLoris}' exists in LORIS...");
            $lorisProjects = $this->_fetchLorisProjects();
            if ($lorisProjects === null) {
                $this->_closeLogs();
                return $this->stats;
            }
            if (!in_array($projectNameForLoris, $lorisProjects, true)) {
                $this->_error('PROJECT_NOT_FOUND',
                    "Project '{$projectNameForLoris}' not found in LORIS."
                    . ' Available projects: ' . implode(', ', $lorisProjects) . '.'
                    . ' Update project_full_name or project_common_name in project.json'
                    . ' to match a LORIS project name exactly.'
                );
                $this->_closeLogs();
                return $this->stats;
            }
            $this->_log("  ✓ Project '{$projectNameForLoris}' confirmed in LORIS");
        }

        $participants = $this->_parseBidsParticipants($bidsDir);
        $bidsSubjects = $this->_discoverBidsSubjects($bidsDir);

        $this->stats['total_bids_dirs']    = count($bidsSubjects);
        $this->stats['total_participants'] = count($participants);

        $this->_log("  sub-* directories : " . count($bidsSubjects));
        $this->_log("  participants.tsv  : " . count($participants));

        if (empty($participants)) {
            $this->_error("PARTICIPANTS_TSV",
                "No participants found — cannot proceed."
                . " Check participants.tsv exists and has data rows."
            );
            $this->_closeLogs();
            return $this->stats;
        }

        $participantMap = [];
        foreach ($participants as $row) {
            $pid = $row['participant_id'] ?? '';
            if ($pid) $participantMap[$pid] = $row;
        }

        // ── Cross-reference ───────────────────────────────────────────────────
        $this->_log("───────────────────────────────────────────────────────────");
        $this->_log("Validating BIDS structure...");

        foreach ($bidsSubjects as $subDir) {
            if (!isset($participantMap[$subDir])) {
                $this->stats['orphan_directories'][] = $subDir;
                $this->_error("ORPHAN_DIR",
                    "{$subDir} — directory exists in BIDS but NOT in participants.tsv."
                    . " Cannot determine ExtStudyID. Will be skipped by reidentifier."
                );
            }
        }

        foreach ($participantMap as $pid => $row) {
            if (!in_array($pid, $bidsSubjects)) {
                $this->stats['missing_directories'][] = $pid;
                $extID = $this->_extractExternalID($row);
                $this->_error("MISSING_DIR",
                    "{$pid} — listed in participants.tsv"
                    . ($extID ? " (ExtStudyID={$extID})" : "")
                    . " but no sub-* directory found in {$bidsDir}."
                    . " Candidate will be created but no BIDS data exists to reidentify."
                );
            }
        }

        if (empty($this->stats['orphan_directories'])
            && empty($this->stats['missing_directories'])
        ) {
            $this->_log("  ✓ All sub-* directories match participants.tsv");
        }

        // ── Process participants ───────────────────────────────────────────────
        $this->_log("───────────────────────────────────────────────────────────");
        $this->_log("Processing participants...");

        $existingCandidates = [];

        foreach ($participantMap as $subjectId => $row) {
            $this->_log("");
            $this->_log("▸ {$subjectId}");

            $externalID = $this->_extractExternalID($row);
            $sex        = $this->_extractSex($row);
            $site       = trim($row['site']    ?? $row['Site']    ?? '');
            $project    = trim($row['project'] ?? $row['Project'] ?? '');
            $dob        = trim($row['dob']     ?? $row['DoB']     ?? $row['date_of_birth'] ?? '');
            $cohort     = trim($row['cohort']  ?? $row['Cohort']  ?? $row['group']         ?? '');

            $defaults = $this->projectDefaults['candidate_defaults'] ?? [];

            if (!$sex && !empty($defaults['sex'])) {
                $sex = $this->_extractSex(['sex' => $defaults['sex']]);
                if ($sex) $this->_warn('SEX_DEFAULT',
                    "{$subjectId} — no 'sex' in participants.tsv."
                    . " Using candidate_defaults.sex from project.json: '{$sex}'."
                );
            }
            if (!$dob && !empty($defaults['dob'])) {
                $dob = trim($defaults['dob']);
                $this->_warn('DOB_DEFAULT',
                    "{$subjectId} — no 'dob' in participants.tsv."
                    . " Using candidate_defaults.dob from project.json: '{$dob}'."
                );
            }
            if (!$cohort && !empty($defaults['cohort'])) {
                $cohort = trim($defaults['cohort']);
                $this->_log("  Cohort: {$cohort} (from project.json → candidate_defaults.cohort)");
            }
            if (!$cohort && !empty($this->projectDefaults['cohorts'])) {
                $cohort = trim($this->projectDefaults['cohorts'][0]);
                $this->_log("  Cohort: {$cohort} (from project.json → cohorts[0])");
            }
            if (!$site && !empty($defaults['site'])) {
                $site = trim($defaults['site']);
                $this->_warn('SITE_DEFAULT',
                    "{$subjectId} — no 'site' in participants.tsv."
                    . " Using candidate_defaults.site from project.json: '{$site}'."
                );
            }
            if (!$site && !empty($this->projectDefaults['sites'])) {
                $site = trim($this->projectDefaults['sites'][0]);
                $this->_warn('SITE_DEFAULT',
                    "{$subjectId} — no 'site' in participants.tsv."
                    . " Using sites[0] from project.json: '{$site}'."
                );
            }
            if (!$project && !empty($this->config['cli_overrides']['project'])) {
                $project = trim($this->config['cli_overrides']['project']);
                $this->_log("  Project: {$project} (from --project flag)");
            }
            if (!$project && !empty($defaults['project'])) {
                $project = trim($defaults['project']);
                $this->_log("  Project: {$project} (from project.json → candidate_defaults.project)");
            }
            if (!$project && !empty($this->projectDefaults['loris_project_name'])) {
                $project = trim($this->projectDefaults['loris_project_name']);
                $this->_log("  Project: {$project} (from project.json → loris_project_name)");
            }
            if (!$project && !empty($this->projectDefaults['project_common_name'])) {
                $project = trim($this->projectDefaults['project_common_name']);
                $this->_log("  Project: {$project} (from project.json → project_common_name)");
            }
            if (!$project && !empty($this->projectDefaults['project_full_name'])) {
                $project = trim($this->projectDefaults['project_full_name']);
                $this->_warn('PROJECT_FULL_NAME',
                    "{$subjectId} — using project_full_name '{$project}' for LORIS candidate creation."
                    . " This may not match LORIS Project.Name exactly."
                );
            }

            // ── Validate required fields ─────────────────────────────────────
            $missingFields = [];
            if (!$externalID) {
                $missingFields[] = 'external_id (add column to participants.tsv,'
                    . ' or ensure participant_id has sub-XX format)';
            }
            if (!$sex) {
                $rawSex = strtolower(trim($row['sex'] ?? $row['Sex'] ?? $row['gender'] ?? ''));
                if ($rawSex === '') {
                    $sex = 'Other';
                    $this->_warn('SEX_UNKNOWN',
                        "{$subjectId} — no sex; defaulting to 'Other'."
                    );
                } else {
                    $missingFields[] = "sex (found: '{$rawSex}' — unrecognised value)";
                }
            }
            if (!$site) {
                $missingFields[] = 'site (add column to participants.tsv,'
                    . ' or candidate_defaults.site / sites[] to project.json)';
            }
            if (!$dob) {
                $this->_warn('DOB_MISSING',
                    "{$subjectId} — no dob; LORIS may reject candidate creation."
                );
                $dob = null;
            }
            if (!$project) {
                $missingFields[] = 'project (add column to participants.tsv,'
                    . ' use --project flag, or candidate_defaults.project to project.json)';
            }

            if (!empty($missingFields)) {
                $this->_error("MISSING_FIELDS",
                    "{$subjectId} — missing required field(s):\n"
                    . implode("\n", array_map(fn($f) => "    - {$f}", $missingFields))
                    . "\n    Skipping this participant."
                );
                $this->stats['external_id_skipped']++;
                continue;
            }

            $pscid = $externalID;

            // Date normalization
            if ($dob !== null && $dob !== '') {
                $dobOriginal = $dob;
                $dob         = $this->_normalizeDateValue($dob);
                if ($dob !== $dobOriginal) {
                    $this->_log("  DoB normalized: {$dobOriginal} → {$dob} (YYYY-MM-01 policy)");
                }
            }

            $this->_log("  ExtStudyID : {$externalID}");
            $this->_log("  Project    : {$project}");
            $this->_log("  Sex / Site : {$sex} / {$site}");
            if ($cohort) $this->_log("  Cohort     : {$cohort}");

            // Step 1: Already mapped in CBIGR?
            if (!$dryRun) {
                $cbigrResult = $this->_lookupExternalIDViaCBIGR($externalID);
                if ($cbigrResult) {
                    $this->_log("  ✓ Already mapped: {$externalID} → PSCID={$cbigrResult['PSCID']}");
                    $this->stats['already_exists']++;
                    $this->stats['external_id_skipped']++;
                    continue;
                }
            }

            // Step 2: Create candidate (CandidatesPlus first, legacy fallback)
            $candID = $existingCandidates[$pscid] ?? null;

            if ($candID) {
                $this->_log("  ✓ Candidate cached: CandID={$candID}");
                $this->stats['already_exists']++;
            } else {
                if ($dryRun) {
                    $this->_log("  [DRY-RUN] Would create candidate: PSCID={$pscid}");
                    $this->stats['created']++;
                    $this->_log("  [DRY-RUN] Would link ExtStudyID: {$externalID}");
                    $this->stats['external_id_linked']++;
                    continue;
                }

                $createdViaPlus = false;

                // STRATEGY 1 — CandidatesPlus
                if ($this->candidatesPlusAvailable) {
                    $projectExternalName = $this->_getProjectExternalName();
                    if ($projectExternalName) {
                        $flags = [];
                        $candID = $this->_createCandidateViaPlus(
                            $pscid, $sex, $site, $project, $dob ?? '',
                            $externalID, $projectExternalName, $flags
                        );
                        if (!empty($flags['plus_unavailable'])) {
                            $this->_warn("CANDIDATES_PLUS",
                                "CandidatesPlus endpoint unavailable on this LORIS server"
                                . " — falling back to legacy candidate-creation path"
                                . " for the rest of this run"
                            );
                            $this->candidatesPlusAvailable = false;
                            $candID = null;
                        } elseif ($candID) {
                            $createdViaPlus = true;
                            $this->stats['created_via_plus']++;
                            $this->stats['created']++;
                            $this->stats['external_id_linked']++; // atomic with creation
                            $existingCandidates[$pscid] = $candID;
                            continue; // CandidatesPlus done — no _appendExternalID needed
                        } else {
                            // Hard failure (bad project name, duplicate PSCID, etc).
                            // Don't fall through — legacy will fail the same way.
                            $this->stats['external_id_skipped']++;
                            continue;
                        }
                    } else {
                        $this->_warn("CANDIDATES_PLUS",
                            "{$subjectId} — candidate_defaults.project not set in project.json,"
                            . " cannot use CandidatesPlus path;"
                            . " falling back to legacy"
                        );
                    }
                }

                // STRATEGY 2 — legacy two-step
                $candID = $this->_createCandidate($pscid, $sex, $site, $project, $dob ?? '');
                if (!$candID) continue;

                $this->stats['created_via_legacy']++;
                $this->stats['created']++;
                $existingCandidates[$pscid] = $candID;

                // Step 3 (legacy only): Link ExternalID via formHandler
                $projectExternalID = $this->_getProjectExternalID($project);
                if (!$projectExternalID) {
                    $this->_error("PROJECT_EXTID",
                        "{$subjectId} — cannot determine ProjectExternalID for project '{$project}'"
                        . " on legacy path. Candidate created (CandID={$candID})"
                        . " but external-ID relation NOT linked."
                        . " Add candidate_defaults.project_external_id to project.json,"
                        . " or upgrade LORIS server to one with CandidatesPlus."
                    );
                    continue;
                }

                if ($this->_appendExternalID($candID, $externalID, $projectExternalID)) {
                    $this->stats['external_id_linked']++;
                }
            }
        }

        $this->_printSummary();
        $this->_closeLogs();
        return $this->stats;
    }

    // =========================================================================
    //  SUMMARY
    // =========================================================================

    private function _printSummary(): void
    {
        $errCount     = count($this->stats['errors']);
        $orphanCount  = count($this->stats['orphan_directories']);
        $missingCount = count($this->stats['missing_directories']);

        $this->_log("");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  SYNC SUMMARY");
        $this->_log("═══════════════════════════════════════════════════════════");
        $this->_log("  sub-* directories  : {$this->stats['total_bids_dirs']}");
        $this->_log("  participants.tsv   : {$this->stats['total_participants']}");
        $this->_log("───────────────────────────────────────────────────────────");
        $this->_log("  Already mapped     : {$this->stats['already_exists']}");
        $this->_log("  Newly created      : {$this->stats['created']}"
            . "  (CandidatesPlus: {$this->stats['created_via_plus']},"
            . " legacy: {$this->stats['created_via_legacy']})"
        );
        $this->_log("  ExternalIDs linked : {$this->stats['external_id_linked']}");
        $this->_log("  ExternalIDs skipped: {$this->stats['external_id_skipped']}");
        $this->_log("───────────────────────────────────────────────────────────");
        $this->_log("  Orphan directories : {$orphanCount}"
            . ($orphanCount > 0 ? ' ← see error log' : ''));
        $this->_log("  Missing directories: {$missingCount}"
            . ($missingCount > 0 ? ' ← see error log' : ''));
        $this->_log("  Total errors       : {$errCount}");
        $this->_log("───────────────────────────────────────────────────────────");
        if ($this->runLogPath)   $this->_log("  Run log  : {$this->runLogPath}");
        if ($this->errorLogPath) $this->_log("  Error log: {$this->errorLogPath}");
        $this->_log("  Result: "
            . ($errCount > 0 ? 'COMPLETED WITH ERRORS' : 'COMPLETED SUCCESSFULLY')
        );
        $this->_log("═══════════════════════════════════════════════════════════");

        $this->stats['orphan_count']  = $orphanCount;
        $this->stats['missing_count'] = $missingCount;
    }

    public function getStats(): array { return $this->stats; }
}