<?php
declare(strict_types=1);

namespace LORIS\Endpoints;

use GuzzleHttp\Client as GuzzleClient;
use Psr\Log\LoggerInterface;
use SplFileObject;

/**
 * ClinicalClient — LORIS API communication layer.
 *
 * Strategy:
 *   Priority 1 → loris-php-api-client (LORISClient\Api\*)
 *   Priority 2 → Direct HTTP via Guzzle (fallback)
 *
 * Server configuration (from loris-php-api-client README):
 *   REST API endpoints: setHost("{$baseUrl}/api/{$version}")  → /login, /candidates, /projects, /sites
 *   Module endpoints:   setHost($baseUrl)                     → /instrument_manager/*
 *
 * Ref: https://github.com/aces/loris-php-api-client
 *
 * CHANGELOG (instrument_manager im3 — explicit parameters):
 *   - Install: sends instrument_type field (bids/linst/redcap) — DD format selector
 *       Client auto-detects from extension: .csv → redcap, .linst → linst, .json → bids
 *   - Data upload: sends 'format' field (LORIS_CSV/REDCAP_CSV/BIDS_TSV)
 *       Client auto-detects: .tsv → BIDS_TSV, CSV with redcap_event_name → REDCAP_CSV, else → LORIS_CSV
 *   - Data upload: sends 'strict' field (bool string) — column validation mode
 *       strict=true  → all template columns must be present
 *       strict=false → non-essential columns may be missing (pipeline default)
 *   - GET template: sends 'format' query param (LORIS_CSV/REDCAP_CSV/BIDS_TSV)
 *   - Client-side helpers:
 *       detectInstrumentType() — auto-detect DD type from extension
 *       detectDataFormat()     — auto-detect data format from content/extension
 *       isRedcapCSV()          — checks for redcap_event_name column
 *       validateColumns()      — pre-flight column check with format-aware delimiters
 *
 * @package LORIS\Endpoints
 */
class ClinicalClient
{
    private string          $baseUrl;
    private string          $apiVersion;
    private LoggerInterface $logger;
    private GuzzleClient    $httpClient;
    private ?string         $token = null;

    private string $username;
    private string $password;
    private int    $tokenExpiryMinutes;
    private ?int   $tokenExpiry = null;

    private bool  $hasApiClient   = false;
    private mixed $apiConfig      = null;
    private mixed $moduleConfig   = null;

    private array $apiVersions;
    private ?string $activeVersion = null;

    private ?array $installedInstruments = null;

    // im3: DD type map — extension → instrument_type value
    private const DD_TYPE_MAP = [
        'csv'  => 'redcap',
        'linst' => 'linst',
        'json' => 'bids',
    ];

    // ──────────────────────────────────────────────────────────────────
    // CONSTRUCTOR
    // ──────────────────────────────────────────────────────────────────

    public function __construct(
        string $baseUrl,
        string $username,
        string $password,
        int    $tokenExpiryMinutes = 55,
        ?LoggerInterface $logger = null,
        string $apiVersion = 'v0.0.4-dev'
    ) {
        $this->baseUrl            = rtrim($baseUrl, '/');
        $this->username           = $username;
        $this->password           = $password;
        $this->apiVersion         = $apiVersion;
        $this->logger             = $logger ?? new \Psr\Log\NullLogger();
        $this->tokenExpiryMinutes = $tokenExpiryMinutes;
        $this->apiVersions        = [$apiVersion];

        $this->httpClient = new GuzzleClient([
            'timeout' => 30,
            'verify'  => true,
        ]);

        if (class_exists(\LORISClient\Configuration::class)
            && class_exists(\LORISClient\Api\AuthenticationApi::class)
        ) {
            $this->initApiClient();
        } else {
            $this->logger->info("loris-php-api-client not installed — HTTP fallback only");
        }
    }

    private function initApiClient(): void
    {
        try {
            $this->registerMissingModels();

            $this->apiConfig = new \LORISClient\Configuration();
            $this->apiConfig->setHost("{$this->baseUrl}/api/{$this->apiVersion}");

            $this->moduleConfig = new \LORISClient\Configuration();
            $this->moduleConfig->setHost($this->baseUrl);

            $this->hasApiClient = true;
            $this->logger->info("loris-php-api-client initialized ✓");
        } catch (\Error|\Exception $e) {
            $this->hasApiClient = false;
            $this->logger->warning("API client init failed: " . $e->getMessage());
        }
    }

    /**
     * Register missing model classes for loris-php-api-client.
     *
     * The OpenAPI schema uses inline response objects for 200/201 responses,
     * which causes the generator to reference auto-named classes that it
     * never actually creates. This registers stub classes so the API client
     * can deserialize responses without crashing. Lives in pipeline code so
     * it survives client regeneration via ./generate.sh.
     */
    private function registerMissingModels(): void
    {
        $models = [
            'UploadInstrumentData201Response',
            'UploadInstrumentData200Response',
            'InstallInstrument201Response',
            'GetInstrumentDataHeaders200Response',
        ];

        foreach ($models as $shortName) {
            $fqcn = "LORISClient\\Model\\{$shortName}";
            if (!class_exists($fqcn, true)) {
                $code = <<<PHPCODE
namespace LORISClient\Model;

class {$shortName} implements \ArrayAccess, \JsonSerializable {
    public const DISCRIMINATOR = null;
    protected static string \$openAPIModelName = '{$shortName}';
    protected static array \$openAPITypes = ['success'=>'bool','message'=>'string','ok'=>'string','idMapping'=>'object'];
    protected static array \$openAPIFormats = ['success'=>null,'message'=>null,'ok'=>null,'idMapping'=>null];
    protected static array \$openAPINullables = ['success'=>false,'message'=>false,'ok'=>false,'idMapping'=>false];
    protected static array \$attributeMap = ['success'=>'success','message'=>'message','ok'=>'ok','idMapping'=>'idMapping'];
    protected static array \$setters = ['success'=>'setSuccess','message'=>'setMessage','ok'=>'setOk','idMapping'=>'setIdMapping'];
    protected static array \$getters = ['success'=>'getSuccess','message'=>'getMessage','ok'=>'getOk','idMapping'=>'getIdMapping'];
    protected array \$container = [];
    protected array \$openAPINullablesSetToNull = [];

    public function __construct(?array \$data = null) {
        \$this->container['success']=\$data['success']??null;
        \$this->container['message']=\$data['message']??null;
        \$this->container['ok']=\$data['ok']??null;
        \$this->container['idMapping']=\$data['idMapping']??null;
    }

    public static function openAPITypes(): array { return static::\$openAPITypes; }
    public static function openAPIFormats(): array { return static::\$openAPIFormats; }
    public static function openAPINullables(): array { return static::\$openAPINullables; }
    public static function isNullable(string \$p): bool { return static::\$openAPINullables[\$p] ?? false; }
    public function isNullableSetToNull(string \$p): bool { return in_array(\$p, \$this->openAPINullablesSetToNull, true); }
    public static function attributeMap(): array { return static::\$attributeMap; }
    public static function setters(): array { return static::\$setters; }
    public static function getters(): array { return static::\$getters; }
    public function getModelName(): string { return static::\$openAPIModelName; }
    public function listInvalidProperties(): array { return []; }
    public function valid(): bool { return true; }
    public function getSuccess(): ?bool { return \$this->container['success']; }
    public function setSuccess(?bool \$v): static { \$this->container['success']=\$v; return \$this; }
    public function getMessage(): ?string { return \$this->container['message']; }
    public function setMessage(?string \$v): static { \$this->container['message']=\$v; return \$this; }
    public function getOk(): ?string { return \$this->container['ok']; }
    public function setOk(?string \$v): static { \$this->container['ok']=\$v; return \$this; }
    public function getIdMapping(): mixed { return \$this->container['idMapping']; }
    public function setIdMapping(mixed \$v): static { \$this->container['idMapping']=\$v; return \$this; }
    public function offsetExists(mixed \$o): bool { return isset(\$this->container[\$o]); }
    public function offsetGet(mixed \$o): mixed { return \$this->container[\$o] ?? null; }
    public function offsetSet(mixed \$o, mixed \$v): void { if(\$o!==null) \$this->container[\$o]=\$v; }
    public function offsetUnset(mixed \$o): void { unset(\$this->container[\$o]); }
    public function jsonSerialize(): mixed { return \$this->container; }
    public function __toString(): string { return json_encode(\$this->container, JSON_PRETTY_PRINT) ?: ''; }
}
PHPCODE;
                eval($code);
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // AUTHENTICATION  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    public function authenticate(): string
    {
        $this->logger->info("Authenticating with LORIS at {$this->baseUrl}");

        if ($this->hasApiClient) {
            try {
                foreach ($this->apiVersions as $version) {
                    $this->logger->info("  Trying API version: {$version}");
                    try {
                        $this->apiConfig = new \LORISClient\Configuration();
                        $this->apiConfig->setHost("{$this->baseUrl}/api/{$version}");

                        $authApi  = new \LORISClient\Api\AuthenticationApi($this->httpClient, $this->apiConfig);
                        $response = $authApi->login(new \LORISClient\Model\LoginRequest([
                            'username' => $this->username, 'password' => $this->password,
                        ]));
                        $this->token = $response->getToken();

                        if (!empty($this->token)) {
                            $this->apiConfig->setAccessToken($this->token);
                            $this->moduleConfig->setAccessToken($this->token);
                            $this->tokenExpiry   = time() + ($this->tokenExpiryMinutes * 60);
                            $this->activeVersion = $version;
                            $this->logger->info("  ✓ Authenticated via API client (version: {$version})");
                            return $this->token;
                        }
                    } catch (\Exception $e) {
                        $this->logger->warning("    ✗ API auth failed ({$version}): " . $e->getMessage());
                    }
                }
                $this->logger->info("  API client auth exhausted — falling back to HTTP...");
            } catch (\Error|\Exception $e) {
                $this->logger->warning("  API auth error: " . $e->getMessage());
            }
        }

        return $this->authenticateHttp();
    }

    private function authenticateHttp(): string
    {
        $lastError = '';
        foreach ($this->apiVersions as $version) {
            $url = "{$this->baseUrl}/api/{$version}/login";
            try {
                $response = $this->httpClient->request('POST', $url, [
                    'json' => ['username' => $this->username, 'password' => $this->password],
                    'http_errors' => false,
                ]);
                if ($response->getStatusCode() !== 200) {
                    $lastError = "HTTP {$response->getStatusCode()}";
                    continue;
                }
                $data = json_decode((string) $response->getBody(), true);
                $this->token = $data['token'] ?? null;
                if (!empty($this->token)) {
                    $this->activeVersion = $version;
                    $this->tokenExpiry   = time() + ($this->tokenExpiryMinutes * 60);
                    $this->apiConfig?->setAccessToken($this->token);
                    $this->moduleConfig?->setAccessToken($this->token);
                    $this->logger->info("  ✓ Authenticated via HTTP (version: {$version})");
                    return $this->token;
                }
                $lastError = "No token in response";
            } catch (\Exception $e) {
                $lastError = $e->getMessage();
            }
        }
        throw new \RuntimeException("All authentication methods failed. Last error: {$lastError}");
    }

    public function refreshTokenIfNeeded(): void
    {
        if ($this->tokenExpiry !== null && time() >= ($this->tokenExpiry - 60)) {
            $this->logger->info("  Token near expiry — refreshing...");
            $this->authenticate();
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // FORMAT DETECTION & COLUMN VALIDATION HELPERS
    // ──────────────────────────────────────────────────────────────────

    /**
     * Detect instrument type from DD file extension.
     *
     * im3: Client sends instrument_type field. This auto-detects the value.
     *
     * @param string $filePath  Path to DD file
     * @return string  bids, linst, or redcap
     * @throws \RuntimeException if extension not recognized
     */
    public function detectInstrumentType(string $filePath): string
    {
        $ext = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));

        if (!isset(self::DD_TYPE_MAP[$ext])) {
            throw new \RuntimeException(
                "Unknown instrument type: " . basename($filePath)
                . " (extension .{$ext} not in DD_TYPE_MAP)"
            );
        }

        return self::DD_TYPE_MAP[$ext];
    }

    /**
     * Detect data format from file content and extension.
     *
     * im3: Client sends format field. This auto-detects the value.
     *   .tsv extension             → BIDS_TSV
     *   CSV with redcap_event_name → REDCAP_CSV
     *   otherwise                  → LORIS_CSV
     *
     * @param string $filePath  Path to data file
     * @return string  BIDS_TSV, REDCAP_CSV, or LORIS_CSV
     */
    public function detectDataFormat(string $filePath): string
    {
        $ext = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));

        if ($ext === 'tsv') {
            return 'BIDS_TSV';
        }

        if ($this->isRedcapCSV($filePath)) {
            return 'REDCAP_CSV';
        }

        return 'LORIS_CSV';
    }

    /**
     * Check if a CSV file is REDCap format (has redcap_event_name column).
     *
     * im3: Used by detectDataFormat() for auto-detection.
     * Also available directly for pipeline informational logging.
     */
    public function isRedcapCSV(string $csvFilePath): bool
    {
        $fh = fopen($csvFilePath, 'r');
        if ($fh === false) {
            return false;
        }
        $headerLine = fgets($fh);
        fclose($fh);

        if ($headerLine !== false) {
            $columns = array_map('trim', str_getcsv(trim($headerLine)));
            return in_array('redcap_event_name', $columns, true);
        }

        return false;
    }

    /**
     * Pre-flight column validation against LORIS expected template.
     *
     * im3: Detects format from file, requests matching template with format param,
     * and uses correct delimiter (tab for TSV, comma for CSV).
     *
     * @return array{valid: bool, missing_essential: string[], missing_nonessential: string[], extra: string[], warnings: string[]}
     */
    public function validateColumns(
        string $csvFilePath,
        string $instrument,
        string $action = 'CREATE_SESSIONS'
    ): array {
        $result = [
            'valid' => true, 'missing_essential' => [], 'missing_nonessential' => [],
            'extra' => [], 'warnings' => [],
        ];

        // im3: Detect format to request matching template
        $format = $this->detectDataFormat($csvFilePath);

        $expectedCsv = $this->getInstrumentDataHeaders($instrument, $action, $format);
        if ($expectedCsv === null) {
            $result['warnings'][] = "Could not fetch expected headers for {$instrument} — skipping column validation";
            return $result;
        }

        // im3: Use correct delimiter based on format
        $delimiter = ($format === 'BIDS_TSV') ? "\t" : ',';

        $expectedHeaders = array_map('trim', str_getcsv(trim($expectedCsv), $delimiter));

        $fh = fopen($csvFilePath, 'r');
        if ($fh === false) {
            $result['valid'] = false;
            $result['warnings'][] = "Could not open {$csvFilePath}";
            return $result;
        }
        $headerLine = fgets($fh);
        fclose($fh);

        if ($headerLine === false) {
            $result['valid'] = false;
            $result['warnings'][] = "Empty file: {$csvFilePath}";
            return $result;
        }

        // im3: Parse with format-appropriate delimiter
        $actualHeaders = array_map('trim', str_getcsv(trim($headerLine), $delimiter));

        // Essential columns: only the structural/identifier columns that LORIS
        // requires to locate or create the correct candidate/session/instrument.
        // All instrument data fields are optional — LORIS accepts any subset.
        $essentialColumns = [
            // Candidate identifiers
            'study_id', 'StudyID', 'PSCID', 'CandID', 'candid',
            // Session identifiers
            'visit_label', 'Visit_label',
            // Demographics (needed for CREATE_SESSIONS)
            'dob', 'DoB', 'sex', 'Sex',
            // Site/project/cohort
            'project', 'Project', 'site', 'Site', 'cohort', 'Cohort',
            // REDCap structural
            'redcap_event_name', 'redcap_repeat_instrument', 'redcap_repeat_instance',
            // BIDS structural
            'participant_id', 'session_id',
        ];

        $missingFromFile = array_diff($expectedHeaders, $actualHeaders);
        $extraInFile     = array_diff($actualHeaders, $expectedHeaders);

        foreach ($missingFromFile as $col) {
            if (in_array($col, $essentialColumns, true)) {
                $result['missing_essential'][] = $col;
            } else {
                $result['missing_nonessential'][] = $col;
            }
        }

        // Known extra columns from REDCap exports — not flagged
        $knownExtraColumns = [
            'redcap_event_name', 'redcap_repeat_instrument',
            'redcap_repeat_instance', 'redcap_data_access_group',
        ];
        foreach ($extraInFile as $col) {
            if (in_array($col, $knownExtraColumns, true) || str_ends_with($col, '_complete')) {
                continue;
            }
            $result['extra'][] = $col;
        }

        if (!empty($result['missing_essential'])) {
            $result['valid'] = false;
        }

        return $result;
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENT DATA UPLOAD  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    /**
     * Upload instrument data (single instrument).
     *
     * im3: Sends action, format, strict, instrument, data_file.
     * Format is auto-detected via detectDataFormat().
     * Pipeline always sends strict=false.
     *
     * @param string $instrument  Instrument name
     * @param string $csvFilePath Path to CSV/TSV file
     * @param string $action      CREATE_SESSIONS or VALIDATE_SESSIONS
     * @param bool   $strict      true=all columns required, false=non-essential may be missing
     * @return array  [success, message, idMapping, method]
     */
    public function uploadInstrumentData(
        string      $instrument,
        string      $csvFilePath,
        string      $action = 'CREATE_SESSIONS',
        string|bool $strict = false
    ): array {
        // Normalize: accept bool or string ('true'/'false') for backward compat
        if (is_string($strict)) {
            $strict = ($strict === 'true');
        }
        $format   = $this->detectDataFormat($csvFilePath);
        $isRedcap = ($format === 'REDCAP_CSV');

        $this->logger->info("  Uploading {$instrument} data ({$action}, format={$format}, strict=" . ($strict ? 'true' : 'false') . ")");

        // Pre-flight column validation
        try {
            $validation = $this->validateColumns($csvFilePath, $instrument, $action);
            if (!$validation['valid']) {
                $this->logger->error("    ✗ [{$instrument}] Missing REQUIRED columns: " . implode(', ', $validation['missing_essential']));
                return [
                    'success' => false,
                    'message' => "Missing required columns for {$instrument}: "
                        . implode(', ', $validation['missing_essential']),
                    'idMapping' => [],
                    'method'    => 'PREFLIGHT',
                ];
            }
            if (!empty($validation['missing_nonessential'])) {
                $this->logger->warning("    ⚠ [{$instrument}] Missing optional columns (" . count($validation['missing_nonessential']) . "): "
                    . implode(', ', array_slice($validation['missing_nonessential'], 0, 10))
                    . (count($validation['missing_nonessential']) > 10 ? ' …' : ''));
            }
        } catch (\Exception $e) {
            $this->logger->debug("    Pre-flight validation skipped: " . $e->getMessage());
        }

        $startTime = microtime(true);

        // --- Priority 1: API client ---
        if ($this->hasApiClient && $this->moduleConfig !== null) {
            try {
                $this->logger->debug("    Trying API client (InstrumentManagerApi)...");

                $api     = new \LORISClient\Api\InstrumentManagerApi($this->httpClient, $this->moduleConfig);
                $fileObj = new SplFileObject($csvFilePath, 'r');

                // im3 Generated signature (from schema required-first order):
                //   uploadInstrumentData($action, $format, $data_file, $strict?, $instrument?, $multi_instrument?)
                $result = $api->uploadInstrumentData(
                    $action,                        // action (required)
                    $format,                        // format (required)
                    $fileObj,                       // data_file (required)
                    $strict ? 'true' : 'false',     // strict (optional)
                    $instrument,                    // instrument (optional)
                    null                            // multi_instrument (optional)
                );

                $elapsed = round(microtime(true) - $startTime, 2);
                $this->logger->info("    Upload completed in {$elapsed}s");

                $parsed           = $this->parseApiUploadResult($result);
                $parsed['method'] = 'API';

                if ($parsed['success']) {
                    $this->logger->info("    ✓ Upload successful via API client");
                    $this->logIdMapping($parsed);
                    return $parsed;
                }

                $this->logger->warning("    API upload returned failure: " . ($parsed['message'] ?? 'unknown'));
            } catch (\Error $e) {
                $elapsed = round(microtime(true) - $startTime, 2);
                $this->logger->warning("    API upload error ({$elapsed}s): " . $e->getMessage());
                $this->logger->info("    Falling back to HTTP...");
            } catch (\Exception $e) {
                $this->logger->warning("    API client upload failed: " . $e->getMessage());
                $this->logger->info("    Falling back to HTTP...");
            }
        }

        // --- Priority 2: Direct HTTP fallback ---
        return $this->uploadInstrumentDataHttp($instrument, $csvFilePath, $action, $format, $strict);
    }

    /**
     * Upload multi-instrument data from a single CSV.
     *
     * im3: Sends action, format, strict, multi-instrument (JSON), data_file.
     */
    public function uploadMultiInstrumentData(
        array       $instrumentNames,
        string      $csvFilePath,
        string      $action = 'CREATE_SESSIONS',
        string|bool $strict = false
    ): array {
        // Normalize: accept bool or string ('true'/'false') for backward compat with pipeline callers
        if (is_string($strict)) {
            $strict = ($strict === 'true');
        }
        $count  = count($instrumentNames);
        $format = $this->detectDataFormat($csvFilePath);

        $this->logger->info("  Uploading multi-instrument data ({$count} instruments, format={$format}, strict=" . ($strict ? 'true' : 'false') . ")");

        // Pre-flight column validation per instrument
        foreach ($instrumentNames as $instName) {
            try {
                $validation = $this->validateColumns($csvFilePath, $instName, $action);
                if (!$validation['valid']) {
                    $this->logger->error("    ✗ [{$instName}] Missing REQUIRED columns: " . implode(', ', $validation['missing_essential']));
                }
                if (!empty($validation['missing_nonessential'])) {
                    $this->logger->warning("    ⚠ [{$instName}] Missing optional columns (" . count($validation['missing_nonessential']) . "): "
                        . implode(', ', array_slice($validation['missing_nonessential'], 0, 10))
                        . (count($validation['missing_nonessential']) > 10 ? ' …' : ''));
                }
            } catch (\Exception $e) {
                $this->logger->debug("    Pre-flight validation skipped for {$instName}: " . $e->getMessage());
            }
        }

        $multiJson = json_encode(
            array_map(fn(string $name) => ['value' => $name], $instrumentNames)
        );

        // --- Priority 1: API client config (direct HTTP with correct field name) ---
        // The generated API client sends 'multi_instrument' (underscore) but LORIS
        // expects 'multi-instrument' (hyphen). We use the API client's authenticated
        // config but build the multipart request ourselves with the correct field name.
        if ($this->hasApiClient && $this->moduleConfig !== null) {
            try {
                $this->logger->debug("    Trying API path (multi-instrument)...");

                $startTime = microtime(true);
                $url = $this->moduleConfig->getHost() . '/instrument_manager/instrument_data';

                $response = $this->httpClient->request('POST', $url, [
                    'headers'   => ['Authorization' => 'Bearer ' . $this->moduleConfig->getAccessToken()],
                    'multipart' => [
                        ['name' => 'action',           'contents' => $action],
                        ['name' => 'format',           'contents' => $format],
                        ['name' => 'strict',           'contents' => $strict ? 'true' : 'false'],
                        ['name' => 'multi-instrument', 'contents' => $multiJson],
                        [
                            'name'     => 'data_file',
                            'contents' => fopen($csvFilePath, 'r'),
                            'filename' => basename($csvFilePath),
                        ],
                    ],
                    'http_errors' => false,
                ]);

                $elapsed    = round(microtime(true) - $startTime, 2);
                $statusCode = $response->getStatusCode();
                $data       = json_decode((string) $response->getBody(), true) ?? [];

                if ($statusCode >= 200 && $statusCode < 300) {
                    $parsed = [
                        'success'   => $data['success'] ?? true,
                        'message'   => $data['message'] ?? 'OK',
                        'idMapping' => $data['idMapping'] ?? [],
                        'method'    => 'API',
                    ];
                    if ($parsed['success']) {
                        $this->logger->info("    ✓ Multi-instrument upload successful via API ({$elapsed}s)");
                        $this->logIdMapping($parsed);
                    }
                    return $parsed;
                }

                $this->logger->warning("    API multi-instrument returned HTTP {$statusCode}");
            } catch (\Error $e) {
                $this->logger->warning("    API multi-instrument error: " . $e->getMessage());
                $this->logger->info("    Falling back to HTTP...");
            } catch (\Exception $e) {
                $this->logger->warning("    API multi-instrument failed: " . $e->getMessage());
                $this->logger->info("    Falling back to HTTP...");
            }
        }

        // --- Priority 2: Direct HTTP fallback ---
        return $this->uploadMultiInstrumentDataHttp(
            $instrumentNames, $csvFilePath, $action, $multiJson, $format, $strict
        );
    }

    /**
     * HTTP fallback for single-instrument data upload.
     * im3: Sends action, format, strict, instrument, data_file.
     */
    private function uploadInstrumentDataHttp(
        string $instrument,
        string $csvFilePath,
        string $action,
        string $format,
        bool   $strict
    ): array {
        $url = "{$this->baseUrl}/instrument_manager/instrument_data";

        try {
            $this->logger->debug("    HTTP POST {$url}");
            $startTime = microtime(true);

            $response = $this->httpClient->request('POST', $url, [
                'headers'   => ['Authorization' => "Bearer {$this->token}"],
                'multipart' => [
                    ['name' => 'action',     'contents' => $action],
                    ['name' => 'format',     'contents' => $format],
                    ['name' => 'strict',     'contents' => $strict ? 'true' : 'false'],
                    ['name' => 'instrument', 'contents' => $instrument],
                    [
                        'name'     => 'data_file',
                        'contents' => fopen($csvFilePath, 'r'),
                        'filename' => basename($csvFilePath),
                    ],
                ],
                'http_errors' => false,
            ]);

            $elapsed    = round(microtime(true) - $startTime, 2);
            $statusCode = $response->getStatusCode();
            $rawBody    = (string) $response->getBody();
            $data       = json_decode($rawBody, true) ?? [];

            $this->logger->info("    Upload completed in {$elapsed}s (HTTP {$statusCode})");

            if ($statusCode >= 200 && $statusCode < 300) {
                $result = [
                    'success'   => true,
                    'message'   => $data['message'] ?? 'OK',
                    'idMapping' => $data['idMapping'] ?? [],
                    'data'      => $data,
                    'method'    => 'HTTP',
                ];
                $this->logger->info("    ✓ Upload successful via HTTP");
                $this->logIdMapping($result);
                return $result;
            }

            return [
                'success' => false, 'message' => $data['error'] ?? "HTTP {$statusCode}",
                'idMapping' => [], 'method' => 'HTTP',
            ];
        } catch (\Exception $e) {
            $this->logger->error("    ✗ HTTP upload failed: " . $e->getMessage());
            return ['success' => false, 'message' => $e->getMessage(), 'idMapping' => [], 'method' => 'HTTP'];
        }
    }

    /**
     * HTTP fallback for multi-instrument data upload.
     *
     * im3: Sends action, format, strict, multi-instrument (JSON), data_file.
     * Form field name is "multi-instrument" (with hyphen) matching
     * instrumentManagerIndex.js and instrument_data.class.inc.
     */
    private function uploadMultiInstrumentDataHttp(
        array   $instrumentNames,
        string  $csvFilePath,
        string  $action,
        ?string $multiJson = null,
        string  $format = 'LORIS_CSV',
        bool    $strict = false
    ): array {
        $url = "{$this->baseUrl}/instrument_manager/instrument_data";

        if ($multiJson === null) {
            $multiJson = json_encode(
                array_map(fn(string $name) => ['value' => $name], $instrumentNames)
            );
        }

        try {
            $this->refreshTokenIfNeeded();
            $this->logger->debug("    HTTP POST {$url} (multi-instrument)");
            $this->logger->debug("    Instruments: " . implode(', ', $instrumentNames));

            $startTime = microtime(true);

            $response = $this->httpClient->request('POST', $url, [
                'headers'   => ['Authorization' => "Bearer {$this->token}"],
                'multipart' => [
                    ['name' => 'action',           'contents' => $action],
                    ['name' => 'format',           'contents' => $format],
                    ['name' => 'strict',           'contents' => $strict ? 'true' : 'false'],
                    ['name' => 'multi-instrument', 'contents' => $multiJson],
                    [
                        'name'     => 'data_file',
                        'contents' => fopen($csvFilePath, 'r'),
                        'filename' => basename($csvFilePath),
                    ],
                ],
                'http_errors' => false,
            ]);

            $elapsed    = round(microtime(true) - $startTime, 2);
            $statusCode = $response->getStatusCode();
            $rawBody    = (string) $response->getBody();
            $data       = json_decode($rawBody, true) ?? [];

            $this->logger->debug("    Response: HTTP {$statusCode} ({$elapsed}s)");

            if ($statusCode >= 200 && $statusCode < 300) {
                $result = [
                    'success'   => $data['success'] ?? true,
                    'message'   => $data['message'] ?? 'OK',
                    'idMapping' => $data['idMapping'] ?? [],
                    'method'    => 'HTTP',
                ];
                if ($result['success']) {
                    $this->logger->info("    ✓ Multi-instrument upload successful via HTTP ({$elapsed}s)");
                    $this->logIdMapping($result);
                }
                return $result;
            }

            $errMsg = $data['error'] ?? $data['message'] ?? "HTTP {$statusCode}";
            $this->logger->error("    ✗ Multi-instrument upload failed: HTTP {$statusCode}");
            return ['success' => false, 'message' => $errMsg, 'idMapping' => [], 'method' => 'HTTP'];

        } catch (\Exception $e) {
            $this->logger->error("    ✗ Multi-instrument HTTP upload failed: " . $e->getMessage());
            return ['success' => false, 'message' => $e->getMessage(), 'idMapping' => [], 'method' => 'HTTP'];
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENT INSTALL  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    /**
     * Install an instrument definition file.
     *
     * im3: Sends install_file + instrument_type field.
     * Client auto-detects from extension via DD_TYPE_MAP:
     *   .csv  → redcap
     *   .linst → linst
     *   .json → bids
     *
     * @param string $filePath  Path to .linst, .csv (REDCap DD), or .json (BIDS)
     * @return array [success, message, method]
     */
    public function installInstrument(string $filePath): array
    {
        $filename = basename($filePath);
        $instrumentType = $this->detectInstrumentType($filePath);

        $this->logger->info("  Installing instrument from: {$filename} (type={$instrumentType})");

        // --- Priority 1: API client ---
        if ($this->hasApiClient && $this->moduleConfig !== null) {
            try {
                $this->logger->debug("    Trying API client (installInstrument)...");

                $api = new \LORISClient\Api\InstrumentManagerApi($this->httpClient, $this->moduleConfig);

                // im3 Generated signature (from schema required-first order):
                //   installInstrument($install_file, $instrument_type)
                $result = $api->installInstrument(
                    new SplFileObject($filePath, 'r'),  // install_file (required)
                    $instrumentType                     // instrument_type (required)
                );

                $this->logger->info("    ✓ Instrument installed via API client");
                $this->clearInstrumentCache();
                return ['success' => true, 'message' => 'Installed via API', 'method' => 'API'];
            } catch (\Error $e) {
                $this->logger->warning("    API install error: " . $e->getMessage());
                $this->logger->info("    Falling back to HTTP...");
            } catch (\Exception $e) {
                $msg = $e->getMessage();
                if (strpos($msg, '409') !== false || stripos($msg, 'already exists') !== false) {
                    $this->logger->info("    ✓ Instrument already installed in LORIS (409 Conflict)");
                    return ['success' => true, 'message' => 'Already installed', 'method' => 'API'];
                }
                $this->logger->warning("    API install failed: {$msg}");
                $this->logger->info("    Falling back to HTTP...");
            }
        }

        // --- Priority 2: Direct HTTP fallback ---
        return $this->installInstrumentHttp($filePath);
    }

    /**
     * Install from a monolithic REDCap data dictionary CSV.
     * Validates required REDCap DD headers before uploading.
     */
    public function installFromRedcap(string $csvFilePath): array
    {
        if (!file_exists($csvFilePath)) {
            throw new \RuntimeException("REDCap DD not found: {$csvFilePath}");
        }

        $handle  = fopen($csvFilePath, 'r');
        $headers = fgetcsv($handle);
        fclose($handle);

        $required = ['Variable / Field Name', 'Form Name', 'Field Type'];
        $missing  = array_diff($required, array_map('trim', $headers ?: []));

        if (!empty($missing)) {
            throw new \RuntimeException(
                "Invalid REDCap DD — missing columns: " . implode(', ', $missing)
            );
        }

        $this->logger->info("  Installing instruments from monolithic REDCap DD");
        return $this->installInstrument($csvFilePath);
    }

    /**
     * HTTP fallback for instrument installation.
     * im3: Sends install_file + instrument_type field.
     */
    private function installInstrumentHttp(string $filePath): array
    {
        $url = "{$this->baseUrl}/instrument_manager";
        $instrumentType = $this->detectInstrumentType($filePath);

        try {
            $this->logger->debug("    HTTP POST {$url}");

            $response = $this->httpClient->request('POST', $url, [
                'headers'   => ['Authorization' => "Bearer {$this->token}"],
                'multipart' => [
                    [
                        'name'     => 'install_file',
                        'contents' => fopen($filePath, 'r'),
                        'filename' => basename($filePath),
                    ],
                    ['name' => 'instrument_type', 'contents' => $instrumentType],
                ],
                'http_errors' => false,
            ]);

            $statusCode = $response->getStatusCode();
            $data       = json_decode((string) $response->getBody(), true) ?? [];

            if ($statusCode >= 200 && $statusCode < 300) {
                $this->logger->info("    ✓ Instrument installed via HTTP");
                $this->clearInstrumentCache();
                return ['success' => true, 'message' => $data['message'] ?? 'Installed via HTTP', 'method' => 'HTTP'];
            }

            if ($statusCode === 409) {
                $this->logger->info("    ✓ Instrument already installed (409 via HTTP)");
                return ['success' => true, 'message' => 'Already installed', 'method' => 'HTTP'];
            }

            $msg = $data['error'] ?? "HTTP {$statusCode}";
            $this->logger->warning("    HTTP install returned {$statusCode}: {$msg}");
            return ['success' => false, 'message' => $msg, 'method' => 'HTTP'];
        } catch (\Exception $e) {
            $this->logger->error("    ✗ HTTP install failed: " . $e->getMessage());
            return ['success' => false, 'message' => $e->getMessage(), 'method' => 'HTTP'];
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENT DATA HEADERS (GET template)
    // ──────────────────────────────────────────────────────────────────

    /**
     * Get expected CSV/TSV headers for an instrument.
     *
     * im3: Sends format query param to get headers in matching format.
     *
     * @param string|null $instrument  Instrument name
     * @param string      $action      CREATE_SESSIONS or VALIDATE_SESSIONS
     * @param string      $format      LORIS_CSV, REDCAP_CSV, or BIDS_TSV
     * @return string|null  CSV/TSV header line, or null on error
     */
    public function getInstrumentDataHeaders(
        ?string $instrument = null,
        string  $action = 'VALIDATE_SESSIONS',
        string  $format = 'LORIS_CSV'
    ): ?string {
        // --- Priority 1: API client ---
        if ($this->hasApiClient && $this->moduleConfig !== null) {
            try {
                $api = new \LORISClient\Api\InstrumentManagerApi($this->httpClient, $this->moduleConfig);

                // im3 Generated signature (from schema required-first order):
                //   getInstrumentDataHeaders($action, $format, $instrument?, $instruments?)
                $result = $api->getInstrumentDataHeaders(
                    $action,        // action (required)
                    $format,        // format (required)
                    $instrument,    // instrument (optional)
                    null            // instruments (optional)
                );

                if (is_string($result)) {
                    $this->logger->debug("    ✓ Got headers via API client");
                    return $result;
                }
                // Generated client returns SplFileObject for octet-stream responses
                if ($result instanceof \SplFileObject) {
                    $result->rewind();
                    $content = '';
                    while (!$result->eof()) {
                        $content .= $result->fgets();
                    }
                    $content = trim($content);
                    if (!empty($content)) {
                        $this->logger->debug("    ✓ Got headers via API client (SplFileObject)");
                        return $content;
                    }
                }
                if (is_object($result)) {
                    $stringResult = json_encode($result);
                    if (!empty($stringResult) && $stringResult !== '{}' && $stringResult !== 'null') {
                        return $stringResult;
                    }
                }
            } catch (\Error|\Exception $e) {
                $this->logger->debug("    API headers failed: " . $e->getMessage());
            }
        }

        // --- Priority 2: HTTP fallback ---
        return $this->getInstrumentDataHeadersHttp($instrument, $action, $format);
    }

    private function getInstrumentDataHeadersHttp(
        ?string $instrument,
        string  $action,
        string  $format = 'LORIS_CSV'
    ): ?string {
        // im3: Send format query param
        $params = ['action' => $action, 'format' => $format];
        if ($instrument) {
            $params['instrument'] = $instrument;
        }

        $url = "{$this->baseUrl}/instrument_manager/instrument_data?" . http_build_query($params);

        try {
            $response = $this->httpClient->request('GET', $url, [
                'headers'     => ['Authorization' => "Bearer {$this->token}"],
                'http_errors' => false,
            ]);
            if ($response->getStatusCode() === 200) {
                return (string) $response->getBody();
            }
            return null;
        } catch (\Exception $e) {
            $this->logger->debug("    HTTP headers failed: " . $e->getMessage());
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENT EXISTS  (cached via Projects API)
    // ──────────────────────────────────────────────────────────────────

    public function instrumentExists(string $instrument, ?string $project = null): bool
    {
        if ($this->installedInstruments === null) {
            $this->installedInstruments = $this->fetchInstalledInstruments($project);
        }
        $exists = in_array($instrument, $this->installedInstruments, true);
        $this->logger->info($exists
            ? "      ✓ '{$instrument}' found in LORIS"
            : "      ✗ '{$instrument}' NOT found in LORIS — will attempt install"
        );
        return $exists;
    }

    public function clearInstrumentCache(): void
    {
        $this->installedInstruments = null;
    }

    private function fetchInstalledInstruments(?string $project = null): array
    {
        $version = $this->activeVersion ?? $this->apiVersion;

        if ($project === null) {
            $project = $this->getFirstProject();
            if ($project === null) {
                $this->logger->warning("    No project found — cannot list instruments");
                return [];
            }
        }

        // Priority 1: API client
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api         = new \LORISClient\Api\ProjectsApi($this->httpClient, $this->apiConfig);
                $response    = $api->getProjectInstruments($project);
                $instruments = $this->parseInstrumentList($response);
                if (!empty($instruments)) {
                    return $instruments;
                }
            } catch (\Error|\Exception $e) {
                $this->logger->debug("    API getProjectInstruments failed: " . $e->getMessage());
            }
        }

        // Priority 2: HTTP
        $url = "{$this->baseUrl}/api/{$version}/projects/" . urlencode($project) . "/instruments";
        try {
            $response = $this->httpClient->request('GET', $url, [
                'headers' => ['Authorization' => "Bearer {$this->token}", 'Accept' => 'application/json'],
                'http_errors' => false,
            ]);
            if ($response->getStatusCode() === 200) {
                return $this->parseInstrumentList(json_decode((string) $response->getBody(), true));
            }
        } catch (\Exception $e) {
            $this->logger->debug("    HTTP instruments failed: " . $e->getMessage());
        }

        return [];
    }

    private function parseInstrumentList($response): array
    {
        $data = is_object($response) ? json_decode(json_encode($response), true)
            : (is_array($response) ? $response : []);

        $instruments = [];
        if (isset($data['Instruments']) && is_array($data['Instruments'])) {
            foreach ($data['Instruments'] as $key => $value) {
                if (is_string($key) && !is_numeric($key)) {
                    $instruments[] = $key;
                } elseif (is_array($value)) {
                    $name = $value['InstrumentName'] ?? $value['Test_name'] ?? $value['testName'] ?? $value['instrument'] ?? null;
                    if ($name) $instruments[] = $name;
                } elseif (is_string($value)) {
                    $instruments[] = $value;
                }
            }
        }
        return $instruments;
    }

    private function getFirstProject(): ?string
    {
        $version = $this->activeVersion ?? $this->apiVersion;
        try {
            $response = $this->httpClient->request('GET', "{$this->baseUrl}/api/{$version}/projects", [
                'headers' => ['Authorization' => "Bearer {$this->token}", 'Accept' => 'application/json'],
                'http_errors' => false,
            ]);
            if ($response->getStatusCode() === 200) {
                $data     = json_decode((string) $response->getBody(), true);
                $projects = $data['Projects'] ?? [];
                if (!empty($projects)) {
                    $firstKey = array_key_first($projects);
                    return is_string($firstKey) ? $firstKey : ($projects[0]['Name'] ?? $projects[0] ?? null);
                }
            }
        } catch (\Exception $e) {
            $this->logger->debug("    Failed to get projects: " . $e->getMessage());
        }
        return null;
    }

    /** Alias for uploadInstrumentData */
    public function uploadInstrumentCSV(string $instrument, string $csvFilePath, string $action = 'CREATE_SESSIONS', string|bool $strict = false): array
    {
        return $this->uploadInstrumentData($instrument, $csvFilePath, $action, $strict);
    }

    // ──────────────────────────────────────────────────────────────────
    // CANDIDATES  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    public function getCandidates(): array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\CandidatesApi($this->httpClient, $this->apiConfig);
                $response = $api->getCandidates();
                if (method_exists($response, 'getCandidates')) {
                    return json_decode(json_encode($response->getCandidates()), true) ?? [];
                }
                return json_decode(json_encode($response), true) ?? [];
            } catch (\Error|\Exception $e) {
                $this->logger->warning("    API getCandidates failed: " . $e->getMessage());
            }
        }
        return $this->apiGet('/candidates')['Candidates'] ?? [];
    }

    public function getCandidate(int $candid): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\CandidatesApi($this->httpClient, $this->apiConfig);
                return json_decode(json_encode($api->getCandidate((string) $candid)), true);
            } catch (\Error|\Exception $e) {
                $this->logger->debug("API getCandidate failed: " . $e->getMessage());
            }
        }
        return $this->apiGet("/candidates/{$candid}");
    }

    public function createCandidate(array $candidateData): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api     = new \LORISClient\Api\CandidatesApi($this->httpClient, $this->apiConfig);
                $request = new \LORISClient\Model\CandidateCreateRequest([
                    'candidate' => new \LORISClient\Model\CandidateCreateRequestCandidate($candidateData),
                ]);
                $response = $api->createCandidate($request);
                $this->logger->info("  ✓ Candidate created via API client");
                return json_decode(json_encode($response), true);
            } catch (\Error|\Exception $e) {
                $this->logger->warning("API createCandidate failed: " . $e->getMessage());
            }
        }
        return $this->apiPost('/candidates', ['candidate' => $candidateData]);
    }

    // ──────────────────────────────────────────────────────────────────
    // VISITS  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    public function getVisit(int $candid, string $visitLabel): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\VisitsApi($this->httpClient, $this->apiConfig);
                return json_decode(json_encode($api->getVisit((string) $candid, $visitLabel)), true);
            } catch (\Error|\Exception $e) {
                $this->logger->debug("API getVisit failed: " . $e->getMessage());
            }
        }
        return $this->apiGet("/candidates/{$candid}/{$visitLabel}");
    }

    public function createVisit(int $candid, string $visitLabel, array $visitData): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\VisitsApi($this->httpClient, $this->apiConfig);
                $api->createVisit((string) $candid, $visitLabel, new \LORISClient\Model\VisitCreateRequest($visitData));
                $this->logger->info("  ✓ Visit {$visitLabel} created via API client");
                return $visitData;
            } catch (\Error|\Exception $e) {
                $this->logger->warning("API createVisit failed: " . $e->getMessage());
            }
        }
        return $this->apiPut("/candidates/{$candid}/{$visitLabel}", $visitData);
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENTS  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    public function getInstrumentData(int $candid, string $visit, string $instrument): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\InstrumentsApi($this->httpClient, $this->apiConfig);
                return json_decode(json_encode($api->getInstrumentData((string) $candid, $visit, $instrument)), true);
            } catch (\Error|\Exception $e) {
                $this->logger->debug("API getInstrumentData failed: " . $e->getMessage());
            }
        }
        return $this->apiGet("/candidates/{$candid}/{$visit}/instruments/{$instrument}");
    }

    public function patchInstrumentData(int $candid, string $visit, string $instrument, array $data): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\InstrumentsApi($this->httpClient, $this->apiConfig);
                return json_decode(json_encode($api->patchInstrumentData(
                    (string) $candid, $visit, $instrument, new \LORISClient\Model\InstrumentDataRequest($data)
                )), true);
            } catch (\Error|\Exception $e) {
                $this->logger->debug("API patchInstrumentData failed: " . $e->getMessage());
            }
        }
        return $this->apiPatch("/candidates/{$candid}/{$visit}/instruments/{$instrument}", $data);
    }

    public function putInstrumentData(int $candid, string $visit, string $instrument, array $data): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\InstrumentsApi($this->httpClient, $this->apiConfig);
                return json_decode(json_encode($api->putInstrumentData(
                    (string) $candid, $visit, $instrument, new \LORISClient\Model\InstrumentDataRequest($data)
                )), true);
            } catch (\Error|\Exception $e) {
                $this->logger->debug("API putInstrumentData failed: " . $e->getMessage());
            }
        }
        return $this->apiPut("/candidates/{$candid}/{$visit}/instruments/{$instrument}", $data);
    }

    // ──────────────────────────────────────────────────────────────────
    // PROJECTS & SITES
    // ──────────────────────────────────────────────────────────────────

    public function getProjects(): array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                return json_decode(json_encode(
                    (new \LORISClient\Api\ProjectsApi($this->httpClient, $this->apiConfig))->getProjects()
                ), true) ?? [];
            } catch (\Error|\Exception $e) {
                $this->logger->debug("API getProjects failed: " . $e->getMessage());
            }
        }
        return $this->apiGet('/projects') ?? [];
    }

    public function getSites(): array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                return json_decode(json_encode(
                    (new \LORISClient\Api\SitesApi($this->httpClient, $this->apiConfig))->getSites()
                ), true) ?? [];
            } catch (\Error|\Exception $e) {
                $this->logger->debug("API getSites failed: " . $e->getMessage());
            }
        }
        return $this->apiGet('/sites') ?? [];
    }

    // ──────────────────────────────────────────────────────────────────
    // GENERIC HTTP HELPERS
    // ──────────────────────────────────────────────────────────────────

    private function apiGet(string $endpoint): ?array
    {
        $url = "{$this->baseUrl}/api/" . ($this->activeVersion ?? $this->apiVersion) . $endpoint;
        try {
            $r = $this->httpClient->request('GET', $url, [
                'headers' => ['Authorization' => "Bearer {$this->token}", 'Accept' => 'application/json'],
                'http_errors' => false,
            ]);
            return $r->getStatusCode() === 200 ? json_decode((string) $r->getBody(), true) : null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP GET {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    private function apiPost(string $endpoint, array $data): ?array
    {
        $url = "{$this->baseUrl}/api/" . ($this->activeVersion ?? $this->apiVersion) . $endpoint;
        try {
            $r = $this->httpClient->request('POST', $url, [
                'headers' => ['Authorization' => "Bearer {$this->token}", 'Content-Type' => 'application/json'],
                'json' => $data, 'http_errors' => false,
            ]);
            return ($r->getStatusCode() >= 200 && $r->getStatusCode() < 300) ? (json_decode((string) $r->getBody(), true) ?? []) : null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP POST {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    private function apiPut(string $endpoint, array $data): ?array
    {
        $url = "{$this->baseUrl}/api/" . ($this->activeVersion ?? $this->apiVersion) . $endpoint;
        try {
            $r = $this->httpClient->request('PUT', $url, [
                'headers' => ['Authorization' => "Bearer {$this->token}", 'Content-Type' => 'application/json'],
                'json' => $data, 'http_errors' => false,
            ]);
            return ($r->getStatusCode() >= 200 && $r->getStatusCode() < 300) ? (json_decode((string) $r->getBody(), true) ?? []) : null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP PUT {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    private function apiPatch(string $endpoint, array $data): ?array
    {
        $url = "{$this->baseUrl}/api/" . ($this->activeVersion ?? $this->apiVersion) . $endpoint;
        try {
            $r = $this->httpClient->request('PATCH', $url, [
                'headers' => ['Authorization' => "Bearer {$this->token}", 'Content-Type' => 'application/json'],
                'json' => $data, 'http_errors' => false,
            ]);
            return ($r->getStatusCode() >= 200 && $r->getStatusCode() < 300) ? (json_decode((string) $r->getBody(), true) ?? []) : null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP PATCH {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // INTERNAL HELPERS
    // ──────────────────────────────────────────────────────────────────

    private function parseApiUploadResult($result): array
    {
        if ($result === null) {
            return ['success' => false, 'message' => 'Null response', 'idMapping' => []];
        }
        if (is_object($result)) {
            $d = [];
            $d['success']   = method_exists($result, 'getSuccess') ? $result->getSuccess() : true;
            $d['message']   = method_exists($result, 'getMessage') ? ($result->getMessage() ?? 'OK') : 'OK';
            $d['idMapping'] = method_exists($result, 'getIdMapping') ? ($result->getIdMapping() ?? []) : [];
            if (!empty($d['idMapping'])) {
                $d['idMapping'] = json_decode(json_encode($d['idMapping']), true) ?? [];
            }
            return $d;
        }
        if (is_array($result)) {
            return [
                'success' => $result['success'] ?? true, 'message' => $result['message'] ?? 'OK',
                'idMapping' => $result['idMapping'] ?? [],
            ];
        }
        return ['success' => false, 'message' => 'Unexpected response type', 'idMapping' => []];
    }

    private function logIdMapping(array $result): void
    {
        if (isset($result['data']['rowsSaved'])) {
            $total = $result['data']['totalRows'] ?? '?';
            $this->logger->info("    Rows saved: {$result['data']['rowsSaved']}/{$total}");
        }
        $idMapping = $result['idMapping'] ?? [];
        if (!empty($idMapping) && is_array($idMapping)) {
            foreach ($idMapping as $m) {
                $sid = $m['ExternalID'] ?? $m['StudyID'] ?? $m['externalId'] ?? '?';
                $cid = $m['CandID'] ?? $m['candid'] ?? $m['candId'] ?? '?';
                $this->logger->debug("      StudyID {$sid} → CandID {$cid}");
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // ACCESSORS
    // ──────────────────────────────────────────────────────────────────

    public function getToken(): ?string        { return $this->token; }
    public function getBaseUrl(): string       { return $this->baseUrl; }
    public function getActiveVersion(): string { return $this->activeVersion ?? $this->apiVersion; }
    public function hasApiClient(): bool       { return $this->hasApiClient; }
    public function getApiConfig(): mixed      { return $this->apiConfig; }
    public function getModuleConfig(): mixed   { return $this->moduleConfig; }
}