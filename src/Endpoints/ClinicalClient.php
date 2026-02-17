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

    // API client objects (null when library not installed or init fails)
    private bool  $hasApiClient   = false;
    private mixed $apiConfig      = null;   // LORISClient\Configuration (REST API host)
    private mixed $moduleConfig   = null;   // LORISClient\Configuration (Module host)

    // Supported API versions (try in order)
    private array $apiVersions;
    private ?string $activeVersion = null;

    // Cache of installed instruments per project
    private ?array $installedInstruments = null;

    // ──────────────────────────────────────────────────────────────────
    // CONSTRUCTOR
    // ──────────────────────────────────────────────────────────────────

    /**
     * @param string               $baseUrl              LORIS base URL
     * @param string               $username              LORIS username
     * @param string               $password              LORIS password
     * @param int                  $tokenExpiryMinutes    Token refresh interval (minutes)
     * @param LoggerInterface|null $logger                PSR-3 logger
     * @param string               $apiVersion            API version (e.g. v0.0.4-dev)
     */
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

        $this->apiVersions = [$apiVersion];

        $this->httpClient = new GuzzleClient([
            'timeout' => 30,
            'verify'  => true,
        ]);

        // Detect and initialize the API client if available
        if (class_exists(\LORISClient\Configuration::class)
            && class_exists(\LORISClient\Api\AuthenticationApi::class)
        ) {
            $this->initApiClient();
        } else {
            $this->logger->info("loris-php-api-client not installed — HTTP fallback only");
        }
    }

    /**
     * Initialize loris-php-api-client Configuration objects.
     *
     * Two configs are needed because the schema defines two server types:
     *  - REST API config: host = {baseUrl}/api/{version}
     *  - Module config:   host = {baseUrl}
     */
    private function initApiClient(): void
    {
        try {
            // Register missing model classes that codegen doesn't create
            // (inline 200/201 response schemas → auto-named classes that don't exist)
            $this->registerMissingModels();

            // Use new Configuration() — NOT getDefaultConfiguration() singleton
            // REST API endpoints (/login, /candidates, /projects, /sites)
            $this->apiConfig = new \LORISClient\Configuration();
            $this->apiConfig->setHost("{$this->baseUrl}/api/{$this->apiVersion}");

            // Module endpoints (/instrument_manager/*)
            $this->moduleConfig = new \LORISClient\Configuration();
            $this->moduleConfig->setHost($this->baseUrl);

            $this->hasApiClient = true;
            $this->logger->info("loris-php-api-client initialized ✓");
        } catch (\Error $e) {
            $this->hasApiClient = false;
            $this->logger->warning("API client init error: " . $e->getMessage());
        } catch (\Exception $e) {
            $this->hasApiClient = false;
            $this->logger->warning("API client init failed: " . $e->getMessage());
        }
    }

    /**
     * Register missing model classes for loris-php-api-client.
     *
     * The OpenAPI schema uses inline response objects for 200/201 responses,
     * which causes the generator to reference auto-named classes like
     * "UploadInstrumentData201Response" that it never actually creates.
     *
     * This registers stub classes so the API client can deserialize
     * responses without crashing. Lives in pipeline code so it
     * survives client regeneration via ./generate.sh.
     *
     * If the real classes are eventually generated, class_exists()
     * returns true and these stubs are skipped automatically.
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

    protected static array \$openAPITypes = [
        'success'   => 'bool',
        'message'   => 'string',
        'ok'        => 'string',
        'idMapping' => 'object',
    ];

    protected static array \$openAPIFormats = [
        'success'   => null,
        'message'   => null,
        'ok'        => null,
        'idMapping' => null,
    ];

    protected static array \$openAPINullables = [
        'success'   => false,
        'message'   => false,
        'ok'        => false,
        'idMapping' => false,
    ];

    protected static array \$attributeMap = [
        'success'   => 'success',
        'message'   => 'message',
        'ok'        => 'ok',
        'idMapping' => 'idMapping',
    ];

    protected static array \$setters = [
        'success'   => 'setSuccess',
        'message'   => 'setMessage',
        'ok'        => 'setOk',
        'idMapping' => 'setIdMapping',
    ];

    protected static array \$getters = [
        'success'   => 'getSuccess',
        'message'   => 'getMessage',
        'ok'        => 'getOk',
        'idMapping' => 'getIdMapping',
    ];

    protected array \$container = [];
    protected array \$openAPINullablesSetToNull = [];

    public function __construct(?array \$data = null) {
        \$this->container['success']   = \$data['success'] ?? null;
        \$this->container['message']   = \$data['message'] ?? null;
        \$this->container['ok']        = \$data['ok'] ?? null;
        \$this->container['idMapping'] = \$data['idMapping'] ?? null;
    }

    public static function openAPITypes(): array { return static::\$openAPITypes; }
    public static function openAPIFormats(): array { return static::\$openAPIFormats; }
    public static function openAPINullables(): array { return static::\$openAPINullables; }
    public static function attributeMap(): array { return static::\$attributeMap; }
    public static function setters(): array { return static::\$setters; }
    public static function getters(): array { return static::\$getters; }
    public static function isNullable(string \$property): bool { return static::\$openAPINullables[\$property] ?? false; }
    public function isNullableSetToNull(string \$property): bool { return in_array(\$property, \$this->openAPINullablesSetToNull, true); }
    public function getModelName(): string { return static::\$openAPIModelName; }
    public function listInvalidProperties(): array { return []; }
    public function valid(): bool { return true; }

    public function getSuccess(): ?bool { return \$this->container['success']; }
    public function setSuccess(?bool \$val): static { \$this->container['success'] = \$val; return \$this; }
    public function getMessage(): ?string { return \$this->container['message']; }
    public function setMessage(?string \$val): static { \$this->container['message'] = \$val; return \$this; }
    public function getOk(): ?string { return \$this->container['ok']; }
    public function setOk(?string \$val): static { \$this->container['ok'] = \$val; return \$this; }
    public function getIdMapping(): mixed { return \$this->container['idMapping']; }
    public function setIdMapping(mixed \$val): static { \$this->container['idMapping'] = \$val; return \$this; }

    public function offsetExists(mixed \$offset): bool { return isset(\$this->container[\$offset]); }
    public function offsetGet(mixed \$offset): mixed { return \$this->container[\$offset] ?? null; }
    public function offsetSet(mixed \$offset, mixed \$value): void { if (\$offset !== null) { \$this->container[\$offset] = \$value; } }
    public function offsetUnset(mixed \$offset): void { unset(\$this->container[\$offset]); }

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

    /**
     * Authenticate with LORIS and obtain a JWT token.
     * Uses stored username/password from constructor.
     *
     * @return string  The JWT token
     * @throws \RuntimeException  If all auth methods fail
     */
    public function authenticate(): string
    {
        $this->logger->info("Authenticating with LORIS at {$this->baseUrl}");

        // --- Priority 1: API client ---
        if ($this->hasApiClient) {
            try {
                $this->logger->debug("  Trying API client (AuthenticationApi)...");

                foreach ($this->apiVersions as $version) {
                    $apiHost = "{$this->baseUrl}/api/{$version}";
                    $this->logger->info("  Trying API version: {$version}");

                    try {
                        // Use new Configuration() — NOT singleton (avoids shared state bug)
                        $this->apiConfig = new \LORISClient\Configuration();
                        $this->apiConfig->setHost($apiHost);

                        $authApi = new \LORISClient\Api\AuthenticationApi(
                            $this->httpClient,
                            $this->apiConfig
                        );

                        $loginRequest = new \LORISClient\Model\LoginRequest([
                            'username' => $this->username,
                            'password' => $this->password,
                        ]);

                        $response    = $authApi->login($loginRequest);
                        $this->token = $response->getToken();

                        if (!empty($this->token)) {
                            $this->apiConfig->setAccessToken($this->token);
                            $this->moduleConfig->setAccessToken($this->token);
                            $this->tokenExpiry   = time() + ($this->tokenExpiryMinutes * 60);
                            $this->activeVersion = $version;
                            $this->logger->info("  ✓ Authenticated via API client (version: {$version})");
                            return $this->token;
                        }

                        $this->logger->warning("    ✗ Empty token for version {$version}");
                    } catch (\Exception $e) {
                        $this->logger->warning("    ✗ API auth failed ({$version}): " . $e->getMessage());
                    }
                }

                $this->logger->info("  API client auth exhausted — falling back to HTTP...");
            } catch (\Error $e) {
                $this->logger->warning("  API auth error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->warning("  API client error: " . $e->getMessage());
            }
        }

        // --- Priority 2: Direct HTTP fallback (Guzzle) ---
        return $this->authenticateHttp();
    }

    /**
     * HTTP fallback for authentication — tries each API version.
     *
     * @return string  The JWT token
     * @throws \RuntimeException  If all versions fail
     */
    private function authenticateHttp(): string
    {
        $lastError = '';

        foreach ($this->apiVersions as $version) {
            $url = "{$this->baseUrl}/api/{$version}/login";

            try {
                $this->logger->debug("  HTTP POST {$url}");

                $response = $this->httpClient->request('POST', $url, [
                    'json' => [
                        'username' => $this->username,
                        'password' => $this->password,
                    ],
                    'http_errors' => false,
                ]);

                $statusCode = $response->getStatusCode();
                $rawBody    = (string) $response->getBody();

                $this->logger->debug("  Response status: {$statusCode}");

                if ($statusCode !== 200) {
                    $lastError = "HTTP {$statusCode}";
                    $this->logger->warning("  HTTP login returned {$statusCode}");
                    continue;
                }

                $data = json_decode($rawBody, true);
                if (json_last_error() !== JSON_ERROR_NONE) {
                    $lastError = json_last_error_msg();
                    $this->logger->warning("  Invalid JSON: {$lastError}");
                    continue;
                }

                $this->token = $data['token'] ?? null;

                if (!empty($this->token)) {
                    $this->activeVersion = $version;
                    $this->tokenExpiry   = time() + ($this->tokenExpiryMinutes * 60);

                    // Also set on API configs if available
                    if ($this->hasApiClient) {
                        if ($this->apiConfig !== null) {
                            $this->apiConfig->setAccessToken($this->token);
                        }
                        if ($this->moduleConfig !== null) {
                            $this->moduleConfig->setAccessToken($this->token);
                        }
                    }

                    $this->logger->info("  ✓ Authenticated via HTTP (version: {$version})");
                    return $this->token;
                }

                $lastError = "No token in response";
                $this->logger->warning("  No token in HTTP response");
            } catch (\Exception $e) {
                $lastError = $e->getMessage();
                $this->logger->warning("  HTTP auth error ({$version}): {$lastError}");
            }
        }

        throw new \RuntimeException("All authentication methods failed. Last error: {$lastError}");
    }

    /**
     * Refresh token if close to expiry.
     */
    public function refreshTokenIfNeeded(): void
    {
        if ($this->tokenExpiry !== null && time() >= ($this->tokenExpiry - 60)) {
            $this->logger->info("  Token near expiry — refreshing...");
            $this->authenticate();
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENT MANAGER  (API → HTTP)
    // Uses MODULE config: host = {baseUrl}
    // ──────────────────────────────────────────────────────────────────

    /**
     * Upload instrument data CSV.
     *
     * @param string $instrument     Instrument name
     * @param string $csvFilePath    Path to CSV file
     * @param string $action         CREATE_SESSIONS or VALIDATE_SESSIONS
     * @return array  [success => bool, message => string, idMapping => [...], method => string]
     */
    public function uploadInstrumentData(
        string $instrument,
        string $csvFilePath,
        string $action = 'CREATE_SESSIONS'
    ): array {
        // Auto-detect format: .tsv = BIDS_TSV, all others = LORIS_CSV
        $ext    = strtolower(pathinfo($csvFilePath, PATHINFO_EXTENSION));
        $format = ($ext === 'tsv') ? 'BIDS_TSV' : 'LORIS_CSV';

        $this->logger->info("  Uploading {$instrument} data ({$action}, {$format})");

        $startTime = microtime(true);

        // --- Priority 1: API client ---
        if ($this->hasApiClient && $this->moduleConfig !== null) {
            try {
                $this->logger->debug("    Trying API client (InstrumentManagerApi)...");

                $api = new \LORISClient\Api\InstrumentManagerApi(
                    $this->httpClient,
                    $this->moduleConfig
                );

                $fileObj = new SplFileObject($csvFilePath, 'r');

                // Signature: uploadInstrumentData($action, $format, $data_file, $instrument?, $multi_instrument?)
                $result = $api->uploadInstrumentData(
                    $action,        // action: CREATE_SESSIONS or VALIDATE_SESSIONS
                    $format,        // format: LORIS_CSV or BIDS_TSV (auto-detected)
                    $fileObj,       // data_file
                    $instrument,    // instrument name
                    null            // multi_instrument
                );

                $elapsed = round(microtime(true) - $startTime, 2);
                $this->logger->info("    Upload completed in {$elapsed}s");

                // Parse result — handle both SuccessResponse and array
                $parsed = $this->parseApiUploadResult($result);
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
        return $this->uploadInstrumentDataHttp($instrument, $csvFilePath, $action, $format);
    }

    /**
     * HTTP fallback for instrument data upload.
     */
    private function uploadInstrumentDataHttp(
        string $instrument,
        string $csvFilePath,
        string $action,
        string $format = 'LORIS_CSV'
    ): array {
        $url = "{$this->baseUrl}/instrument_manager/instrument_data";

        try {
            $this->logger->debug("    HTTP POST {$url}");

            $startTime = microtime(true);

            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                ],
                'multipart' => [
                    ['name' => 'action',     'contents' => $action],
                    ['name' => 'format',     'contents' => $format],
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

            $this->logger->debug("    Response status: {$statusCode}");
            $this->logger->info("    Upload completed in {$elapsed}s");

            $data = json_decode($rawBody, true) ?? [];

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
                'success' => false,
                'message' => $data['error'] ?? "HTTP {$statusCode}",
                'idMapping' => [],
                'method' => 'HTTP',
            ];
        } catch (\Exception $e) {
            $this->logger->error("    ✗ HTTP upload failed: " . $e->getMessage());
            return [
                'success' => false,
                'message' => $e->getMessage(),
                'idMapping' => [],
                'method' => 'HTTP',
            ];
        }
    }

    /**
     * Install an instrument from a .linst file or REDCap data dictionary CSV.
     *
     * @param string $filePath  Path to .linst or REDCap .csv
     * @return array [success => bool, message => string, method => string]
     */
    public function installInstrument(string $filePath): array
    {
        $filename = basename($filePath);
        $this->logger->info("  Installing instrument from: {$filename}");

        // Detect instrument type from file extension
        // Schema accepts: 'bids', 'linst', or 'redcap'
        $ext = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));
        $instrumentType = ($ext === 'linst') ? 'linst' : 'redcap';

        // --- Priority 1: API client ---
        if ($this->hasApiClient && $this->moduleConfig !== null) {
            try {
                $this->logger->debug("    Trying API client (installInstrument)...");

                $api = new \LORISClient\Api\InstrumentManagerApi(
                    $this->httpClient,
                    $this->moduleConfig
                );

                // Signature: installInstrument($install_file, $instrument_type)
                $result = $api->installInstrument(
                    new SplFileObject($filePath, 'r'),  // install_file
                    $instrumentType                      // instrument_type: 'linst' or 'redcap'
                );

                $this->logger->info("    ✓ Instrument installed via API client (type: {$instrumentType})");
                $this->clearInstrumentCache();
                return [
                    'success' => true,
                    'message' => 'Installed via API',
                    'method'  => 'API',
                ];
            } catch (\Error $e) {
                $this->logger->warning("    API install error: " . $e->getMessage());
                $this->logger->info("    Falling back to HTTP...");
            } catch (\Exception $e) {
                $msg = $e->getMessage();

                // 409 Conflict = already installed → treat as success
                if (strpos($msg, '409') !== false || stripos($msg, 'already exists') !== false) {
                    $this->logger->info("    ✓ Instrument already installed in LORIS (409 Conflict)");
                    return [
                        'success' => true,
                        'message' => 'Already installed',
                        'method'  => 'API',
                    ];
                }

                $this->logger->warning("    API install failed: {$msg}");
                $this->logger->info("    Falling back to HTTP...");
            }
        }

        // --- Priority 2: Direct HTTP fallback ---
        return $this->installInstrumentHttp($filePath);
    }

    /**
     * HTTP fallback for instrument installation.
     */
    private function installInstrumentHttp(string $filePath): array
    {
        $url = "{$this->baseUrl}/instrument_manager";

        try {
            $this->logger->debug("    HTTP POST {$url}");

            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                ],
                'multipart' => [
                    [
                        'name'     => 'instrument_file',
                        'contents' => fopen($filePath, 'r'),
                        'filename' => basename($filePath),
                    ],
                ],
                'http_errors' => false,
            ]);

            $statusCode = $response->getStatusCode();
            $rawBody    = (string) $response->getBody();
            $data       = json_decode($rawBody, true) ?? [];

            if ($statusCode >= 200 && $statusCode < 300) {
                $this->logger->info("    ✓ Instrument installed via HTTP");
                $this->clearInstrumentCache();
                return [
                    'success' => true,
                    'message' => $data['message'] ?? 'Installed via HTTP',
                    'method'  => 'HTTP',
                ];
            }

            // 409 Conflict = already installed → success
            if ($statusCode === 409) {
                $this->logger->info("    ✓ Instrument already installed (409 via HTTP)");
                return [
                    'success' => true,
                    'message' => 'Already installed',
                    'method'  => 'HTTP',
                ];
            }

            $msg = $data['error'] ?? "HTTP {$statusCode}";
            $this->logger->warning("    HTTP install returned {$statusCode}: {$msg}");
            return ['success' => false, 'message' => $msg, 'method' => 'HTTP'];
        } catch (\Exception $e) {
            $this->logger->error("    ✗ HTTP install failed: " . $e->getMessage());
            return ['success' => false, 'message' => $e->getMessage(), 'method' => 'HTTP'];
        }
    }

    /**
     * Get expected CSV headers for an instrument.
     *
     * @param string|null $instrument  Optional instrument name filter
     * @return string|null  CSV headers string, or null if not found / error
     */
    public function getInstrumentDataHeaders(?string $instrument = null): ?string
    {
        // --- Priority 1: API client ---
        if ($this->hasApiClient && $this->moduleConfig !== null) {
            try {
                $this->logger->debug("    Trying API client (getInstrumentDataHeaders)...");

                $api = new \LORISClient\Api\InstrumentManagerApi(
                    $this->httpClient,
                    $this->moduleConfig
                );

                // Signature: getInstrumentDataHeaders($action, $format, $instrument?, $instruments?)
                $result = $api->getInstrumentDataHeaders(
                    'VALIDATE_SESSIONS',  // action
                    'LORIS_CSV',          // format: LORIS_CSV or BIDS_TSV
                    $instrument,          // instrument (optional)
                    null                  // instruments (optional)
                );

                // Handle both string and object responses
                if (is_string($result)) {
                    $this->logger->debug("    ✓ Got headers via API client");
                    return $result;
                }

                if (is_object($result)) {
                    if (method_exists($result, 'getError')) {
                        $this->logger->debug("    API returned ErrorResponse: " . $result->getError());
                    } else {
                        $stringResult = json_encode($result);
                        if (!empty($stringResult) && $stringResult !== '{}' && $stringResult !== 'null') {
                            return $stringResult;
                        }
                    }
                }

                $this->logger->debug("    API client returned unusable result, falling back to HTTP");
            } catch (\Error $e) {
                $this->logger->debug("    API headers error: " . $e->getMessage());
                $this->logger->debug("    Falling back to HTTP...");
            } catch (\Exception $e) {
                $this->logger->debug("    API client headers failed: " . $e->getMessage());
                $this->logger->debug("    Falling back to HTTP...");
            }
        }

        // --- Priority 2: Direct HTTP fallback ---
        return $this->getInstrumentDataHeadersHttp($instrument);
    }

    /**
     * HTTP fallback for getting instrument data headers.
     */
    private function getInstrumentDataHeadersHttp(?string $instrument = null): ?string
    {
        $url = "{$this->baseUrl}/instrument_manager/instrument_data";
        if ($instrument) {
            $url .= '?' . http_build_query(['instrument' => $instrument]);
        }

        try {
            $this->logger->debug("    HTTP GET {$url}");

            $response = $this->httpClient->request('GET', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                ],
                'http_errors' => false,
            ]);

            $statusCode = $response->getStatusCode();

            if ($statusCode === 200) {
                $body = (string) $response->getBody();
                $this->logger->debug("    ✓ Got headers via HTTP");
                return $body;
            }

            $this->logger->debug("    HTTP headers returned {$statusCode}");
            return null;
        } catch (\Exception $e) {
            $this->logger->debug("    HTTP headers failed: " . $e->getMessage());
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENT EXISTS  (cached via Projects API)
    // ──────────────────────────────────────────────────────────────────

    /**
     * Check if an instrument exists in LORIS.
     *
     * Uses GET /projects/{project}/instruments to get the full list
     * of installed instruments and checks against it.
     * Results are cached so we only call the API once per run.
     *
     * @param string      $instrument   Instrument name to check
     * @param string|null $project      Project name (optional, uses first project if null)
     * @return bool
     */
    public function instrumentExists(string $instrument, ?string $project = null): bool
    {
        // Load and cache instrument list if not already done
        if ($this->installedInstruments === null) {
            $this->installedInstruments = $this->fetchInstalledInstruments($project);
        }

        $exists = in_array($instrument, $this->installedInstruments, true);

        if ($exists) {
            $this->logger->info("      ✓ '{$instrument}' found in LORIS");
        } else {
            $this->logger->info("      ✗ '{$instrument}' NOT found in LORIS — will attempt install");
        }

        return $exists;
    }

    /**
     * Clear the cached instrument list (e.g. after installing a new instrument).
     */
    public function clearInstrumentCache(): void
    {
        $this->installedInstruments = null;
    }

    /**
     * Fetch list of all installed instrument names from LORIS.
     *
     * Priority 1: API client → ProjectsApi::getProjectInstruments
     * Priority 2: HTTP → GET /projects/{project}/instruments
     *
     * @param string|null $project  Project name
     * @return array  List of instrument test_name strings
     */
    private function fetchInstalledInstruments(?string $project = null): array
    {
        $version = $this->activeVersion ?? $this->apiVersion;

        // If no project specified, get the first project
        if ($project === null) {
            $project = $this->getFirstProject();
            if ($project === null) {
                $this->logger->warning("    No project found — cannot list instruments");
                return [];
            }
        }

        $this->logger->debug("    Fetching instruments for project '{$project}'...");

        // --- Priority 1: API client ---
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\ProjectsApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $response = $api->getProjectInstruments($project);
                $instruments = $this->parseInstrumentList($response);

                if (!empty($instruments)) {
                    return $instruments;
                }
            } catch (\Error $e) {
                $this->logger->debug("    API getProjectInstruments error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->debug("    API getProjectInstruments failed: " . $e->getMessage());
            }
        }

        // --- Priority 2: HTTP fallback ---
        $url = "{$this->baseUrl}/api/{$version}/projects/" . urlencode($project) . "/instruments";

        try {
            $this->logger->debug("    HTTP GET {$url}");

            $response = $this->httpClient->request('GET', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Accept'        => 'application/json',
                ],
                'http_errors' => false,
            ]);

            $statusCode = $response->getStatusCode();

            if ($statusCode === 200) {
                $data = json_decode((string) $response->getBody(), true);
                $instruments = $this->parseInstrumentList($data);
                return $instruments;
            }

            $this->logger->debug("    HTTP instruments returned {$statusCode}");
        } catch (\Exception $e) {
            $this->logger->debug("    HTTP instruments failed: " . $e->getMessage());
        }

        return [];
    }

    /**
     * Parse instrument list from various response formats.
     */
    private function parseInstrumentList($response): array
    {
        // Convert objects to array
        if (is_object($response)) {
            $data = json_decode(json_encode($response), true);
        } elseif (is_array($response)) {
            $data = $response;
        } else {
            return [];
        }

        $instruments = [];

        if (isset($data['Instruments'])) {
            $list = $data['Instruments'];
            if (is_array($list)) {
                foreach ($list as $key => $value) {
                    if (is_string($key) && !is_numeric($key)) {
                        $instruments[] = $key;
                    } elseif (is_array($value)) {
                        $name = $value['InstrumentName']
                            ?? $value['Test_name']
                            ?? $value['testName']
                            ?? $value['instrument']
                            ?? null;
                        if ($name) {
                            $instruments[] = $name;
                        }
                    } elseif (is_string($value)) {
                        $instruments[] = $value;
                    }
                }
            }
        }

        return $instruments;
    }

    /**
     * Get the first project name from LORIS.
     */
    private function getFirstProject(): ?string
    {
        $version = $this->activeVersion ?? $this->apiVersion;
        $url     = "{$this->baseUrl}/api/{$version}/projects";

        try {
            $response = $this->httpClient->request('GET', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Accept'        => 'application/json',
                ],
                'http_errors' => false,
            ]);

            if ($response->getStatusCode() === 200) {
                $data = json_decode((string) $response->getBody(), true);
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

    /**
     * Alias for uploadInstrumentData — ClinicalPipeline calls this name.
     */
    public function uploadInstrumentCSV(
        string $instrument,
        string $csvFilePath,
        string $action = 'CREATE_SESSIONS'
    ): array {
        return $this->uploadInstrumentData($instrument, $csvFilePath, $action);
    }

    // ──────────────────────────────────────────────────────────────────
    // CANDIDATES  (API → HTTP)
    // Uses REST API config: host = {baseUrl}/api/{version}
    // ──────────────────────────────────────────────────────────────────

    /**
     * Get list of all candidates.
     */
    public function getCandidates(): array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\CandidatesApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $response = $api->getCandidates();

                if (method_exists($response, 'getCandidates')) {
                    return json_decode(json_encode($response->getCandidates()), true) ?? [];
                }

                return json_decode(json_encode($response), true) ?? [];
            } catch (\Error $e) {
                $this->logger->warning("    API getCandidates error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->warning("API getCandidates failed: " . $e->getMessage());
            }
        }

        return $this->apiGet('/candidates')['Candidates'] ?? [];
    }

    /**
     * Get a single candidate by CandID.
     */
    public function getCandidate(int $candid): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\CandidatesApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $response = $api->getCandidate((string) $candid);
                return json_decode(json_encode($response), true);
            } catch (\Error $e) {
                $this->logger->warning("    API getCandidate error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->debug("API getCandidate failed: " . $e->getMessage());
            }
        }

        return $this->apiGet("/candidates/{$candid}");
    }

    /**
     * Create a new candidate.
     *
     * @param array $candidateData  ['Project' => ..., 'DoB' => ..., 'Sex' => ..., 'Site' => ...]
     */
    public function createCandidate(array $candidateData): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\CandidatesApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $request = new \LORISClient\Model\CandidateCreateRequest([
                    'candidate' => new \LORISClient\Model\CandidateCreateRequestCandidate($candidateData),
                ]);

                $response = $api->createCandidate($request);
                $result   = json_decode(json_encode($response), true);

                $this->logger->info("  ✓ Candidate created via API client");
                return $result;
            } catch (\Error $e) {
                $this->logger->warning("    API createCandidate error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->warning("API createCandidate failed: " . $e->getMessage());
            }
        }

        return $this->apiPost('/candidates', ['candidate' => $candidateData]);
    }

    // ──────────────────────────────────────────────────────────────────
    // VISITS  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    /**
     * Get visit details.
     */
    public function getVisit(int $candid, string $visitLabel): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\VisitsApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $response = $api->getVisit((string) $candid, $visitLabel);
                return json_decode(json_encode($response), true);
            } catch (\Error $e) {
                $this->logger->warning("    API getVisit error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->debug("API getVisit failed: " . $e->getMessage());
            }
        }

        return $this->apiGet("/candidates/{$candid}/{$visitLabel}");
    }

    /**
     * Create a new visit.
     */
    public function createVisit(int $candid, string $visitLabel, array $visitData): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\VisitsApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $request = new \LORISClient\Model\VisitCreateRequest($visitData);
                $response = $api->createVisit((string) $candid, $visitLabel, $request);

                $this->logger->info("  ✓ Visit {$visitLabel} created via API client");
                return json_decode(json_encode($response), true);
            } catch (\Error $e) {
                $this->logger->warning("    API createVisit error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->warning("API createVisit failed: " . $e->getMessage());
            }
        }

        return $this->apiPut("/candidates/{$candid}/{$visitLabel}", $visitData);
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENTS  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    /**
     * Get instrument data for a candidate/visit.
     */
    public function getInstrumentData(int $candid, string $visit, string $instrument): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\InstrumentsApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $response = $api->getInstrumentData((string) $candid, $visit, $instrument);
                return json_decode(json_encode($response), true);
            } catch (\Error $e) {
                $this->logger->warning("    API getInstrumentData error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->debug("API getInstrumentData failed: " . $e->getMessage());
            }
        }

        return $this->apiGet("/candidates/{$candid}/{$visit}/instruments/{$instrument}");
    }

    /**
     * Update (PATCH) instrument data.
     */
    public function patchInstrumentData(int $candid, string $visit, string $instrument, array $data): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\InstrumentsApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $request = new \LORISClient\Model\InstrumentDataRequest($data);
                $response = $api->patchInstrumentData((string) $candid, $visit, $instrument, $request);
                return json_decode(json_encode($response), true);
            } catch (\Error $e) {
                $this->logger->warning("    API patchInstrumentData error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->debug("API patchInstrumentData failed: " . $e->getMessage());
            }
        }

        return $this->apiPatch("/candidates/{$candid}/{$visit}/instruments/{$instrument}", $data);
    }

    /**
     * Replace (PUT) instrument data.
     */
    public function putInstrumentData(int $candid, string $visit, string $instrument, array $data): ?array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\InstrumentsApi(
                    $this->httpClient,
                    $this->apiConfig
                );

                $request = new \LORISClient\Model\InstrumentDataRequest($data);
                $response = $api->putInstrumentData((string) $candid, $visit, $instrument, $request);
                return json_decode(json_encode($response), true);
            } catch (\Error $e) {
                $this->logger->warning("    API putInstrumentData error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->debug("API putInstrumentData failed: " . $e->getMessage());
            }
        }

        return $this->apiPut("/candidates/{$candid}/{$visit}/instruments/{$instrument}", $data);
    }

    // ──────────────────────────────────────────────────────────────────
    // PROJECTS & SITES  (API → HTTP)
    // ──────────────────────────────────────────────────────────────────

    public function getProjects(): array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\ProjectsApi($this->httpClient, $this->apiConfig);
                $response = $api->getProjects();
                return json_decode(json_encode($response), true) ?? [];
            } catch (\Error $e) {
                $this->logger->warning("    API getProjects error: " . $e->getMessage());
            } catch (\Exception $e) {
                $this->logger->debug("API getProjects failed: " . $e->getMessage());
            }
        }

        return $this->apiGet('/projects') ?? [];
    }

    public function getSites(): array
    {
        if ($this->hasApiClient && $this->apiConfig !== null) {
            try {
                $api = new \LORISClient\Api\SitesApi($this->httpClient, $this->apiConfig);
                $response = $api->getSites();
                return json_decode(json_encode($response), true) ?? [];
            } catch (\Error $e) {
                $this->logger->warning("    API getSites error: " . $e->getMessage());
            } catch (\Exception $e) {
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
        $version = $this->activeVersion ?? $this->apiVersion;
        $url     = "{$this->baseUrl}/api/{$version}{$endpoint}";

        try {
            $response = $this->httpClient->request('GET', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Accept'        => 'application/json',
                ],
                'http_errors' => false,
            ]);

            if ($response->getStatusCode() === 200) {
                return json_decode((string) $response->getBody(), true);
            }

            $this->logger->debug("HTTP GET {$endpoint} returned {$response->getStatusCode()}");
            return null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP GET {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    private function apiPost(string $endpoint, array $data): ?array
    {
        $version = $this->activeVersion ?? $this->apiVersion;
        $url     = "{$this->baseUrl}/api/{$version}{$endpoint}";

        try {
            $response = $this->httpClient->request('POST', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json'        => $data,
                'http_errors' => false,
            ]);

            if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
                return json_decode((string) $response->getBody(), true) ?? [];
            }

            $this->logger->debug("HTTP POST {$endpoint} returned {$response->getStatusCode()}");
            return null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP POST {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    private function apiPut(string $endpoint, array $data): ?array
    {
        $version = $this->activeVersion ?? $this->apiVersion;
        $url     = "{$this->baseUrl}/api/{$version}{$endpoint}";

        try {
            $response = $this->httpClient->request('PUT', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json'        => $data,
                'http_errors' => false,
            ]);

            if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
                return json_decode((string) $response->getBody(), true) ?? [];
            }

            $this->logger->debug("HTTP PUT {$endpoint} returned {$response->getStatusCode()}");
            return null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP PUT {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    private function apiPatch(string $endpoint, array $data): ?array
    {
        $version = $this->activeVersion ?? $this->apiVersion;
        $url     = "{$this->baseUrl}/api/{$version}{$endpoint}";

        try {
            $response = $this->httpClient->request('PATCH', $url, [
                'headers' => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json'        => $data,
                'http_errors' => false,
            ]);

            if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
                return json_decode((string) $response->getBody(), true) ?? [];
            }

            $this->logger->debug("HTTP PATCH {$endpoint} returned {$response->getStatusCode()}");
            return null;
        } catch (\Exception $e) {
            $this->logger->debug("HTTP PATCH {$endpoint} error: " . $e->getMessage());
            return null;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // INTERNAL HELPERS
    // ──────────────────────────────────────────────────────────────────

    /**
     * Parse the API client upload result into a standard array.
     */
    private function parseApiUploadResult($result): array
    {
        if ($result === null) {
            return ['success' => false, 'message' => 'Null response', 'idMapping' => []];
        }

        if (is_object($result)) {
            $data = [];

            if (method_exists($result, 'getSuccess')) {
                $data['success'] = $result->getSuccess();
            } elseif (method_exists($result, 'getMessage')) {
                $data['success'] = true;
            } else {
                $data['success'] = true;
            }

            $data['message'] = method_exists($result, 'getMessage')
                ? ($result->getMessage() ?? 'OK')
                : 'OK';

            $data['idMapping'] = method_exists($result, 'getIdMapping')
                ? ($result->getIdMapping() ?? [])
                : [];

            if (!empty($data['idMapping'])) {
                $data['idMapping'] = json_decode(json_encode($data['idMapping']), true) ?? [];
            }

            if (method_exists($result, 'getRowsSaved')) {
                $data['rowsSaved'] = $result->getRowsSaved();
            }
            if (method_exists($result, 'getRecordsCreated')) {
                $data['recordsCreated'] = $result->getRecordsCreated();
            }

            return $data;
        }

        if (is_array($result)) {
            return [
                'success'   => $result['success'] ?? true,
                'message'   => $result['message'] ?? 'OK',
                'idMapping' => $result['idMapping'] ?? [],
            ];
        }

        return ['success' => false, 'message' => 'Unexpected response type', 'idMapping' => []];
    }

    /**
     * Log ID mapping from upload result.
     */
    private function logIdMapping(array $result): void
    {
        if (isset($result['data']['rowsSaved'])) {
            $total = $result['data']['totalRows'] ?? '?';
            $this->logger->info("    Rows saved: {$result['data']['rowsSaved']}/{$total}");
        }
        if (isset($result['data']['recordsCreated'])) {
            $this->logger->info("    New Records created: {$result['data']['recordsCreated']}");
        }

        $idMapping = $result['idMapping'] ?? [];
        if (!empty($idMapping) && is_array($idMapping)) {
            foreach ($idMapping as $mapping) {
                $studyId = $mapping['ExternalID'] ?? $mapping['StudyID'] ?? $mapping['externalId'] ?? '?';
                $candId  = $mapping['CandID'] ?? $mapping['candid'] ?? $mapping['candId'] ?? '?';
                $this->logger->debug("      StudyID {$studyId} → CandID {$candId}");
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // ACCESSORS
    // ──────────────────────────────────────────────────────────────────

    public function getToken(): ?string
    {
        return $this->token;
    }

    public function getBaseUrl(): string
    {
        return $this->baseUrl;
    }

    public function getActiveVersion(): string
    {
        return $this->activeVersion ?? $this->apiVersion;
    }

    public function hasApiClient(): bool
    {
        return $this->hasApiClient;
    }

    public function getApiConfig(): mixed
    {
        return $this->apiConfig;
    }

    public function getModuleConfig(): mixed
    {
        return $this->moduleConfig;
    }
}