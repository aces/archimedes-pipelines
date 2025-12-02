<?php
declare(strict_types=1);

namespace LORIS\Endpoints;

use LORISClient\Configuration;
use LORISClient\Api\AuthenticationApi;
use LORISClient\Api\InstrumentManagerApi;
use LORISClient\Model\LoginRequest;
use GuzzleHttp\Client;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use SplFileObject;

/**
 * Clinical Data Ingestion Client
 *
 * Uses loris-php-api-client for:
 * - Authentication (AuthenticationApi)
 * - Instrument upload (InstrumentManagerApi) with HTTP fallback
 */
class ClinicalClient
{
    private Configuration $apiConfig;
    private Configuration $moduleConfig;
    private Client $httpClient;
    private LoggerInterface $logger;

    private string $baseUrl;
    private string $username;
    private string $password;
    private int $tokenExpiryMinutes;

    private ?string $token = null;
    private ?int $tokenExpiry = null;
    private ?string $activeVersion = null;

    private array $apiVersions = ['v0.0.4-dev', 'v0.0.4', 'v0.0.3'];

    public function __construct(
        string $baseUrl,
        string $username,
        string $password,
        int $tokenExpiryMinutes = 55,
        ?LoggerInterface $logger = null
    ) {
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->username = $username;
        $this->password = $password;
        $this->tokenExpiryMinutes = $tokenExpiryMinutes;
        $this->logger = $logger ?? new NullLogger();

        $this->httpClient = new Client([
            'timeout' => 120,
            'http_errors' => false,
            'verify' => false,
        ]);

        $this->apiConfig = new Configuration();
        $this->moduleConfig = new Configuration();
        $this->moduleConfig->setHost($this->baseUrl);
    }

    // ──────────────────────────────────────────────────────────────────
    // AUTHENTICATION
    // ──────────────────────────────────────────────────────────────────

    public function authenticate(): string
    {
        $this->logger->info('Authenticating with LORIS API...');

        $lastError = null;

        foreach ($this->apiVersions as $version) {
            $apiHost = "{$this->baseUrl}/api/{$version}";
            $this->logger->info("Trying API version: {$version}");

            try {
                $this->apiConfig = new Configuration();
                $this->apiConfig->setHost($apiHost);

                $authApi = new AuthenticationApi($this->httpClient, $this->apiConfig);
                $response = $authApi->login(new LoginRequest([
                    'username' => $this->username,
                    'password' => $this->password
                ]));

                $this->token = $response->getToken();

                if (empty($this->token)) {
                    $lastError = 'Empty token in response';
                    continue;
                }

                $this->apiConfig->setAccessToken($this->token);
                $this->moduleConfig->setAccessToken($this->token);
                $this->tokenExpiry = time() + ($this->tokenExpiryMinutes * 60);
                $this->activeVersion = $version;

                $this->logger->info("✓ Authenticated using {$version}");
                return $this->token;

            } catch (\LORISClient\ApiException $e) {
                $this->logger->warning("API error for {$version}: " . $e->getMessage());
                $lastError = $e->getMessage();
            } catch (\Exception $e) {
                $this->logger->warning("Error for {$version}: " . $e->getMessage());
                $lastError = $e->getMessage();
            }
        }

        throw new \RuntimeException("Authentication failed. Last error: {$lastError}");
    }

    public function getToken(): string
    {
        if ($this->token === null || ($this->tokenExpiry !== null && time() >= $this->tokenExpiry)) {
            return $this->authenticate();
        }
        return $this->token;
    }

    public function hasValidToken(): bool
    {
        return $this->token !== null
            && $this->tokenExpiry !== null
            && time() < $this->tokenExpiry;
    }

    private function getAuthHeaders(): array
    {
        return [
            'Authorization' => 'Bearer ' . $this->token,
            'Accept' => 'application/json',
        ];
    }

    // ──────────────────────────────────────────────────────────────────
    // INSTRUMENT MANAGER
    // ──────────────────────────────────────────────────────────────────

    private function getInstrumentManagerApi(): InstrumentManagerApi
    {
        if (empty($this->moduleConfig->getAccessToken()) && $this->token) {
            $this->moduleConfig->setAccessToken($this->token);
        }
        return new InstrumentManagerApi($this->httpClient, $this->moduleConfig);
    }

    /**
     * Upload instrument CSV - tries API client first, falls back to HTTP
     */
    public function uploadInstrumentCSV(
        string $instrument,
        string $csvFilePath,
        string $action = 'CREATE_SESSIONS'
    ): array {
        if (!file_exists($csvFilePath)) {
            throw new \RuntimeException("CSV file not found: {$csvFilePath}");
        }

        $this->getToken();
        $this->logger->info("Uploading: {$instrument}");

        // Try API client first
        try {
            $result = $this->uploadViaApiClient($instrument, $csvFilePath, $action);
            if ($result !== null) {
                return $result;
            }
        } catch (\Exception $e) {
            $this->logger->warning("API client failed: " . $e->getMessage() . " - trying HTTP fallback");
        }

        // Fallback to direct HTTP
        return $this->uploadViaHttp($instrument, $csvFilePath, $action);
    }

    /**
     * Upload via loris-php-api-client InstrumentManagerApi
     */
    private function uploadViaApiClient(
        string $instrument,
        string $csvFilePath,
        string $action
    ): ?array {
        $api = $this->getInstrumentManagerApi();

        $result = $api->uploadInstrumentData(
            $action,
            new SplFileObject($csvFilePath, 'r'),
            $instrument,
            null
        );

        $success = $result->getSuccess();
        $idMappingRaw = $result->getIdMapping() ?? [];

        // Convert idMapping objects to arrays
        $idMapping = [];
        foreach ($idMappingRaw as $map) {
            $idMapping[] = [
                'ExtStudyID' => $map->getExtStudyId(),
                'CandID' => $map->getCandId()
            ];
        }

        // Build message - API client returns empty object, so generate from data
        $totalRows = count($idMapping);
        $message = $totalRows > 0
            ? "Saved {$totalRows} out of {$totalRows} rows to the database."
            : $this->extractMessage($result->getMessage());

        $this->logger->info($success ? "✓ Upload successful (API client)" : "✗ Upload failed");

        return [
            'success' => $success,
            'message' => $message,
            'idMapping' => $idMapping
        ];
    }

    /**
     * Upload via direct HTTP (fallback)
     */
    private function uploadViaHttp(
        string $instrument,
        string $csvFilePath,
        string $action
    ): array {
        $url = "{$this->baseUrl}/instrument_manager/instrument_data";

        $fileHandle = fopen($csvFilePath, 'r');
        if ($fileHandle === false) {
            throw new \RuntimeException("Failed to open CSV file: {$csvFilePath}");
        }

        try {
            $response = $this->httpClient->post($url, [
                'headers' => $this->getAuthHeaders(),
                'multipart' => [
                    ['name' => 'instrument', 'contents' => $instrument],
                    ['name' => 'action', 'contents' => $action],
                    [
                        'name' => 'data_file',
                        'contents' => $fileHandle,
                        'filename' => basename($csvFilePath)
                    ]
                ]
            ]);

            if (is_resource($fileHandle)) {
                fclose($fileHandle);
            }

            $result = $this->parseHttpResponse($response);
            $this->logger->info($result['success'] ? "✓ Upload successful (HTTP)" : "✗ Upload failed");

            return $result;

        } catch (\Exception $e) {
            if (is_resource($fileHandle)) {
                fclose($fileHandle);
            }
            $this->logger->error("HTTP upload failed: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Upload multi-instrument CSV
     */
    public function uploadMultiInstrumentCSV(
        string $csvFilePath,
        string $action = 'CREATE_SESSIONS'
    ): array {
        if (!file_exists($csvFilePath)) {
            throw new \RuntimeException("CSV file not found: {$csvFilePath}");
        }

        $this->getToken();
        $this->logger->info("Uploading multi-instrument CSV");

        // Try API client first
        try {
            $api = $this->getInstrumentManagerApi();
            $result = $api->uploadInstrumentData(
                $action,
                new SplFileObject($csvFilePath, 'r'),
                null,
                'true'
            );

            $idMappingRaw = $result->getIdMapping() ?? [];
            $idMapping = [];
            foreach ($idMappingRaw as $map) {
                $idMapping[] = [
                    'ExtStudyID' => $map->getExtStudyId(),
                    'CandID' => $map->getCandId()
                ];
            }

            $totalRows = count($idMapping);
            $message = $totalRows > 0
                ? "Saved {$totalRows} out of {$totalRows} rows to the database."
                : $this->extractMessage($result->getMessage());

            return [
                'success' => $result->getSuccess(),
                'message' => $message,
                'idMapping' => $idMapping
            ];

        } catch (\Exception $e) {
            $this->logger->warning("API client failed: " . $e->getMessage() . " - trying HTTP");
        }

        // Fallback to HTTP
        $url = "{$this->baseUrl}/instrument_manager/instrument_data";
        $fileHandle = fopen($csvFilePath, 'r');

        try {
            $response = $this->httpClient->post($url, [
                'headers' => $this->getAuthHeaders(),
                'multipart' => [
                    ['name' => 'action', 'contents' => $action],
                    ['name' => 'multi-instrument', 'contents' => 'true'],
                    [
                        'name' => 'data_file',
                        'contents' => $fileHandle,
                        'filename' => basename($csvFilePath)
                    ]
                ]
            ]);

            if (is_resource($fileHandle)) {
                fclose($fileHandle);
            }

            return $this->parseHttpResponse($response);

        } catch (\Exception $e) {
            if (is_resource($fileHandle)) {
                fclose($fileHandle);
            }
            throw $e;
        }
    }

    /**
     * Install instrument from LINST file or REDCap CSV
     */
    public function installInstrument(string $filePath): bool
    {
        if (!file_exists($filePath)) {
            throw new \RuntimeException("File not found: {$filePath}");
        }

        $this->getToken();
        $this->logger->info("Installing instrument from: " . basename($filePath));

        try {
            $api = $this->getInstrumentManagerApi();
            $api->installInstrument(new SplFileObject($filePath, 'r'));
            $this->logger->info("✓ Instrument installed");
            return true;
        } catch (\Exception $e) {
            $this->logger->error("Install failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Get expected CSV headers for instrument(s)
     * Returns empty string on error instead of throwing
     */
    public function getInstrumentDataHeaders(
        string $action = 'CREATE_SESSIONS',
        ?string $instrument = null,
        ?string $instruments = null
    ): string {
        $this->getToken();

        try {
            $api = $this->getInstrumentManagerApi();
            $result = $api->getInstrumentDataHeaders($action, $instrument, $instruments);

            // Handle if API returns ErrorResponse instead of string
            if (!is_string($result)) {
                return '';
            }

            return $result;
        } catch (\Exception $e) {
            $this->logger->debug("Failed to get headers: " . $e->getMessage());
            return '';
        }
    }

    /**
     * Check if instrument exists in LORIS
     * Uses direct HTTP to avoid type issues with API client
     */
    public function instrumentExists(string $instrument): bool
    {
        try {
            $this->getToken();

            // Use direct HTTP GET to check - more reliable than API client
            $url = "{$this->baseUrl}/instrument_manager/instrument_data";
            $response = $this->httpClient->get($url, [
                'headers' => $this->getAuthHeaders(),
                'query' => [
                    'action' => 'VALIDATE_SESSIONS',
                    'instrument' => $instrument
                ]
            ]);

            $statusCode = $response->getStatusCode();

            // 200 = instrument exists, 400/404 = doesn't exist
            return $statusCode >= 200 && $statusCode < 300;

        } catch (\Exception $e) {
            $this->logger->debug("Instrument check failed for '{$instrument}': " . $e->getMessage());
            return false;
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // HELPERS
    // ──────────────────────────────────────────────────────────────────

    /**
     * Extract message string from various formats
     */
    private function extractMessage($message): string
    {
        if (is_string($message)) {
            return $message;
        }
        if (is_null($message)) {
            return '';
        }
        if (is_array($message)) {
            return json_encode($message);
        }
        if (is_object($message)) {
            if (method_exists($message, '__toString')) {
                return (string) $message;
            }
            if (method_exists($message, 'getMessage')) {
                return $this->extractMessage($message->getMessage());
            }
            if ($message instanceof \JsonSerializable) {
                $data = $message->jsonSerialize();
                return is_string($data) ? $data : json_encode($data);
            }
            return json_encode($message);
        }
        return (string) $message;
    }

    /**
     * Parse HTTP response into standard array format
     */
    private function parseHttpResponse($response): array
    {
        $statusCode = $response->getStatusCode();
        $body = $response->getBody()->getContents();

        if (empty($body)) {
            return [
                'success' => $statusCode >= 200 && $statusCode < 300,
                'message' => "HTTP {$statusCode}",
                'idMapping' => []
            ];
        }

        $data = json_decode($body, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            return [
                'success' => $statusCode >= 200 && $statusCode < 300,
                'message' => $body,
                'idMapping' => []
            ];
        }

        if ($statusCode >= 400) {
            return [
                'success' => false,
                'message' => $data['message'] ?? $data['error'] ?? "HTTP {$statusCode}",
                'idMapping' => []
            ];
        }

        return [
            'success' => $data['success'] ?? true,
            'message' => $data['message'] ?? 'OK',
            'idMapping' => $data['idMapping'] ?? []
        ];
    }

    // ──────────────────────────────────────────────────────────────────
    // ACCESSORS
    // ──────────────────────────────────────────────────────────────────

    public function getBaseUrl(): string
    {
        return $this->baseUrl;
    }

    public function getActiveVersion(): string
    {
        return $this->activeVersion ?? $this->apiVersions[0];
    }

    public function getApiConfig(): Configuration
    {
        return $this->apiConfig;
    }

    public function getModuleConfig(): Configuration
    {
        return $this->moduleConfig;
    }
}
