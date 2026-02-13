<?php
declare(strict_types=1);

namespace LORIS\Endpoints;

use GuzzleHttp\Exception\RequestException;
use Psr\Log\LoggerInterface;
use GuzzleHttp\Client as GuzzleClient;

/**
 * Clinical Data Upload Client
 *
 * Strategy: Try using LORISClient generated API classes first, fall back to raw Guzzle on any error
 *
 * This ensures maximum compatibility - uses the clean generated API when available,
 * but always has a working fallback.
 */
class ClinicalClient
{
    private string $baseUrl;
    private string $username;
    private string $password;
    private int $tokenExpiryMinutes;
    private LoggerInterface $logger;

    private ?GuzzleClient $httpClient = null;
    private ?object $config = null;
    private ?object $authApi = null;
    private ?object $instrumentManagerApi = null;
    private ?object $instrumentsApi = null;

    private ?string $token = null;
    private ?int $tokenExpiry = null;

    // Track if we should skip trying generated API (after first failure)
    private bool $useGuzzleOnly = false;

    public function __construct(
        string $baseUrl,
        string $username,
        string $password,
        int $tokenExpiryMinutes,
        LoggerInterface $logger
    ) {
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->username = $username;
        $this->password = $password;
        $this->tokenExpiryMinutes = $tokenExpiryMinutes;
        $this->logger = $logger;
    }

    /**
     * Authenticate - try generated API first, fall back to Guzzle
     */
    public function authenticate(): void
    {
        $this->logger->debug("Authenticating with LORIS API...");

        // Create HTTP client (used by both approaches)
        $this->httpClient = new GuzzleClient([
            'verify' => false,
            'timeout' => 60,
            'connect_timeout' => 10,
        ]);

        // Try generated API first (unless we've already determined it won't work)
        if (!$this->useGuzzleOnly) {
            try {
                $this->authenticateWithGeneratedAPI();
                return; // Success!
            } catch (\Exception $e) {
                $this->logger->debug("Generated API authentication failed: " . $e->getMessage());
                $this->logger->debug("Falling back to raw Guzzle");
                $this->useGuzzleOnly = true; // Skip generated API for future calls
            }
        }

        // Fall back to raw Guzzle
        $this->authenticateWithGuzzle();
    }

    /**
     * Try authentication with generated LORISClient API
     */
    private function authenticateWithGeneratedAPI(): void
    {
        // Check if classes exist
        if (!class_exists('\\LORISClient\\Configuration')) {
            throw new \RuntimeException("LORISClient\\Configuration not found");
        }

        $configClass = '\\LORISClient\\Configuration';
        $authApiClass = '\\LORISClient\\Api\\AuthenticationApi';
        $loginRequestClass = '\\LORISClient\\Model\\LoginRequest';

        // Configure for REST API endpoints
        $this->config = $configClass::getDefaultConfiguration();
        $this->config->setHost($this->baseUrl . '/api/v0.0.3');

        // Create authentication API
        $this->authApi = new $authApiClass($this->httpClient, $this->config);

        // Login request
        $loginRequest = new $loginRequestClass([
            'username' => $this->username,
            'password' => $this->password
        ]);

        $loginResponse = $this->authApi->login($loginRequest);
        $this->token = $loginResponse->getToken();
        $this->tokenExpiry = time() + ($this->tokenExpiryMinutes * 60);

        // Update configuration with token
        $this->config->setAccessToken($this->token);

        // Initialize other API instances
        $moduleConfig = clone $this->config;
        $moduleConfig->setHost($this->baseUrl);

        if (class_exists('\\LORISClient\\Api\\InstrumentManagerApi')) {
            $this->instrumentManagerApi = new \LORISClient\Api\InstrumentManagerApi($this->httpClient, $moduleConfig);
        }

        if (class_exists('\\LORISClient\\Api\\InstrumentsApi')) {
            $this->instrumentsApi = new \LORISClient\Api\InstrumentsApi($this->httpClient, $this->config);
        }

        $this->logger->debug("✓ Authenticated using LORISClient API");
        $this->logger->debug("  Token expires in {$this->tokenExpiryMinutes} minutes");
    }

    /**
     * Authenticate with raw Guzzle (fallback)
     */
    private function authenticateWithGuzzle(): void
    {
        try {
            $loginUrl = $this->baseUrl . '/api/v0.0.3/login';

            $response = $this->httpClient->post($loginUrl, [
                'json' => [
                    'username' => $this->username,
                    'password' => $this->password
                ],
                'headers' => [
                    'Content-Type' => 'application/json',
                    'Accept' => 'application/json'
                ]
            ]);

            $result = json_decode($response->getBody()->getContents(), true);

            if (!isset($result['token'])) {
                throw new \RuntimeException("No token received from authentication");
            }

            $this->token = $result['token'];
            $this->tokenExpiry = time() + ($this->tokenExpiryMinutes * 60);

            $this->logger->debug("✓ Authenticated using raw Guzzle");
            $this->logger->debug("  Token expires in {$this->tokenExpiryMinutes} minutes");

        } catch (RequestException $e) {
            $statusCode = $e->getResponse() ? $e->getResponse()->getStatusCode() : 0;
            $responseBody = $e->getResponse() ? $e->getResponse()->getBody()->getContents() : '';
            $this->logger->error("✗ Authentication failed (HTTP {$statusCode}): " . $e->getMessage());
            $this->logger->debug("Response: " . $responseBody);
            throw new \RuntimeException("Failed to authenticate with LORIS API: " . $e->getMessage());
        }
    }

    /**
     * Ensure client is authenticated (refresh token if needed)
     */
    private function ensureAuthenticated(): void
    {
        if ($this->httpClient === null || $this->token === null) {
            throw new \RuntimeException("Not authenticated. Call authenticate() first.");
        }

        // Refresh token if expired or about to expire (5 min buffer)
        if ($this->tokenExpiry !== null && time() >= ($this->tokenExpiry - 300)) {
            $this->logger->debug("Token expiring soon, re-authenticating...");
            $this->authenticate();
        }
    }

    /**
     * Check if instrument exists - always use Guzzle (API method doesn't exist)
     */
    public function instrumentExists(string $instrument): bool
    {
        $this->ensureAuthenticated();

        $this->logger->debug("Checking if instrument '{$instrument}' exists in LORIS...");

        // Use raw Guzzle (getInstrument method doesn't exist in current API)
        try {
            $url = $this->baseUrl . "/api/v0.0.3/instruments/{$instrument}";

            $response = $this->httpClient->get($url, [
                'headers' => [
                    'Authorization' => 'Bearer ' . $this->token,
                    'Accept' => 'application/json'
                ],
                'http_errors' => false
            ]);

            $statusCode = $response->getStatusCode();

            if ($statusCode === 200) {
                $this->logger->debug("  ✓ Instrument '{$instrument}' exists");
                return true;
            } elseif ($statusCode === 404) {
                $this->logger->debug("  ✗ Instrument '{$instrument}' not found (404)");
                return false;
            }

            $this->logger->warning("  Unexpected status code {$statusCode} checking instrument");
            return false;

        } catch (\Exception $e) {
            $this->logger->warning("  Exception checking instrument: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Install instrument - try API first, fall back to Guzzle
     */
    public function installInstrument(string $filePath): bool
    {
        $this->ensureAuthenticated();

        if (!file_exists($filePath)) {
            throw new \InvalidArgumentException("Instrument file not found: {$filePath}");
        }

        $extension = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));

        $instrumentType = match($extension) {
            'linst' => 'linst',
            'csv' => 'redcap',
            'json' => 'bids',
            default => throw new \InvalidArgumentException("Unsupported file type: {$extension}")
        };

        $this->logger->debug("Installing instrument from {$instrumentType} file: " . basename($filePath));

        // Try generated API first
        if (!$this->useGuzzleOnly && $this->instrumentManagerApi) {
            try {
                $result = $this->instrumentManagerApi->installInstrument(
                    $instrumentType,
                    fopen($filePath, 'r')
                );

                if (isset($result['ok']) && $result['ok'] === 'ok') {
                    $this->logger->debug("  ✓ Instrument installed successfully");
                    return true;
                }
            } catch (\Exception $e) {
                $this->logger->debug("Generated API failed: " . $e->getMessage() . ", using Guzzle");
            }
        }

        // Fall back to raw Guzzle
        return $this->installInstrumentWithGuzzle($filePath, $instrumentType);
    }

    /**
     * Install instrument with raw Guzzle
     */
    private function installInstrumentWithGuzzle(string $filePath, string $instrumentType): bool
    {
        try {
            $url = $this->baseUrl . '/instrument_manager';

            $response = $this->httpClient->post($url, [
                'multipart' => [
                    [
                        'name' => 'instrument_type',
                        'contents' => $instrumentType
                    ],
                    [
                        'name' => 'install_file',
                        'contents' => fopen($filePath, 'r'),
                        'filename' => basename($filePath)
                    ]
                ],
                'headers' => [
                    'Authorization' => 'Bearer ' . $this->token,
                    'Accept' => 'application/json'
                ]
            ]);

            $statusCode = $response->getStatusCode();
            $result = json_decode($response->getBody()->getContents(), true);

            if ($statusCode === 201 && isset($result['ok']) && $result['ok'] === 'ok') {
                $this->logger->debug("  ✓ Instrument installed successfully");
                return true;
            }

            $this->logger->error("  ✗ Install failed (HTTP {$statusCode}): " . json_encode($result));
            return false;

        } catch (RequestException $e) {
            $statusCode = $e->getResponse() ? $e->getResponse()->getStatusCode() : 0;
            $responseBody = $e->getResponse() ? $e->getResponse()->getBody()->getContents() : '';
            $this->logger->error("  ✗ Install request failed (HTTP {$statusCode}): " . $e->getMessage());
            $this->logger->debug("  Response: " . $responseBody);
            return false;
        } catch (\Exception $e) {
            $this->logger->error("  ✗ Install exception: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Upload instrument data - always use Guzzle (needs format parameter)
     *
     * The generated API likely doesn't have the updated schema with format parameter,
     * so we always use raw Guzzle for this to ensure compatibility.
     */
    public function uploadInstrumentData(
        string $instrument,
        string $filePath,
        string $action = 'CREATE_SESSIONS',
        string $format = 'LORIS_CSV'
    ): array {
        $this->ensureAuthenticated();

        if (!file_exists($filePath)) {
            throw new \InvalidArgumentException("Data file not found: {$filePath}");
        }

        if (!in_array($action, ['CREATE_SESSIONS', 'VALIDATE_SESSIONS'])) {
            throw new \InvalidArgumentException("Invalid action: {$action}");
        }

        if (!in_array($format, ['LORIS_CSV', 'BIDS_TSV'])) {
            throw new \InvalidArgumentException("Invalid format: {$format}");
        }

        // CRITICAL: BIDS_TSV cannot create sessions
        if ($format === 'BIDS_TSV' && $action === 'CREATE_SESSIONS') {
            $this->logger->warning("BIDS_TSV format cannot use CREATE_SESSIONS, forcing VALIDATE_SESSIONS");
            $action = 'VALIDATE_SESSIONS';
        }

        $this->logger->debug("Uploading instrument data: {$instrument}");
        $this->logger->debug("  File: " . basename($filePath));
        $this->logger->debug("  Action: {$action}");
        $this->logger->debug("  Format: {$format}");

        // Always use Guzzle for instrument data upload (needs format parameter)
        try {
            $url = $this->baseUrl . '/instrument_manager/instrument_data';

            $response = $this->httpClient->post($url, [
                'multipart' => [
                    [
                        'name' => 'action',
                        'contents' => $action
                    ],
                    [
                        'name' => 'format',
                        'contents' => $format
                    ],
                    [
                        'name' => 'instrument',
                        'contents' => $instrument
                    ],
                    [
                        'name' => 'data_file',
                        'contents' => fopen($filePath, 'r'),
                        'filename' => basename($filePath)
                    ]
                ],
                'headers' => [
                    'Authorization' => 'Bearer ' . $this->token,
                    'Accept' => 'application/json'
                ]
            ]);

            $statusCode = $response->getStatusCode();
            $result = json_decode($response->getBody()->getContents(), true);

            if ($statusCode === 201 && isset($result['success']) && $result['success'] === true) {
                $this->logger->debug("  ✓ Upload successful");
                return $result;
            }

            if ($statusCode === 200 && isset($result['success']) && $result['success'] === false) {
                $this->logger->debug("  ✗ Upload failed (validation errors)");
                return $result;
            }

            $this->logger->warning("  ⚠ Unexpected response (HTTP {$statusCode})");
            return [
                'success' => false,
                'message' => 'Unexpected API response: ' . json_encode($result)
            ];

        } catch (RequestException $e) {
            $statusCode = $e->getResponse() ? $e->getResponse()->getStatusCode() : 0;
            $responseBody = $e->getResponse() ? $e->getResponse()->getBody()->getContents() : '';

            $this->logger->error("  ✗ Upload request failed (HTTP {$statusCode})");
            $this->logger->debug("  Response: " . $responseBody);

            $errorData = json_decode($responseBody, true);
            $errorMessage = $errorData['error'] ?? $errorData['message'] ?? $e->getMessage();

            return [
                'success' => false,
                'message' => $errorMessage
            ];

        } catch (\Exception $e) {
            $this->logger->error("  ✗ Upload exception: " . $e->getMessage());

            return [
                'success' => false,
                'message' => $e->getMessage()
            ];
        }
    }

    /**
     * Legacy method - maintained for backward compatibility
     */
    public function uploadInstrumentCSV(
        string $instrument,
        string $csvFile,
        string $action = 'CREATE_SESSIONS'
    ): array {
        return $this->uploadInstrumentData($instrument, $csvFile, $action, 'LORIS_CSV');
    }
}