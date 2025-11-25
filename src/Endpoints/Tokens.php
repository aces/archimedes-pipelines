<?php
declare(strict_types=1);

namespace LORIS\Endpoints;

use GuzzleHttp\Client;
use Psr\Log\LoggerInterface;

/**
 * Authentication and JWT Token Management
 * Handles LORIS API authentication with version fallback
 */
class Tokens
{
    private Client $client;
    private string $baseUrl;
    private string $username;
    private string $password;
    private ?string $token = null;
    private ?int $tokenExpiry = null;
    private int $tokenExpiryMinutes;
    private LoggerInterface $logger;

    private ?string $activeVersion = null;

    /**
     * List of API versions to try (in order of preference)
     */
    private array $apiVersions = [
        'api/v0.0.3',
        'api/v0.0.4-dev'
    ];

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
        $this->logger = $logger ?? new \Psr\Log\NullLogger();

        // Don't use base_uri — causes issues with some LORIS setups
        $this->client = new Client([
            'timeout' => 120,
            'http_errors' => false,
            'verify' => false,
            'headers' => [
                'Accept' => 'application/json',
                'User-Agent' => 'LORIS-PHP-Client/1.0'
            ]
        ]);
    }

    /**
     * Authenticate and get JWT token (tries multiple API versions)
     */
    public function authenticate(): string
    {
        $this->logger->info('Authenticating with LORIS API...');
        $this->logger->debug("Base URL: {$this->baseUrl}");
        $this->logger->debug("Username: {$this->username}");

        $lastError = null;

        foreach ($this->apiVersions as $version) {
            $loginUrl = "{$this->baseUrl}/{$version}/login";
            $this->logger->info("🔍 Trying authentication endpoint: {$loginUrl}");

            try {
                $response = $this->client->post($loginUrl, [
                    'json' => [
                        'username' => $this->username,
                        'password' => $this->password,
                    ],
                    'headers' => ['Content-Type' => 'application/json']
                ]);

                $statusCode = $response->getStatusCode();
                $body = $response->getBody()->getContents();

                $this->logger->debug("Response status: {$statusCode}");
                $this->logger->debug("Response body (first 200 chars): " . substr($body, 0, 200));

                if ($statusCode !== 200) {
                    $this->logger->warning("Authentication failed for {$version} (HTTP {$statusCode})");
                    $lastError = "HTTP {$statusCode}: {$body}";
                    continue;
                }

                $data = json_decode($body, true);

                if (json_last_error() !== JSON_ERROR_NONE) {
                    $this->logger->warning("Invalid JSON for {$version}: " . json_last_error_msg());
                    $lastError = "Invalid JSON response";
                    continue;
                }

                if (!isset($data['token'])) {
                    $this->logger->warning("No token found in response for {$version}");
                    $lastError = 'No token key in response';
                    continue;
                }

                // ✅ Success
                $this->token = $data['token'];
                $this->tokenExpiry = time() + ($this->tokenExpiryMinutes * 60);
                $this->activeVersion = $version; // store working version

                $this->logger->info("✓ Authenticated successfully using {$version}");
                $this->logger->debug("Token expires in {$this->tokenExpiryMinutes} minutes");
                return $this->token;

            } catch (\GuzzleHttp\Exception\ConnectException $e) {
                $this->logger->warning("Connection failed for {$version}: " . $e->getMessage());
                $lastError = $e->getMessage();
            } catch (\Exception $e) {
                $this->logger->warning("Unexpected error for {$version}: " . $e->getMessage());
                $lastError = $e->getMessage();
            }
        }

        throw new \RuntimeException("Authentication failed for all API versions. Last error: {$lastError}");
    }

    /**
     * Get valid token (re-authenticate if expired)
     */
    public function getToken(): string
    {
        if ($this->token === null || ($this->tokenExpiry !== null && time() >= $this->tokenExpiry)) {
            $this->logger->info('Fetching new token...');
            return $this->authenticate();
        }
        return $this->token;
    }

    /**
     * Check if a valid token exists
     */
    public function hasValidToken(): bool
    {
        return $this->token !== null &&
            $this->tokenExpiry !== null &&
            time() < $this->tokenExpiry;
    }

    /**
     * Force token refresh
     */
    public function refreshToken(): string
    {
        $this->logger->info('Forcing token refresh...');
        $this->token = null;
        $this->tokenExpiry = null;
        return $this->authenticate();
    }

    /**
     * Return the working HTTP client
     */
    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * Return the base LORIS URL
     */
    public function getBaseUrl(): string
    {
        return $this->baseUrl;
    }

    /**
     * Get the active API version that succeeded during authentication
     */
    public function getActiveVersion(): string
    {
        return $this->activeVersion ?? ($this->apiVersions[0] ?? 'api/v0.0.4-dev');
    }

    /**
     * Get authorization headers
     */
    public function getAuthHeaders(): array
    {
        return [
            'Authorization' => 'Bearer ' . $this->getToken(),
            'Accept' => 'application/json',
        ];
    }
}
