<?php
declare(strict_types=1);

namespace LORIS\Client\Endpoints;

use GuzzleHttp\Client;
use Psr\Log\LoggerInterface;

/**
 * Authentication and JWT Token Management
 * Handles LORIS API authentication with token expiry
 * 
 * API Endpoint: POST /login
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

        // IMPORTANT: Don't use base_uri - it causes issues with some LORIS configurations
        // Instead, use full URLs in each request
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
     * Authenticate and get JWT token
     * POST /login
     */
    public function authenticate(): string
    {
        $this->logger->info('Authenticating with LORIS API...');
        $this->logger->debug("Base URL: {$this->baseUrl}");
        $this->logger->debug("Username: {$this->username}");

        try {
            $requestData = [
                'username' => $this->username,
                'password' => $this->password,
            ];
            
            // Use full URL, not relative path (base_uri causes issues)
            $loginUrl = $this->baseUrl . '/login';
            
            $this->logger->debug("Sending POST request to {$loginUrl}");
            $this->logger->debug("Request data: " . json_encode(['username' => $this->username, 'password' => '***']));
            
            $response = $this->client->post($loginUrl, [
                'json' => $requestData,
                'headers' => [
                    'Content-Type' => 'application/json'
                ]
            ]);

            $statusCode = $response->getStatusCode();
            $body = $response->getBody()->getContents();
            
            $this->logger->debug("Response status: {$statusCode}");
            $this->logger->debug("Response Content-Type: " . $response->getHeaderLine('Content-Type'));
            $this->logger->debug("Response body (first 500 chars): " . substr($body, 0, 500));

            if ($statusCode !== 200) {
                // Log response headers for debugging
                $this->logger->error("Authentication failed with status {$statusCode}");
                foreach ($response->getHeaders() as $name => $values) {
                    $this->logger->debug("Response header {$name}: " . implode(', ', $values));
                }
                throw new \RuntimeException("Authentication failed (HTTP {$statusCode}): {$body}");
            }

            $data = json_decode($body, true);

            if (json_last_error() !== JSON_ERROR_NONE) {
                $this->logger->error("Response is not valid JSON!");
                $this->logger->error("JSON Error: " . json_last_error_msg());
                $this->logger->error("Raw response body (first 500 chars):");
                $this->logger->error($body ? substr($body, 0, 500) : '(empty response)');
                
                // Detect common issues
                if (strpos($body, '<html') !== false || strpos($body, '<!DOCTYPE') !== false) {
                    $this->logger->error("⚠ Response is HTML, not JSON! This usually means:");
                    $this->logger->error("  - Wrong API URL (getting web page instead of API endpoint)");
                    $this->logger->error("  - API endpoint doesn't exist at this path");
                    $this->logger->error("  - Redirect to login page");
                    $this->logger->error("Check your base_url in config: {$this->baseUrl}");
                }
                
                throw new \RuntimeException('Invalid JSON in authentication response: ' . json_last_error_msg() . ". Check logs for raw response.");
            }

            if (!isset($data['token'])) {
                $this->logger->error("No token in response. Response keys: " . implode(', ', array_keys($data)));
                throw new \RuntimeException('No token in authentication response');
            }

            $this->token = $data['token'];
            $this->tokenExpiry = time() + ($this->tokenExpiryMinutes * 60);
            
            $this->logger->info('✓ Authenticated successfully');
            $this->logger->debug("Token expires in {$this->tokenExpiryMinutes} minutes");
            $this->logger->debug("Token (first 20 chars): " . substr($this->token, 0, 20) . "...");

            return $this->token;
            
        } catch (\GuzzleHttp\Exception\ConnectException $e) {
            $this->logger->error("Connection failed: Cannot reach {$this->baseUrl}");
            $this->logger->error("Error: " . $e->getMessage());
            throw new \RuntimeException("Cannot connect to LORIS API at {$this->baseUrl}. Check base_url in config.");
        } catch (\GuzzleHttp\Exception\RequestException $e) {
            $this->logger->error("Request failed: " . $e->getMessage());
            if ($e->hasResponse()) {
                $response = $e->getResponse();
                $body = $response->getBody()->getContents();
                $this->logger->error("Response: " . $body);
            }
            throw $e;
        } catch (\RuntimeException $e) {
            throw $e;
        } catch (\Exception $e) {
            $this->logger->error("Authentication failed: " . $e->getMessage());
            $this->logger->error("Stack trace: " . $e->getTraceAsString());
            throw $e;
        }
    }

    /**
     * Get valid token (re-authenticate if expired)
     */
    public function getToken(): string
    {
        // No token yet
        if ($this->token === null) {
            return $this->authenticate();
        }

        // Token expired - re-authenticate
        if ($this->tokenExpiry !== null && time() >= $this->tokenExpiry) {
            $this->logger->info('Token expired, re-authenticating...');
            return $this->authenticate();
        }

        return $this->token;
    }

    /**
     * Check if token is valid
     */
    public function hasValidToken(): bool
    {
        if ($this->token === null) {
            return false;
        }

        if ($this->tokenExpiry !== null && time() >= $this->tokenExpiry) {
            return false;
        }

        return true;
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
     * Get HTTP client
     */
    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * Get base URL
     */
    public function getBaseUrl(): string
    {
        return $this->baseUrl;
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
