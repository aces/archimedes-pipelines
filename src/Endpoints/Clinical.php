<?php
declare(strict_types=1);

namespace LORIS\Endpoints;

use Psr\Log\LoggerInterface;

/**
 * Clinical Data API Endpoints
 * Handles clinical/instrument data operations via LORIS API
 *
 * Priority: USE API ENDPOINTS WHEN AVAILABLE
 */
class Clinical
{
    private Tokens $tokens;
    private LoggerInterface $logger;

    public function __construct(Tokens $tokens, LoggerInterface $logger)
    {
        $this->tokens = $tokens;
        $this->logger = $logger;
    }

    /**
     * Build full API URL dynamically using the active version
     */
    private function buildUrl(string $endpoint): string
    {
        $baseUrl = rtrim($this->tokens->getBaseUrl(), '/');
        $endpoint = ltrim($endpoint, '/');
        $version = $this->tokens->getActiveVersion();

        $url = "{$baseUrl}/{$version}/{$endpoint}";
        $this->logger->debug("Resolved API URL: {$url}");
        return $url;
    }

    /**
     * Upload instrument CSV file (bulk upload)
     * Fallbacks to web module if API unavailable
     */
    public function uploadInstrumentCSV(
        string $instrument,
        string $csvFilePath,
        string $action = 'CREATE_SESSIONS'
    ): array {
        if (!file_exists($csvFilePath)) {
            throw new \RuntimeException("CSV file not found: {$csvFilePath}");
        }

        $this->logger->info("Uploading instrument CSV: {$instrument}");
        $this->logger->debug("File: {$csvFilePath}, Action: {$action}");

        $fileSizeMB = round(filesize($csvFilePath) / 1024 / 1024, 2);
        $this->logger->info("File size: {$fileSizeMB} MB");

        $client = $this->tokens->getClient();
        $fileHandle = fopen($csvFilePath, 'r');

        if ($fileHandle === false) {
            throw new \RuntimeException("Failed to open CSV file: {$csvFilePath}");
        }

        try {
            // 1️⃣ Try API endpoint first
            $apiUrl = $this->buildUrl('/instrument_manager/instrument_data');
            $this->logger->debug("Trying API URL: {$apiUrl}");

            $response = $client->post($apiUrl, [
                'headers' => $this->tokens->getAuthHeaders(),
                'multipart' => [
                    ['name' => 'instrument', 'contents' => $instrument],
                    ['name' => 'action', 'contents' => $action],
                    ['name' => 'data_file', 'contents' => $fileHandle, 'filename' => basename($csvFilePath)]
                ]
            ]);

            // ✅ Reopen the file if we need to retry (Guzzle closes it after use)
            if ($response->getStatusCode() === 404) {
                $this->logger->warning("API endpoint not found, falling back to web module...");

                // Safely reopen file for fallback POST
                if (is_resource($fileHandle)) {
                    fclose($fileHandle);
                }
                $fileHandle = fopen($csvFilePath, 'r');
                if ($fileHandle === false) {
                    throw new \RuntimeException("Failed to reopen file for fallback: {$csvFilePath}");
                }

                $webUrl = $this->tokens->getBaseUrl() . '/instrument_manager/instrument_data';
                $this->logger->debug("Fallback web URL: {$webUrl}");

                $response = $client->post($webUrl, [
                    'headers' => $this->tokens->getAuthHeaders(),
                    'multipart' => [
                        ['name' => 'instrument', 'contents' => $instrument],
                        ['name' => 'action', 'contents' => $action],
                        ['name' => 'data_file', 'contents' => $fileHandle, 'filename' => basename($csvFilePath)]
                    ]
                ]);
            }

            if (is_resource($fileHandle)) {
                fclose($fileHandle);
            }

            $result = $this->parseResponse($response);

            if (!empty($result['success'])) {
                $this->logger->info("✓ Upload successful");
            } else {
                $this->logger->error("✗ Upload failed");
                if (isset($result['message'])) {
                    $msg = is_array($result['message']) ? json_encode($result['message']) : $result['message'];
                    $this->logger->error("  Message: {$msg}");
                }
            }

            return $result;

        } catch (\Exception $e) {
            if (is_resource($fileHandle)) {
                fclose($fileHandle);
            }
            $this->logger->error("✗ Exception during upload: " . $e->getMessage());
            throw $e;
        }
    }

    // ──────────────────────────────────────────────
    // API CALL HELPERS
    // ──────────────────────────────────────────────

    public function getInstruments(): array
    {
        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl('/instruments'), [
            'headers' => $this->tokens->getAuthHeaders()
        ]);
        return $this->parseResponse($response);
    }

    public function instrumentExists(string $instrument): bool
    {
        try {
            $result = $this->getInstruments();
            $instruments = $result['Instruments'] ?? [];
            return in_array($instrument, $instruments);
        } catch (\Exception $e) {
            $this->logger->debug("Instrument existence check failed: " . $e->getMessage());
            return true;
        }
    }

    public function createCandidate(array $candidateData): array
    {
        $client = $this->tokens->getClient();
        $response = $client->post($this->buildUrl('/candidates'), [
            'headers' => $this->tokens->getAuthHeaders(),
            'json' => $candidateData
        ]);
        return $this->parseResponse($response);
    }

    public function getCandidate(string $candID): array
    {
        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl("/candidates/{$candID}"), [
            'headers' => $this->tokens->getAuthHeaders()
        ]);
        return $this->parseResponse($response);
    }

    public function getCandidates(array $filters = []): array
    {
        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl('/candidates'), [
            'headers' => $this->tokens->getAuthHeaders(),
            'query' => $filters
        ]);
        return $this->parseResponse($response);
    }

    public function createTimepoint(string $candID, string $visitLabel, array $visitData): array
    {
        $client = $this->tokens->getClient();
        $response = $client->put($this->buildUrl("/candidates/{$candID}/{$visitLabel}"), [
            'headers' => $this->tokens->getAuthHeaders(),
            'json' => $visitData
        ]);
        return $this->parseResponse($response);
    }

    public function getVisit(string $candID, string $visitLabel): array
    {
        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl("/candidates/{$candID}/{$visitLabel}"), [
            'headers' => $this->tokens->getAuthHeaders()
        ]);
        return $this->parseResponse($response);
    }

    public function getInstrumentData(string $candID, string $visitLabel, string $instrument): array
    {
        $client = $this->tokens->getClient();
        $response = $client->get(
            $this->buildUrl("/candidates/{$candID}/{$visitLabel}/instruments/{$instrument}"),
            ['headers' => $this->tokens->getAuthHeaders()]
        );
        return $this->parseResponse($response);
    }

    public function uploadInstrumentData(
        string $candID,
        string $visitLabel,
        string $instrument,
        array $data
    ): array {
        $client = $this->tokens->getClient();
        $response = $client->put(
            $this->buildUrl("/candidates/{$candID}/{$visitLabel}/instruments/{$instrument}"),
            [
                'headers' => array_merge(
                    $this->tokens->getAuthHeaders(),
                    ['Content-Type' => 'application/json']
                ),
                'json' => $data
            ]
        );
        return $this->parseResponse($response);
    }

    /**
     * Parse HTTP response
     */
    private function parseResponse($response): array
    {
        $statusCode = $response->getStatusCode();
        $body = $response->getBody()->getContents();

        if (empty($body)) {
            return ['success' => $statusCode >= 200 && $statusCode < 300, 'statusCode' => $statusCode];
        }

        $data = json_decode($body, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new \RuntimeException("Invalid JSON (HTTP {$statusCode}): {$body}");
        }

        if ($statusCode >= 400) {
            $msg = $data['error'] ?? $data['message'] ?? $body;
            throw new \RuntimeException("API error {$statusCode}: {$msg}");
        }

        return $data;
    }
}
