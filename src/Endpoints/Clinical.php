<?php
declare(strict_types=1);

namespace LORIS\Client\Endpoints;

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
     * Build full URL from endpoint path
     * Since we don't use base_uri, we need to build full URLs
     */
    private function buildUrl(string $endpoint): string
    {
        $baseUrl = $this->tokens->getBaseUrl();
        $endpoint = ltrim($endpoint, '/');
        return "{$baseUrl}/{$endpoint}";
    }

    /**
     * Upload instrument CSV file - BULK UPLOAD
     * POST /instrument_manager/instrument_data
     * 
     * This is the PRIMARY method for clinical data ingestion
     * Automatically creates candidates and sessions if needed (CREATE_SESSIONS)
     * 
     * @param string $instrument Instrument name
     * @param string $csvFilePath Path to CSV file
     * @param string $action Action (CREATE_SESSIONS, VALIDATE_SESSIONS)
     * @return array Response data with detailed results
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
        
        // Log file details
        $fileSize = filesize($csvFilePath);
        $fileSizeMB = round($fileSize / 1024 / 1024, 2);
        $this->logger->info("  File size: {$fileSizeMB} MB");

        $client = $this->tokens->getClient();
        $fileHandle = fopen($csvFilePath, 'r');

        try {
            $this->logger->debug("Sending POST request to /instrument_manager/instrument_data");
            
            $response = $client->post($this->buildUrl('/instrument_manager/instrument_data'), [
                'headers' => $this->tokens->getAuthHeaders(),
                'multipart' => [
                    ['name' => 'instrument', 'contents' => $instrument],
                    ['name' => 'action', 'contents' => $action],
                    ['name' => 'data_file', 'contents' => $fileHandle, 'filename' => basename($csvFilePath)]
                ]
            ]);

            fclose($fileHandle);

            $result = $this->parseResponse($response);
            
            // Log the results
            if (isset($result['success']) && $result['success']) {
                $this->logger->info("  ✓ Upload successful");
                
                // Parse and log detailed results
                if (isset($result['message'])) {
                    $this->logger->info("  " . $result['message']);
                }
                
                // Log ID mappings if new candidates were created
                if (isset($result['idMapping']) && !empty($result['idMapping'])) {
                    $this->logger->info("  Created " . count($result['idMapping']) . " new candidate(s)");
                    foreach ($result['idMapping'] as $mapping) {
                        $this->logger->debug("    StudyID: {$mapping['ExtStudyID']} → CandID: {$mapping['CandID']}");
                    }
                }
            } else {
                $this->logger->error("  ✗ Upload failed");
                
                // Log errors in detail
                if (isset($result['message'])) {
                    if (is_array($result['message'])) {
                        $this->logger->error("  Errors:");
                        foreach ($result['message'] as $error) {
                            if (is_array($error)) {
                                $this->logger->error("    - " . ($error['message'] ?? json_encode($error)));
                            } else {
                                $this->logger->error("    - " . $error);
                            }
                        }
                    } else {
                        $this->logger->error("  " . $result['message']);
                    }
                }
            }
            
            return $result;
            
        } catch (\Exception $e) {
            fclose($fileHandle);
            $this->logger->error("  ✗ Exception during upload: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Get all instruments
     * GET /instruments
     */
    public function getInstruments(): array
    {
        $this->logger->debug("Fetching instruments list");

        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl('/instruments'), [
            'headers' => $this->tokens->getAuthHeaders()
        ]);

        return $this->parseResponse($response);
    }

    /**
     * Check if instrument exists in LORIS
     */
    public function instrumentExists(string $instrument): bool
    {
        try {
            $url = $this->buildUrl('/instruments');
            $this->logger->debug("Checking instrument existence at: {$url}");
            
            $result = $this->getInstruments();
            $instruments = $result['Instruments'] ?? [];
            return in_array($instrument, $instruments);
        } catch (\Exception $e) {
            // Log the error but don't fail - this is just a check
            $this->logger->debug("Could not check instrument existence: " . $e->getMessage());
            // Return true to allow upload to proceed
            return true;
        }
    }

    /**
     * Create new candidate
     * POST /candidates
     * 
     * @param array $candidateData Candidate information
     * @return array Response with CandID, PSCID
     */
    public function createCandidate(array $candidateData): array
    {
        $this->logger->info("Creating candidate via API");
        $this->logger->debug("Data: " . json_encode($candidateData));

        $client = $this->tokens->getClient();
        $response = $client->post($this->buildUrl('/candidates/'), [
            'headers' => $this->tokens->getAuthHeaders(),
            'json' => $candidateData
        ]);

        $result = $this->parseResponse($response);
        
        if (isset($result['CandID'])) {
            $this->logger->info("✓ Candidate created: CandID={$result['CandID']}, PSCID={$result['PSCID']}");
        }

        return $result;
    }

    /**
     * Get candidate information
     * GET /candidates/{candID}
     */
    public function getCandidate(string $candID): array
    {
        $this->logger->debug("Fetching candidate: {$candID}");

        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl("/candidates/{$candID}"), [
            'headers' => $this->tokens->getAuthHeaders()
        ]);

        return $this->parseResponse($response);
    }

    /**
     * Get all candidates (with optional filters)
     * GET /candidates
     */
    public function getCandidates(array $filters = []): array
    {
        $this->logger->debug("Fetching candidates" . ($filters ? " with filters" : ""));

        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl('/candidates'), [
            'headers' => $this->tokens->getAuthHeaders(),
            'query' => $filters
        ]);

        return $this->parseResponse($response);
    }

    /**
     * Create/Update timepoint (visit/session)
     * PUT /candidates/{candID}/{visitLabel}
     * 
     * @param string $candID Candidate ID
     * @param string $visitLabel Visit label (e.g., V1, Biospecimen01)
     * @param array $visitData Visit information
     * @return array Response data
     */
    public function createTimepoint(
        string $candID,
        string $visitLabel,
        array $visitData
    ): array {
        $this->logger->info("Creating timepoint: {$candID}/{$visitLabel}");
        $this->logger->debug("Data: " . json_encode($visitData));

        $client = $this->tokens->getClient();
        $response = $client->put($this->buildUrl("/candidates/{$candID}/{$visitLabel}"), [
            'headers' => $this->tokens->getAuthHeaders(),
            'json' => $visitData
        ]);

        $result = $this->parseResponse($response);
        $this->logger->info("✓ Timepoint created");

        return $result;
    }

    /**
     * Get visit/timepoint information
     * GET /candidates/{candID}/{visitLabel}
     */
    public function getVisit(string $candID, string $visitLabel): array
    {
        $this->logger->debug("Fetching visit: {$candID}/{$visitLabel}");

        $client = $this->tokens->getClient();
        $response = $client->get($this->buildUrl("/candidates/{$candID}/{$visitLabel}/"), [
            'headers' => $this->tokens->getAuthHeaders()
        ]);

        return $this->parseResponse($response);
    }

    /**
     * Get instrument data for specific candidate/visit
     * GET /candidates/{candID}/{visitLabel}/instruments/{instrument}
     */
    public function getInstrumentData(
        string $candID,
        string $visitLabel,
        string $instrument
    ): array {
        $this->logger->debug("Fetching instrument data: {$candID}/{$visitLabel}/{$instrument}");

        $client = $this->tokens->getClient();
        $response = $client->get(
            $this->buildUrl("/candidates/{$candID}/{$visitLabel}/instruments/{$instrument}"),
            ['headers' => $this->tokens->getAuthHeaders()]
        );

        return $this->parseResponse($response);
    }

    /**
     * Upload single instrument record
     * PUT /candidates/{candID}/{visitLabel}/instruments/{instrument}
     */
    public function uploadInstrumentData(
        string $candID,
        string $visitLabel,
        string $instrument,
        array $data
    ): array {
        $this->logger->info("Uploading instrument data: {$candID}/{$visitLabel}/{$instrument}");

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

        // Handle empty responses
        if (empty($body)) {
            if ($statusCode >= 200 && $statusCode < 300) {
                return ['success' => true, 'statusCode' => $statusCode];
            }
            throw new \RuntimeException("API error {$statusCode}");
        }

        // Decode JSON
        $data = json_decode($body, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            if ($statusCode >= 400) {
                throw new \RuntimeException("API error {$statusCode}: {$body}");
            }
            return ['data' => $body, 'statusCode' => $statusCode];
        }

        // Check for errors
        if ($statusCode >= 400) {
            $errorMsg = $data['error'] ?? $data['message'] ?? $body;
            throw new \RuntimeException("API error {$statusCode}: {$errorMsg}");
        }

        return $data;
    }
}
