<?php
/**
 * ARCHIMEDES Imaging Client
 * 
 * API client for BIDS imaging import using loris-php-api-client ScriptApi.
 * Calls POST /cbigr_api/script/bidsimport endpoint.
 * 
 * @package LORIS\Endpoints
 */

namespace LORIS\Endpoints;

use OpenAPI\Client\Api\AuthenticationApi;
use OpenAPI\Client\Api\ScriptApi;
use OpenAPI\Client\Configuration;
use OpenAPI\Client\Model\BidsImportRequest;
use GuzzleHttp\Client;

class ImagingClient
{
    private string $baseUrl;
    private bool $verifySsl;
    private ?string $token = null;
    private ?ScriptApi $scriptApi = null;
    private ?Configuration $config = null;
    
    private const DEFAULT_TIMEOUT = 600; // 10 minutes for BIDS import

    public function __construct(string $baseUrl, bool $verifySsl = true)
    {
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->verifySsl = $verifySsl;
    }

    /**
     * Authenticate with LORIS API
     */
    public function authenticate(string $username, string $password): bool
    {
        try {
            $this->config = new Configuration();
            $this->config->setHost($this->baseUrl);
            
            $client = new Client([
                'verify' => $this->verifySsl,
                'timeout' => 30,
            ]);
            
            $authApi = new AuthenticationApi($client, $this->config);
            $response = $authApi->login([
                'username' => $username,
                'password' => $password,
            ]);
            
            $this->token = $response->getToken();
            $this->config->setAccessToken($this->token);
            
            // Initialize ScriptApi with authenticated config
            $this->scriptApi = new ScriptApi(
                new Client([
                    'verify' => $this->verifySsl,
                    'timeout' => self::DEFAULT_TIMEOUT,
                ]),
                $this->config
            );
            
            return true;
        } catch (\Exception $e) {
            error_log("Authentication failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Run BIDS import via ScriptApi
     * 
     * @param string $bidsDirectory Path to BIDS directory
     * @param array $options Import options
     * @return array Result with 'success', 'data', 'error' keys
     */
    public function runBidsImport(string $bidsDirectory, array $options = []): array
    {
        if (!$this->scriptApi) {
            return [
                'success' => false,
                'error' => 'Not authenticated',
                'data' => null,
            ];
        }
        
        try {
            // Build flags array
            $flags = [];
            if ($options['create_candidate'] ?? true) {
                $flags[] = 'createcandidate';
            }
            if ($options['create_session'] ?? true) {
                $flags[] = 'createsession';
            }
            if ($options['verbose'] ?? false) {
                $flags[] = 'verbose';
            }
            if ($options['no_bids_validation'] ?? false) {
                $flags[] = 'nobidsvalidation';
            }
            
            // Build request
            $request = new BidsImportRequest([
                'args' => [
                    'directory' => $bidsDirectory,
                    'profile' => $options['profile'] ?? 'prod',
                ],
                'flags' => $flags,
                'async' => $options['async'] ?? false,
            ]);
            
            // Call API
            $response = $this->scriptApi->runBidsImport($request);
            
            // Check response type (sync vs async)
            if ($options['async'] ?? false) {
                // Async response: job_id, pid, log_file
                return [
                    'success' => true,
                    'async' => true,
                    'data' => [
                        'job_id' => $response->getJobId(),
                        'pid' => $response->getPid(),
                        'log_file' => $response->getLogFile(),
                    ],
                    'error' => null,
                ];
            } else {
                // Sync response: status, code, output, errors
                $status = $response->getStatus();
                $code = $response->getCode();
                $success = ($status === 'success' && $code === 0);
                
                return [
                    'success' => $success,
                    'async' => false,
                    'data' => [
                        'status' => $status,
                        'code' => $code,
                        'output' => $response->getOutput(),
                        'warnings' => $response->getWarnings() ?? [],
                        'errors' => $response->getErrors() ?? [],
                    ],
                    'error' => $success ? null : ($response->getErrors()[0] ?? 'Import failed'),
                ];
            }
        } catch (\Exception $e) {
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'data' => null,
            ];
        }
    }

    /**
     * Check async job status
     */
    public function checkJobStatus(string $jobId): array
    {
        if (!$this->scriptApi) {
            return [
                'success' => false,
                'error' => 'Not authenticated',
                'data' => null,
            ];
        }
        
        try {
            $response = $this->scriptApi->getScriptJobStatus($jobId);
            
            return [
                'success' => true,
                'data' => [
                    'job_id' => $response->getJobId(),
                    'status' => $response->getStatus(),
                    'progress' => $response->getProgress() ?? null,
                    'output' => $response->getOutput() ?? null,
                ],
                'error' => null,
            ];
        } catch (\Exception $e) {
            return [
                'success' => false,
                'error' => $e->getMessage(),
                'data' => null,
            ];
        }
    }

    public function getToken(): ?string
    {
        return $this->token;
    }

    public function isAuthenticated(): bool
    {
        return $this->token !== null;
    }
}
