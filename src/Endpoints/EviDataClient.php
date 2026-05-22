<?php
declare(strict_types=1);

namespace LORIS\Endpoints;

use CURLFile;
use RuntimeException;

/**
 * EviData Privacy Service Client
 *
 * Thin HTTP client for the Woodway Assurance EviData privacy service.
 * Used by ClinicalPipeline as a pre-flight gate: every CSV/TSV
 * destined for LORIS is uploaded to EviData, a privacy risk report
 * is generated, and the boolean `overall_passed` field decides
 * whether the pipeline proceeds.
 *
 * Auth flow (Keycloak password grant):
 *   - Single POST to token_url returns a bearer token
 *   - Token is cached on the instance; checkBatch() authenticates once
 *     even when processing N files
 *   - The 'openid' scope is required: the EviData API rejects tokens
 *     issued without it (HTTP 401), even though Keycloak returns 200.
 *
 * Report lifecycle per file:
 *   1. POST /datasets/upload          -> dataset_id      (multipart)
 *   2. POST /reports/generate         -> report_id       (JSON)
 *   3. GET  /reports/{report_id}      -> poll status until completed
 *   4. GET  /reports/{report_id}/results       -> JSON pass/fail payload
 *   5. GET  /reports/{report_id}/download-zip  -> bundled PDF artifact
 *
 * Per-file quasi-identifiers:
 *   EviData's server-side QI validation is CASE-SENSITIVE and matches
 *   the QI names sent in /reports/generate against the uploaded CSV's
 *   columns exactly. Different source files may use different column
 *   names or casing (e.g. 'Sex' vs 'sex'), so the QI list is resolved
 *   PER FILE by the caller (ClinicalPipeline) and passed in alongside
 *   each path — see checkBatch(). There is no single global QI list.
 *
 * Failure semantics:
 *   - overall_passed=false        -> file failed the privacy check
 *   - HTTP / auth / timeout error -> treated as a hard failure;
 *                                    pipeline cannot verify privacy,
 *                                    so it must not ingest
 *   - per-file errors in checkBatch() are CAPTURED, not thrown — the
 *     caller always sees a complete per-file result map
 *
 * @package LORIS\Endpoints
 */
class EviDataClient
{
    /** EviData config block, sourced from config/evidata_config.json. */
    private array $cfg;

    /** Bearer token from Keycloak; populated by authenticate(). */
    private ?string $token = null;

    /** Default polling interval (seconds) if not in config. */
    private const DEFAULT_POLL_INTERVAL = 10;

    /** Default polling timeout (seconds) if not in config. */
    private const DEFAULT_POLL_TIMEOUT = 600;

    /** cURL timeout for non-poll HTTP calls (seconds). */
    private const HTTP_TIMEOUT = 120;

    /**
     * OAuth2 scope requested in the token grant. The EviData API
     * requires the 'openid' scope on the bearer token; a token issued
     * without it is rejected with HTTP 401 "Failed to authenticate
     * user" even though Keycloak itself returns the token with 200.
     * Overridable via the optional 'scope' config key.
     */
    private const DEFAULT_SCOPE = 'openid profile email';

    // ──────────────────────────────────────────────────────────────────
    //  Construction
    // ──────────────────────────────────────────────────────────────────

    /**
     * Build a client from the resolved EviData config block.
     *
     * Required keys: api_base_url, token_url, client_id,
     *                client_secret_env, username_env, password_env.
     * Optional:      analysis_type, client_name, recipient_name,
     *                population_size, poll_interval_seconds,
     *                poll_timeout_seconds, scope.
     *
     * NOTE: 'qis' is NOT read from config here. QI lists are resolved
     * per file by the caller and passed into checkBatch() / check().
     */
    public function __construct(array $evidataConfig)
    {
        $required = [
            'api_base_url', 'token_url', 'client_id',
            'client_secret_env', 'username_env', 'password_env',
        ];
        foreach ($required as $key) {
            if (empty($evidataConfig[$key])) {
                throw new RuntimeException("EviData config missing required key '{$key}'");
            }
        }
        $this->cfg = $evidataConfig;
    }

    // ══════════════════════════════════════════════════════════════════
    //  Public API
    // ══════════════════════════════════════════════════════════════════

    /**
     * Check one CSV/TSV against the given quasi-identifier list.
     * Returns true if EviData reports overall_passed=true.
     *
     * Throws on transport / auth / timeout failure. For batch use
     * with fail-as-result semantics, prefer checkBatch().
     *
     * @param string        $csvPath Absolute path to the CSV/TSV.
     * @param array<string> $qis     QI column names for THIS file
     *                               (case must match the file's headers).
     */
    public function check(string $csvPath, array $qis): bool
    {
        $this->authenticate();
        $datasetId = $this->uploadDataset($csvPath);
        $reportId  = $this->generateReport($datasetId, $qis);
        $this->waitForCompletion($reportId);
        $results = $this->http('GET', "/reports/{$reportId}/results");
        return (bool)($results['overall_passed'] ?? false);
    }

    /**
     * Check multiple CSV/TSV files in one batch, each against its own
     * quasi-identifier list.
     *
     * Authenticates ONCE, then for each file runs the full lifecycle
     * (upload -> generate -> poll -> results -> download-zip). Per-file
     * exceptions are caught and recorded as passed=false entries so
     * the caller sees a complete view even if some files errored
     * mid-way.
     *
     * @param array<string, array<string>> $filesWithQis
     *        Map of absolute CSV/TSV path => QI column list for that
     *        file. The QI names must match that file's header casing —
     *        EviData's server-side QI validation is case-sensitive.
     *
     * @return array<string, array{
     *     passed:     bool,
     *     error:      ?string,
     *     report_id:  ?string,
     *     results:    ?array,   // /reports/{id}/results JSON payload
     *     report_zip: ?string,  // raw bytes of /download-zip, null on failure
     * }> Keyed by basename.
     */
    public function checkBatch(array $filesWithQis): array
    {
        $this->authenticate();

        $results = [];
        foreach ($filesWithQis as $csvPath => $qis) {
            $name = basename($csvPath);
            try {
                if (!is_array($qis) || empty($qis)) {
                    throw new RuntimeException(
                        "No quasi-identifier list supplied for " . $name
                    );
                }

                $datasetId      = $this->uploadDataset($csvPath);
                $reportId       = $this->generateReport($datasetId, $qis);
                $this->waitForCompletion($reportId);
                $resultsPayload = $this->http('GET', "/reports/{$reportId}/results");

                // Pull the bundled report ZIP unconditionally — compliance
                // wants the artifact whether the file passed or failed.
                // Passing reports are audit evidence; failing reports
                // show what went wrong.
                $zipBytes = $this->downloadReportZip($reportId);

                $results[$name] = [
                    'passed'     => (bool)($resultsPayload['overall_passed'] ?? false),
                    'error'      => null,
                    'report_id'  => $reportId,
                    'results'    => $resultsPayload,
                    'report_zip' => $zipBytes,
                ];
            } catch (\Throwable $e) {
                // Network / auth / timeout / EviData-side failure.
                // No artifacts to record; the error message is the
                // only thing we know.
                $results[$name] = [
                    'passed'     => false,
                    'error'      => $e->getMessage(),
                    'report_id'  => null,
                    'results'    => null,
                    'report_zip' => null,
                ];
            }
        }
        return $results;
    }

    // ══════════════════════════════════════════════════════════════════
    //  Authentication
    // ══════════════════════════════════════════════════════════════════

    /**
     * Acquire a bearer token via OAuth2 password grant and cache it
     * on the instance. Idempotent — subsequent calls reuse the cached
     * token until the instance is destroyed.
     *
     * Credentials are read from environment variables whose NAMES live
     * in config (client_secret_env, username_env, password_env). The
     * values themselves never appear in JSON or logs.
     *
     * The 'openid' scope MUST be requested — without it the EviData
     * API rejects every call with HTTP 401, even though Keycloak
     * issues the token successfully. See DEFAULT_SCOPE.
     */
    private function authenticate(): void
    {
        if ($this->token !== null) {
            return;
        }

        $clientSecret = getenv($this->cfg['client_secret_env']);
        $username     = getenv($this->cfg['username_env']);
        $password     = getenv($this->cfg['password_env']);

        if (empty($clientSecret) || empty($username) || empty($password)) {
            throw new RuntimeException(
                "EviData credentials missing from environment — expected "
                . "{$this->cfg['client_secret_env']}, "
                . "{$this->cfg['username_env']}, "
                . "{$this->cfg['password_env']}"
            );
        }

        $payload = http_build_query([
            'grant_type'    => 'password',
            'client_id'     => $this->cfg['client_id'],
            'client_secret' => $clientSecret,
            'username'      => $username,
            'password'      => $password,
            // Required: the EviData API rejects tokens issued without
            // the openid scope (401 "Failed to authenticate user"),
            // even though Keycloak returns 200. Config-overridable.
            'scope'         => $this->cfg['scope'] ?? self::DEFAULT_SCOPE,
        ]);

        $resp = $this->raw(
            'POST',
            $this->cfg['token_url'],
            $payload,
            ['Content-Type: application/x-www-form-urlencoded']
        );

        $token = $resp['access_token'] ?? null;
        if (empty($token)) {
            throw new RuntimeException("EviData authentication failed — no access_token in response");
        }
        $this->token = $token;
    }

    // ══════════════════════════════════════════════════════════════════
    //  Report lifecycle
    // ══════════════════════════════════════════════════════════════════

    /**
     * Upload one CSV to /datasets/upload as multipart/form-data.
     * Returns the dataset_id used by /reports/generate.
     */
    private function uploadDataset(string $csvPath): string
    {
        if (!is_readable($csvPath)) {
            throw new RuntimeException("CSV not readable: {$csvPath}");
        }

        $resp = $this->http(
            'POST',
            '/datasets/upload',
            [
                'file'          => new CURLFile($csvPath, 'text/csv', basename($csvPath)),
                'analysis_type' => $this->cfg['analysis_type'] ?? 'de-identified',
            ],
            multipart: true
        );

        $datasetId = $resp['dataset_id'] ?? null;
        if (empty($datasetId)) {
            throw new RuntimeException(
                "EviData upload returned no dataset_id for " . basename($csvPath)
            );
        }
        return (string)$datasetId;
    }

    /**
     * Kick off async report generation for an uploaded dataset.
     *
     * The QI list is supplied PER CALL (not read from config) and used
     * for both p2s_qis (population-to-sample) and s2p_qis
     * (sample-to-population) — the pre-flight check is symmetric.
     *
     * EviData validates these QI names against the uploaded dataset's
     * columns CASE-SENSITIVELY; the caller is responsible for passing
     * names that match the specific file's header casing.
     *
     * @param string        $datasetId Dataset id from uploadDataset().
     * @param array<string> $qis       QI column names for this dataset.
     */
    private function generateReport(string $datasetId, array $qis): string
    {
        if (empty($qis)) {
            throw new RuntimeException(
                "generateReport called with an empty QI list (dataset {$datasetId})"
            );
        }

        // Re-index so the JSON encodes as an array, not an object.
        $qis = array_values($qis);

        $resp = $this->http('POST', '/reports/generate', [
            'data_type'               => 'De-identified',
            'client_name'             => $this->cfg['client_name']     ?? 'Archimedes',
            'recipient_name'          => $this->cfg['recipient_name']  ?? 'Archimedes Pipeline',
            'population_size'         => $this->cfg['population_size'] ?? 50000,
            'deidentified_dataset_id' => $datasetId,
            'p2s_qis'                 => $qis,
            's2p_qis'                 => $qis,
        ]);

        $reportId = $resp['report_id'] ?? null;
        if (empty($reportId)) {
            throw new RuntimeException("EviData /reports/generate returned no report_id");
        }
        return (string)$reportId;
    }

    /**
     * Poll /reports/{id} every poll_interval_seconds until status is
     * 'completed' (success) or 'failed' (EviData-side error). Times
     * out after poll_timeout_seconds.
     *
     * EviData status='failed' is treated as a hard fail: the privacy
     * check produced no verdict, and silence is not a pass.
     */
    private function waitForCompletion(string $reportId): void
    {
        $interval = (int)($this->cfg['poll_interval_seconds'] ?? self::DEFAULT_POLL_INTERVAL);
        $timeout  = (int)($this->cfg['poll_timeout_seconds']  ?? self::DEFAULT_POLL_TIMEOUT);
        $deadline = time() + $timeout;

        while (time() < $deadline) {
            $r      = $this->http('GET', "/reports/{$reportId}");
            $status = $r['status'] ?? '';

            if ($status === 'completed') {
                return;
            }
            if ($status === 'failed') {
                $errMsg = $r['error_message'] ?? '(no error message provided)';
                throw new RuntimeException("EviData report failed: {$errMsg}");
            }
            sleep($interval);
        }
        throw new RuntimeException(
            "EviData report {$reportId} timed out after {$timeout}s"
        );
    }

    /**
     * Fetch the bundled report ZIP from /reports/{id}/download-zip.
     * Returns raw bytes for the caller to persist or attach.
     *
     * Uses the low-level cURL path directly because the response is
     * binary, not JSON — the standard http()/raw() decode path doesn't
     * apply.
     */
    private function downloadReportZip(string $reportId): string
    {
        $url = rtrim($this->cfg['api_base_url'], '/')
            . "/reports/{$reportId}/download-zip";

        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_CUSTOMREQUEST  => 'GET',
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER     => ["Authorization: Bearer {$this->token}"],
            CURLOPT_TIMEOUT        => self::HTTP_TIMEOUT,
            CURLOPT_FOLLOWLOCATION => true,   // matches `-L` in the cURL examples
        ]);
        $body = curl_exec($ch);
        $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err  = curl_error($ch);
        curl_close($ch);

        if ($body === false) {
            throw new RuntimeException(
                "EviData report ZIP download failed (report_id={$reportId}): {$err}"
            );
        }
        if ($code >= 400) {
            throw new RuntimeException(
                "EviData report ZIP download HTTP {$code} (report_id={$reportId})"
            );
        }
        return $body;
    }

    // ══════════════════════════════════════════════════════════════════
    //  HTTP layer
    // ══════════════════════════════════════════════════════════════════

    /**
     * Convenience wrapper that prefixes api_base_url, adds the bearer
     * auth header, and JSON-encodes the body (unless multipart).
     */
    private function http(
        string $method,
        string $path,
        array|string|null $body = null,
        bool $multipart = false
    ): array {
        $headers = ["Authorization: Bearer {$this->token}"];
        $payload = $body;

        if ($body !== null && !$multipart) {
            $headers[] = 'Content-Type: application/json';
            $payload   = json_encode($body);
        }

        return $this->raw(
            $method,
            rtrim($this->cfg['api_base_url'], '/') . $path,
            $payload,
            $headers
        );
    }

    /**
     * Low-level cURL wrapper used by both http() and authenticate().
     * Returns the decoded JSON body. Throws on HTTP >= 400 or cURL
     * error, with the response body trimmed into the message so the
     * caller can see what EviData said.
     */
    private function raw(string $method, string $url, $body, array $headers): array
    {
        $ch = curl_init($url);
        if ($ch === false) {
            throw new RuntimeException("curl_init failed for {$url}");
        }

        curl_setopt_array($ch, [
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER     => $headers,
            CURLOPT_TIMEOUT        => self::HTTP_TIMEOUT,
            CURLOPT_CONNECTTIMEOUT => 10,
        ]);

        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
        }

        $rawBody  = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $curlErr  = curl_error($ch);
        curl_close($ch);

        if ($rawBody === false) {
            throw new RuntimeException(
                "EviData {$method} {$url} — cURL error: {$curlErr}"
            );
        }

        if ($httpCode >= 400) {
            // Trim response body so the message stays readable in
            // logs and emails. 500 chars covers most EviData errors.
            $excerpt = strlen($rawBody) > 500
                ? substr($rawBody, 0, 500) . '…'
                : $rawBody;
            throw new RuntimeException(
                "EviData {$method} {$url} -> HTTP {$httpCode}: {$excerpt}"
            );
        }

        $decoded = json_decode($rawBody, true);
        return is_array($decoded) ? $decoded : [];
    }
}