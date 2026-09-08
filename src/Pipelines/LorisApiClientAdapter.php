<?php

/**
 * LorisApiClientAdapter
 *
 * Implements LorisApiClientInterface for the DICOM pipeline, mirroring the
 * candidate-creation strategy already proven in BidsParticipantSync.
 *
 * Candidate creation strategy (same as BIDS, deliberately):
 *   1. CandidatesPlus (POST /cbigr_api/candidatesPlus) - accepts
 *      ProjectExternalName + ExtStudyID directly. The server resolves the
 *      external-project lookup and inserts candidate_project_extid_rel
 *      atomically, so no separate linking call is needed.
 *   2. Legacy two-step fallback - POST /api/{version}/candidates, then link
 *      via candidate_parameters/ajax/formHandler.php with the numeric
 *      ProjectID. Kept until CandidatesPlus ships everywhere.
 *
 * The fallback triggers ONLY on 404 or a transport error. A 400/409/500 is a
 * real problem - bad project name, duplicate PSCID - and the legacy path would
 * fail identically, so it is surfaced rather than retried. Once CandidatesPlus
 * 404s, it is not tried again for the rest of the run.
 *
 * Lookup goes through /cbigr_api/externalToInternalIdMapper, the same mapper
 * BidsParticipantSync uses.
 *
 * No database connection.
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines;

use RuntimeException;

class LorisApiClientAdapter implements LorisApiClientInterface
{
    private array $config;

    private string $baseUrl;

    private string $version;

    private ?object $logger;

    private ?string $token = null;

    private ?string $site = null;

    /** Exact LORIS Project.Name, e.g. "FDG PET". */
    private ?string $projectName = null;

    /**
     * Numeric ProjectExternalID. Used two ways: the pre-PR CandidatesPlus
     * payload, and the legacy formHandler linking call.
     */
    private ?string $projectExternalId = null;

    /**
     * Value sent as ProjectExternalName. Must match an entry in
     * ProjectCBIGR::getExternalProjects() - the Project_external table - which
     * is NOT necessarily the same as LORIS Project.Name. They often coincide,
     * so candidate_defaults.project is the default, but
     * candidate_defaults.project_external_name overrides it when they differ.
     */
    private ?string $projectExternalName = null;

    /**
     * Latches false once CandidatesPlus 404s, so the rest of the run goes
     * straight to the legacy path instead of retrying an absent endpoint.
     */
    private bool $candidatesPlusAvailable = true;

    /** @var array<string, array{pscid: string, cand_id: string}> */
    private array $resolved = [];

    /** @var string[]|null */
    private ?array $visitLabels = null;

    /** @var array<string, bool> */
    private array $sessionsEnsured = [];

    public function __construct(array $config, ?object $logger = null)
    {
        $loris = $config['loris'] ?? $config['api'] ?? [];

        if (empty($loris['base_url'])) {
            throw new RuntimeException('loris.base_url is not configured');
        }

        $this->config  = $config;
        $this->baseUrl = rtrim($loris['base_url'], '/');
        $this->version = $loris['api_version'] ?? 'v0.0.4-dev';
        $this->logger  = $logger;

        if (!function_exists('curl_init')) {
            throw new RuntimeException('ext-curl is required');
        }
    }

    public function setSite(?string $site): void
    {
        $this->site = $site;
    }

    public function setToken(string $token): void
    {
        $this->token = $token;
    }

    /**
     * Resolve project identity from project.json.
     *
     * ProjectExternalName is the single source BidsParticipantSync uses for
     * CandidatesPlus: candidate_defaults.project, and nothing else. If it is
     * absent, CandidatesPlus cannot be used and the legacy path takes over -
     * which is why the numeric ID is resolved too.
     */
    public function resolveProject(array $projectConfig, array $config = []): void
    {
        $defaults = $projectConfig['candidate_defaults'] ?? [];

        // Same single source as _getProjectExternalName(). Deliberately not a
        // fallback chain: sending a guessed name to CandidatesPlus produces a
        // hard 400, not a fallback.
        if (!empty($defaults['project'])) {
            $this->projectName = (string) $defaults['project'];
        }

        // ProjectExternalName defaults to the LORIS project name, since in
        // practice they usually match, but is separately overridable.
        $this->projectExternalName = !empty($defaults['project_external_name'])
            ? (string) $defaults['project_external_name']
            : $this->projectName;

        if ($this->projectName === null) {
            $this->warn(
                'candidate_defaults.project is not set - CandidatesPlus cannot '
                . 'be used, falling back to the legacy candidate-creation path'
            );
        }

        // Numeric ID: legacy linking only. Same priority as _getProjectExternalID().
        if (!empty($defaults['project_external_id'])) {
            $this->projectExternalId = (string) $defaults['project_external_id'];
            return;
        }

        $mappings = $projectConfig['project_mappings'] ?? [];
        $name     = $this->projectName;

        if ($name !== null && isset($mappings[$name])) {
            $this->projectExternalId = (string) $mappings[$name];
            return;
        }

        if ($name !== null) {
            $wanted = str_replace([' ', '_'], '-', strtolower($name));

            foreach ($mappings as $key => $value) {
                if (str_replace([' ', '_'], '-', strtolower((string) $key)) === $wanted) {
                    $this->projectExternalId = (string) $value;
                    return;
                }
            }
        }

        $this->projectExternalId = $config['loris']['project_external_id']
            ?? $config['api']['project_external_id']
            ?? null;

        if ($this->projectExternalId === null) {
            // Not fatal: CandidatesPlus needs only the name. It becomes fatal
            // only if the legacy path is reached, which is checked there.
            $this->info(sprintf(
                'No ProjectExternalID configured - using ProjectExternalName '
                . "'%s' via CandidatesPlus. The legacy fallback will not be "
                . 'available on a server without it.',
                (string) ($this->projectExternalName ?? '?')
            ));
        }
    }

    public function getProjectName(): ?string
    {
        return $this->projectName;
    }

    // =========================================================================
    //  LOOKUP
    // =========================================================================

    /**
     * Resolve an external identifier through the CBIGR mapper.
     *
     * Returns PSCID only - the mapper does not give a CandID. That is enough
     * for the reidentifier, which needs the PSCID for the directory name and
     * the header rewrite.
     */
    public function findCandidateByExternalId(string $externalId): ?array
    {
        if (isset($this->resolved[$externalId])) {
            return $this->resolved[$externalId];
        }

        $response = $this->raw(
            'POST',
            $this->baseUrl . '/cbigr_api/externalToInternalIdMapper',
            [$externalId]
        );

        if ($response['status'] !== 200) {
            return null;
        }

        // The mapper answers in CSV: header line, then one line per ID.
        $lines = explode("\n", trim($response['raw']));

        if (count($lines) < 2) {
            return null;
        }

        $parts = str_getcsv($lines[1]);

        if (count($parts) < 2) {
            return null;
        }

        $pscid = trim($parts[1]);

        if ($pscid === '' || $pscid === 'unauthorized_access') {
            return null;
        }

        $this->resolved[$externalId] = ['pscid' => $pscid, 'cand_id' => ''];

        return $this->resolved[$externalId];
    }

    // =========================================================================
    //  CREATION
    // =========================================================================

    /**
     * Create a candidate, CandidatesPlus first, legacy second.
     *
     * @param string      $pscid      PSCID to assign.
     * @param string      $externalId ExtStudyID to register.
     * @param string|null $dob        Jittered to YYYY-MM-01 before sending.
     * @param string|null $sex        Male / Female / Other.
     *
     * @return array{pscid: string, cand_id: string}
     */
    public function createCandidate(
        string $pscid,
        string $externalId,
        ?string $dob,
        ?string $sex
    ): array {
        $dobValue = $this->normaliseDate($dob ?? '');
        $sexValue = $this->normaliseSex($sex) ?? '';

        // --- Strategy 1: CandidatesPlus with ProjectExternalName -------------
        // Only ProjectExternalName is sent. The endpoint reads nothing else for
        // the external-project link:
        //
        //     if (!empty($data['Candidate']['ExtStudyID'])
        //         && !empty($data['Candidate']['ProjectExternalName'])) { ... }
        //
        // Send a ProjectExternalID instead and that block is skipped, the
        // candidate is created, and 201 comes back with NO extid relation - a
        // silent orphan the mapper can never resolve. So there is no
        // ProjectExternalID variant here by design; the numeric ID belongs only
        // to the legacy formHandler path below.
        if ($this->candidatesPlusAvailable
            && $this->projectExternalName !== null
            && $this->projectExternalName !== ''
        ) {
            $body = ['Candidate' => [
                'PSCID'               => $pscid,
                'Project'             => $this->projectName ?? '',
                'Site'                => $this->site ?? '',
                'DoB'                 => $dobValue,
                'Sex'                 => $sexValue,
                'ExtStudyID'          => $externalId,
                'ProjectExternalName' => $this->projectExternalName,
            ]];

            $response = $this->raw(
                'POST',
                $this->baseUrl . '/cbigr_api/candidatesPlus',
                $body
            );

            if ($response['status'] === 404 || $response['status'] === 0) {
                // Endpoint absent (server predates CandidatesPlus entirely), or
                // transport failure. Fall through to legacy, once per run.
                $this->candidatesPlusAvailable = false;
                $this->warn(
                    'CandidatesPlus unavailable on this server - using the '
                    . 'legacy candidate-creation path for the rest of this run'
                );
            } elseif ($response['status'] === 200 || $response['status'] === 201) {
                $candId = $response['body']['CandID']
                    ?? $response['body']['Meta']['CandID']
                    ?? null;

                if ($candId === null) {
                    throw new RuntimeException(
                        "CandidatesPlus returned no CandID for PSCID={$pscid}: "
                        . substr($response['raw'], 0, 300)
                    );
                }

                $result = ['pscid' => $pscid, 'cand_id' => (string) $candId];
                $this->resolved[$externalId] = $result;

                $this->info(
                    "Created via CandidatesPlus: CandID={$candId} PSCID={$pscid} "
                    . "ExtStudyID={$externalId} (relation linked atomically)"
                );

                return $result;
            } elseif ($response['status'] === 400
                && stripos($response['raw'], 'Unknown external project name') !== false
            ) {
                // The server knows CandidatesPlus but not this external project
                // name. It says outright that nothing was created, so falling
                // through to legacy is safe. Two causes: the name genuinely is
                // not in Project_external, or this build predates the
                // accept-Project-name PR.
                $this->warn(sprintf(
                    "CandidatesPlus rejected ProjectExternalName '%s' as unknown "
                    . '(nothing was created). Either the name is not in '
                    . 'Project_external, or this server predates the '
                    . 'accept-Project-name PR. Falling back to the legacy path.',
                    $this->projectExternalName
                ));
                $this->candidatesPlusAvailable = false;
            } else {
                // Duplicate PSCID, bad site, validation error. The legacy path
                // hits the same wall, so surface it once instead of twice.
                throw new RuntimeException(sprintf(
                    'CandidatesPlus HTTP %d for PSCID=%s ExtStudyID=%s: %s',
                    $response['status'],
                    $pscid,
                    $externalId,
                    substr($response['raw'], 0, 300)
                ));
            }
        }

        // --- Strategy 2: legacy two-step -------------------------------------
        // The linking value is checked BEFORE creating anything. The legacy
        // path cannot link without a numeric ProjectExternalID, and a candidate
        // created without its extid relation is an orphan: the mapper resolves
        // by ExtStudyID, so it will never be found again, and the next run
        // retries the same PSCID and collects a 409. Failing first leaves
        // nothing behind to clean up.
        if ($this->projectExternalId === null || $this->projectExternalId === '') {
            throw new RuntimeException(sprintf(
                "Cannot create candidate for ExtStudyID '%s': CandidatesPlus is "
                . 'unavailable and no ProjectExternalID is configured, so the '
                . 'legacy path could not link it. Nothing was created. Add '
                . 'candidate_defaults.project_external_id (or project_mappings) '
                . 'to project.json%s.',
                $externalId,
                $this->projectExternalName !== null
                    ? ", or deploy the CandidatesPlus accept-Project-name change so "
                      . "'{$this->projectExternalName}' can be used instead"
                    : ''
            ));
        }

        $candId = $this->createCandidateLegacy($pscid, $sexValue, $dobValue);

        $this->appendExternalId($candId, $externalId, $this->projectExternalId);

        $result = ['pscid' => $pscid, 'cand_id' => $candId];
        $this->resolved[$externalId] = $result;

        return $result;
    }

    /**
     * POST /api/{version}/candidates. Legacy path only.
     */
    private function createCandidateLegacy(
        string $pscid,
        string $sex,
        string $dob
    ): string {
        if ($this->projectName === null) {
            throw new RuntimeException(
                'candidate_defaults.project must be set to create candidates'
            );
        }

        $body = ['Candidate' => [
            'PSCID'   => $pscid,
            'Project' => $this->projectName,
            'Site'    => $this->site ?? '',
            'DoB'     => $dob,
            'Sex'     => $sex,
        ]];

        $response = $this->raw(
            'POST',
            $this->baseUrl . '/api/' . $this->version . '/candidates',
            $body
        );

        if ($response['status'] === 409) {
            $this->warn("PSCID={$pscid} already exists (409) - looking up CandID");
            $existing = $this->lookupCandidateByPscid($pscid);

            if ($existing !== null) {
                return $existing;
            }
        }

        if ($response['status'] !== 200 && $response['status'] !== 201) {
            throw new RuntimeException(sprintf(
                'Candidate creation HTTP %d for PSCID=%s: %s',
                $response['status'],
                $pscid,
                substr($response['raw'], 0, 300)
            ));
        }

        $candId = $response['body']['Meta']['CandID']
            ?? $response['body']['CandID']
            ?? null;

        if ($candId === null) {
            throw new RuntimeException(
                "No CandID in candidate-creation response for PSCID={$pscid}"
            );
        }

        $this->info("Created via legacy: CandID={$candId} PSCID={$pscid}");

        return (string) $candId;
    }

    /**
     * Link an ExtStudyID via the candidate_parameters form handler.
     *
     * Multipart, not JSON - this is a form endpoint, not the REST API.
     */
    private function appendExternalId(
        string $candId,
        string $extStudyId,
        string $projectExternalId
    ): void {
        $fields = [
            'tab'        => 'externalIdentifier',
            'candID'     => $candId,
            'ProjectID'  => $projectExternalId,
            'ExtStudyID' => $extStudyId,
        ];

        $response = $this->raw(
            'POST',
            $this->baseUrl . '/candidate_parameters/ajax/formHandler.php',
            $fields,
            true
        );

        if ($response['status'] < 200 || $response['status'] >= 300) {
            throw new RuntimeException(sprintf(
                'HTTP %d linking ExtStudyID=%s to CandID=%s: %s',
                $response['status'],
                $extStudyId,
                $candId,
                substr($response['raw'], 0, 300)
            ));
        }

        $this->info("ExternalID linked: {$extStudyId} -> CandID {$candId}");
    }

    private function lookupCandidateByPscid(string $pscid): ?string
    {
        $response = $this->raw(
            'GET',
            $this->baseUrl . '/api/' . $this->version . '/candidates'
        );

        foreach ($response['body']['Candidates'] ?? [] as $candidate) {
            if (($candidate['PSCID'] ?? '') === $pscid) {
                return (string) ($candidate['CandID'] ?? '');
            }
        }

        return null;
    }

    // =========================================================================
    //  VISITS AND SESSIONS
    // =========================================================================

    /**
     * Visit labels configured for the project.
     *
     * @return string[]
     */
    public function getConfiguredVisitLabels(): array
    {
        if ($this->visitLabels !== null) {
            return $this->visitLabels;
        }

        if ($this->projectName === null) {
            throw new RuntimeException(
                'candidate_defaults.project must be set to resolve visit labels'
            );
        }

        $response = $this->raw(
            'GET',
            $this->baseUrl . '/api/' . $this->version . '/projects/'
            . rawurlencode($this->projectName)
        );

        $labels = [];
        $visits = $response['body']['Visits'] ?? [];

        if (is_array($visits)) {
            foreach ($visits as $key => $value) {
                if (is_string($key) && !is_int($key)) {
                    $labels[] = $key;
                } elseif (is_string($value)) {
                    $labels[] = $value;
                } elseif (is_array($value) && isset($value['VisitLabel'])) {
                    $labels[] = (string) $value['VisitLabel'];
                }
            }
        }

        $this->visitLabels = array_values(array_unique(array_filter($labels)));

        if (empty($this->visitLabels)) {
            $this->warn(
                "No visit labels returned for '{$this->projectName}' - every "
                . 'study will be treated as visit_not_configured'
            );
        }

        return $this->visitLabels;
    }

    /**
     * Ensure a session exists. Idempotent.
     */
    public function ensureSession(string $candId, string $visitLabel): void
    {
        $key = $candId . '|' . $visitLabel;

        if (isset($this->sessionsEnsured[$key]) || $candId === '') {
            return;
        }

        $url = $this->baseUrl . '/api/' . $this->version . '/candidates/'
            . rawurlencode($candId) . '/' . rawurlencode($visitLabel);

        $existing = $this->raw('GET', $url);

        if ($existing['status'] >= 200 && $existing['status'] < 300) {
            $this->sessionsEnsured[$key] = true;
            return;
        }

        $meta = array_filter(
            [
                'CandID'  => $candId,
                'Visit'   => $visitLabel,
                'Site'    => $this->site,
                'Project' => $this->projectName,
            ],
            static fn ($v): bool => $v !== null && $v !== ''
        );

        $response = $this->raw('PUT', $url, ['Meta' => $meta]);

        if ($response['status'] < 200 || $response['status'] >= 300) {
            throw new RuntimeException(sprintf(
                'HTTP %d creating session %s/%s: %s',
                $response['status'],
                $candId,
                $visitLabel,
                substr($response['raw'], 0, 300)
            ));
        }

        $this->sessionsEnsured[$key] = true;

        $this->info("Ensured session {$candId}/{$visitLabel}");
    }

    // =========================================================================
    //  HTTP
    // =========================================================================

    private function login(): string
    {
        if ($this->token !== null) {
            return $this->token;
        }

        $loris = $this->config['loris'] ?? $this->config['api'] ?? [];

        foreach (['username', 'password'] as $key) {
            if (empty($loris[$key])) {
                throw new RuntimeException("loris.{$key} is not configured");
            }
        }

        $response = $this->raw(
            'POST',
            $this->baseUrl . '/api/' . $this->version . '/login',
            ['username' => $loris['username'], 'password' => $loris['password']],
            false,
            false
        );

        $token = $response['body']['token'] ?? null;

        if (!is_string($token) || $token === '') {
            throw new RuntimeException(
                'LORIS login did not return a token (HTTP ' . $response['status'] . ')'
            );
        }

        $this->token = $token;

        return $this->token;
    }

    /**
     * One HTTP request. Never throws on status - callers decide, because a 404
     * from CandidatesPlus is a fallback signal, not an error.
     *
     * @return array{status: int, body: mixed, raw: string}
     */
    private function raw(
        string $method,
        string $url,
        $payload = null,
        bool $multipart = false,
        bool $authenticated = true
    ): array {
        $headers = ['Accept: application/json'];

        if ($authenticated) {
            $headers[] = 'Authorization: Bearer ' . $this->login();
        }

        $handle = curl_init();

        $options = [
            CURLOPT_URL            => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_TIMEOUT        => (int) ($this->config['timeout'] ?? 1800),
            CURLOPT_CONNECTTIMEOUT => 10,
            CURLOPT_FOLLOWLOCATION => false,
        ];

        if ($payload !== null) {
            if ($multipart) {
                $options[CURLOPT_POSTFIELDS] = $payload;
            } else {
                $encoded = json_encode($payload);

                if ($encoded === false) {
                    throw new RuntimeException(
                        'Could not encode payload: ' . json_last_error_msg()
                    );
                }

                $options[CURLOPT_POSTFIELDS] = $encoded;
                $headers[]                   = 'Content-Type: application/json';
            }
        }

        $options[CURLOPT_HTTPHEADER] = $headers;

        curl_setopt_array($handle, $options);

        $raw    = curl_exec($handle);
        $status = (int) curl_getinfo($handle, CURLINFO_HTTP_CODE);
        $error  = curl_error($handle);

        curl_close($handle);

        if ($raw === false) {
            // status 0 signals a transport failure, which callers treat the
            // same as 404 for fallback purposes.
            $this->warn("{$method} {$url} failed: {$error}");
            return ['status' => 0, 'body' => null, 'raw' => $error];
        }

        $decoded = json_decode((string) $raw, true);

        return [
            'status' => $status,
            'body'   => json_last_error() === JSON_ERROR_NONE ? $decoded : null,
            'raw'    => (string) $raw,
        ];
    }

    // =========================================================================
    //  NORMALISATION
    // =========================================================================

    /**
     * Jitter a date to YYYY-MM-01, per ARCHIMEDES privacy policy.
     *
     * Missing day -> 01, missing month -> 01, year-only -> YYYY-01-01. Empty
     * stays empty. Unparseable values pass through unchanged so LORIS surfaces
     * the validation error rather than the pipeline silently mangling it.
     */
    private function normaliseDate(string $value): string
    {
        $value = trim($value);

        if ($value === '') {
            return '';
        }

        if (preg_match('/^(\d{4})-(\d{2})-\d{2}$/', $value, $m)) {
            return $m[1] . '-' . $m[2] . '-01';
        }

        if (preg_match('/^(\d{4})-(\d{2})$/', $value, $m)) {
            return $m[1] . '-' . $m[2] . '-01';
        }

        if (preg_match('/^(\d{4})$/', $value, $m)) {
            return $m[1] . '-01-01';
        }

        $timestamp = strtotime($value);

        if ($timestamp !== false) {
            return date('Y-m', $timestamp) . '-01';
        }

        return $value;
    }

    private function normaliseSex(?string $sex): ?string
    {
        if ($sex === null || trim($sex) === '') {
            return null;
        }

        return match (strtolower(trim($sex))) {
            'm', 'male'                                                 => 'Male',
            'f', 'female'                                               => 'Female',
            'o', 'other', 'nb', 'non-binary', 'nonbinary',
            'unknown', 'u', 'n/a'                                       => 'Other',
            default                                                     => null,
        };
    }

    private function info(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->info($message);
        }
    }

    private function warn(string $message): void
    {
        if ($this->logger !== null) {
            $this->logger->warning($message);
        }
    }
}
