<?php

declare(strict_types=1);

namespace LORIS\Pipelines;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use LORIS\Utils\{Notification, CleanLogFormatter};

/**
 * Participant Metadata Pipeline
 *
 * Scans BIDS, clinical, and custom-configured sources for participant
 * metadata updates and applies them to LORIS via the
 * cbigr_api/candidatesPlus PUT endpoint.
 *
 * Default scan folders (relative to each project directory):
 *   - deidentified-raw/bids/      (scans participants.tsv if present)
 *   - deidentified-raw/clinical/  (scans every *.csv found)
 *
 * Additional custom sources go in participant_metadata.sources. Type is
 * auto-detected from file extension (.csv, .tsv, .json).
 *
 * --- Per-source 'fields' config ---
 * Maps a LORIS field name to a list of possible source column names.
 * The first non-empty source column for each field is used. Example:
 *
 *   "fields": {
 *     "DoD": ["DoD", "DateOfDeath", "date_of_death"],
 *     "Sex": ["Sex", "sex", "gender"],          // future
 *     "DoB": ["DoB", "DateOfBirth", "dob"]      // future
 *   }
 *
 * Today the cbigr_api/candidatesPlus PUT endpoint validates and accepts
 * only the DoD field. To add support for additional fields in future:
 *   1. Extend PUT validation in CandidatesPlus.php endpoint.
 *   2. Ensure GET /candidatesPlus response includes the new field so
 *      idempotency check can compare against current LORIS state.
 *   3. Add the field as a key under 'fields' in each project.json.
 *
 * No pipeline code change required. A warning is logged if a configured
 * field is not present in the GET response.
 *
 * --- Backward compat ---
 * Legacy 'dod_fields' is accepted as alias for 'fields: {DoD: [...]}'.
 *
 * --- Duplicate / conflict handling ---
 * Same candidate+field+value across sources -> silent skip (duplicate).
 * Same candidate+field, different values -> first wins, logged as
 * conflict (not flagged as error).
 *
 * @package LORIS\Pipelines
 */
class ParticipantMetadataPipeline
{
    const DEFAULT_BIDS_DIR           = 'deidentified-raw/bids';
    const DEFAULT_CLINICAL_DIR       = 'deidentified-raw/clinical';
    const BIDS_PARTICIPANTS_FILENAME = 'participants.tsv';

    private Logger  $logger;
    private array   $config;
    private bool    $dryRun;
    private bool    $verbose;
    private ?string $token = null;

    private string  $runTimestamp;
    private ?string $logDir     = null;
    private ?string $runLogPath = null;

    /** @var resource|null */
    private $runLogFh = null;

    private array $stats              = [];
    private array $processedThisRun   = [];
    private array $grandTotal         = [];
    private bool  $anyFailed          = false;
    private Notification $notification;
    private bool $notificationSent    = false;

    // Hash-based change detection (matches ClinicalPipeline pattern).
    private bool $force = false;
    private array $trackingData = [];
    private ?string $trackingPath = null;

    public function __construct(array $config, bool $dryRun = false, bool $verbose = false, bool $force = false)
    {
        // Same auth-config compatibility shim as BidsParticipantSync
        if (!isset($config['loris']) && isset($config['api'])) {
            $config['loris'] = $config['api'];
        }
        if (empty($config['loris']['base_url'])) {
            throw new \RuntimeException("Missing required config: loris.base_url");
        }
        if (empty($config['loris']['username']) || empty($config['loris']['password'])) {
            throw new \RuntimeException("Missing required config: loris.username / loris.password");
        }

        $this->config       = $config;
        $this->dryRun       = $dryRun;
        $this->verbose      = $verbose;
        $this->force        = $force;
        $this->runTimestamp = date('Y-m-d_H-i-s');
        $this->logger       = $this->_initLogger();
        $this->notification = new Notification();
    }

    // =========================================================================
    //  PUBLIC ENTRY POINT
    // =========================================================================

    /**
     * Entry point. Walks the config's collections/projects, optionally
     * filtered, and invokes _processProject() for each match.
     *
     * @param array $filters ['collection' => string, 'project' => string]
     *
     * @return int Exit code: 0 if no failures, 1 if any unresolved or PUT failures
     */
    public function run(array $filters = []): int
    {
        $this->grandTotal = $this->_emptyStats();
        $this->anyFailed  = false;

        $collections = $this->config['collections'] ?? [];
        $wantColl    = $filters['collection'] ?? null;
        $wantProject = $filters['project'] ?? null;

        $projectsProcessed = 0;

        if ($this->dryRun) {
            echo "\n";
            echo "╔══════════════════════════════════════╗\n";
            echo "║         DRY RUN MODE                 ║\n";
            echo "║  No changes will be made.            ║\n";
            echo "╚══════════════════════════════════════╝\n";
            echo "\n";
        }
        if ($this->force) {
            echo "Mode: FORCE (hash check bypassed - all sources will be reprocessed)\n\n";
        }

        // Authenticate once for the whole run (live mode only).
        if (!$this->dryRun) {
            if (!$this->_authenticate()) {
                fwrite(STDERR, "FATAL: authentication failed; aborting\n");
                return 1;
            }
        }

        foreach ($collections as $coll) {
            $collName = $coll['name'] ?? '';
            $basePath = rtrim($coll['base_path'] ?? '', '/');
            $enabled  = $coll['enabled'] ?? true;
            $projects = $coll['projects'] ?? [];

            if ($wantColl !== null && $wantColl !== $collName) {
                continue;
            }
            if (!$enabled && $wantColl !== $collName) {
                continue;
            }
            if (empty($basePath) || empty($projects)) {
                continue;
            }

            foreach ($projects as $proj) {
                $projName    = $proj['name'] ?? '';
                $projEnabled = $proj['enabled'] ?? true;

                if ($wantProject !== null && $wantProject !== $projName) {
                    continue;
                }
                if (!$projEnabled && $wantProject !== $projName) {
                    continue;
                }

                $projectPath = "{$basePath}/{$projName}";

                if (!is_dir($projectPath)) {
                    fwrite(STDERR, "  Warning: project dir not found: {$projectPath} - skipping\n");
                    continue;
                }

                echo "────────────────────────────────────────\n";
                echo "Collection: {$collName}  Project: {$projName}\n";
                echo "────────────────────────────────────────\n";

                $stats = $this->_processProject($projectPath, $collName, $projName);
                $this->_accumulate($stats);
                $projectsProcessed++;
            }
        }

        if ($projectsProcessed === 0) {
            fwrite(STDERR, "No projects matched the given filters.\n");
            return 1;
        }

        if ($projectsProcessed > 1) {
            $this->_printGrandTotal($projectsProcessed);
        }

        return $this->anyFailed ? 1 : 0;
    }

    // =========================================================================
    //  PER-PROJECT PROCESSING
    // =========================================================================

    private function _processProject(string $projectDir, string $collection, string $project): array
    {
        $this->_resetStats();
        $this->notificationSent = false;

        $projectJson = $projectDir . '/project.json';
        if (!is_file($projectJson)) {
            fwrite(STDERR, "  Warning: project.json not found at $projectJson - skipping\n");
            return $this->stats;
        }

        $projectConfig = json_decode(file_get_contents($projectJson), true);
        if ($projectConfig === null) {
            fwrite(STDERR, "  Warning: invalid project.json at $projectJson - skipping\n");
            return $this->stats;
        }

        $metaConfig = $projectConfig['participant_metadata'] ?? null;
        if ($metaConfig === null || empty($metaConfig['enabled'])) {
            if ($this->verbose) {
                echo "  participant_metadata not enabled for $project - skipping\n";
            }
            return $this->stats;
        }

        $this->_openLog($projectDir);
        $this->_log("=== ParticipantMetadataPipeline run for $collection / $project ===");
        $this->_log(
            "Dry run: " . ($this->dryRun ? "yes" : "no")
            . ", Verbose: " . ($this->verbose ? "yes" : "no")
            . ", Force: " . ($this->force ? "yes" : "no")
        );

        // Load hash-tracking for this project's sources (matches Clinical pattern).
        $this->_loadTrackingFile($projectDir);

        $candidateIndex = $this->_buildCandidateIndex($projectConfig, $project);
        if ($candidateIndex === null) {
            $this->_log("Could not build candidate index for $project - skipping");
            $this->_closeLog();
            $this->anyFailed = true;
            return $this->stats;
        }

        $sources = $this->_buildEffectiveSources($metaConfig, $projectDir);
        $this->_log("Effective sources: " . count($sources));

        foreach ($sources as $source) {
            $this->_processSource($source, $projectDir, $candidateIndex, $project);
        }

        $this->_logSummary($project);
        $this->_saveTrackingFile();
        $this->_sendNotification($projectConfig, $collection, $project);
        $this->_log("=== Done ===");
        $this->_closeLog();

        if (!empty($this->stats['candidates_unresolved'])
            || !empty($this->stats['updates_failed'])
        ) {
            $this->anyFailed = true;
        }

        return $this->stats;
    }

    // =========================================================================
    //  HTTP / AUTH (mirrors BidsParticipantSync pattern)
    // =========================================================================

    private function _http(): \GuzzleHttp\Client
    {
        return new \GuzzleHttp\Client(['verify' => false, 'timeout' => 60]);
    }

    private function _authenticate(): bool
    {
        $baseUrl  = rtrim($this->config['loris']['base_url'], '/');
        $version  = $this->config['loris']['api_version'] ?? 'v0.0.4-dev';
        $username = $this->config['loris']['username'];
        $password = $this->config['loris']['password'];

        echo "Authenticating with LORIS at {$baseUrl}\n";

        try {
            $response = $this->_http()->request(
                'POST',
                "{$baseUrl}/api/{$version}/login",
                ['json' => compact('username', 'password')]
            );
            $data        = json_decode((string) $response->getBody(), true);
            $this->token = $data['token'] ?? null;

            if (!$this->token) {
                fwrite(STDERR, "AUTH: No token in login response - check credentials\n");
                return false;
            }
            echo "  Authenticated\n";
            return true;
        } catch (\Exception $e) {
            fwrite(STDERR, "AUTH: Authentication failed: " . $e->getMessage() . "\n");
            return false;
        }
    }

    // =========================================================================
    //  CANDIDATE INDEX (GET /cbigr_api/candidatesPlus)
    // =========================================================================

    private function _buildCandidateIndex(array $projectConfig, string $folderProjectName): ?array
    {
        // Resolve the LORIS project name (what GET /candidatesPlus returns
        // in the 'Project' field). Falls back to the folder name.
        $lorisProjectName = $projectConfig['candidate_defaults']['project']
            ?? $projectConfig['loris_project_name']
            ?? $projectConfig['project_common_name']
            ?? $folderProjectName;

        $this->_log("Filtering candidates by LORIS project name: '$lorisProjectName'");

        $index = ['by_pscid' => [], 'by_extid' => []];

        if ($this->dryRun) {
            // In dry-run, attempt to fetch but tolerate failures silently
            // so users can preview without valid creds.
            if (!$this->token && !$this->_authenticate()) {
                $this->_log("Dry run: could not authenticate; candidates will be unresolved");
                return $index;
            }
        }

        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url     = "{$baseUrl}/cbigr_api/candidatesPlus";

        try {
            $response = $this->_http()->request('GET', $url, [
                'headers'     => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'http_errors' => false,
            ]);
            $statusCode = $response->getStatusCode();
            $rawBody    = (string) $response->getBody();

            if ($statusCode !== 200) {
                $this->_log(
                    "  !! GET /cbigr_api/candidatesPlus returned HTTP $statusCode. "
                    . "Body: " . substr($rawBody, 0, 500)
                );
                return null;
            }

            $data       = json_decode($rawBody, true);
            $candidates = $data['Candidates'] ?? [];
        } catch (\Exception $e) {
            $this->_log("  !! Exception calling GET /cbigr_api/candidatesPlus: " . $e->getMessage());
            return null;
        }

        $totalCandidates    = count($candidates);
        $matchingCandidates = 0;
        $seenProjectNames   = [];

        foreach ($candidates as $cand) {
            // The provisioner SQL aliases p.Name as ProjectName, but
            // CandidatesPlusRow's toArray() exposes it as 'Project'.
            // Be permissive: accept either key.
            $candProject = $cand['Project'] ?? $cand['ProjectName'] ?? null;

            if ($candProject !== null) {
                $seenProjectNames[$candProject] = true;
            }
            if ($candProject !== $lorisProjectName) {
                continue;
            }
            $matchingCandidates++;

            $row = [
                'CandID'  => $cand['CandID'],
                'PSCID'   => $cand['PSCID'],
                'Project' => $candProject,
                'current' => $cand,
            ];
            $index['by_pscid'][$cand['PSCID']] = $row;

            // ExtStudyIDs comes back as an associative object:
            //   {"FDG PET": "MRAC-NORM-002", "OtherProject": "..."}
            // Older shapes (JSON-encoded string) are also tolerated.
            $extStudyIDs = $cand['ExtStudyIDs'] ?? null;
            if (is_string($extStudyIDs)) {
                $decoded = json_decode($extStudyIDs, true);
                $extStudyIDs = is_array($decoded) ? $decoded : [];
            }
            if (!is_array($extStudyIDs)) {
                $extStudyIDs = [];
            }
            // Keys are project external names; values are the ExtStudyIDs we
            // want to index on for incoming source rows to resolve against.
            foreach ($extStudyIDs as $extID) {
                if (!empty($extID) && $extID !== 'null') {
                    $index['by_extid'][$extID] = $row;
                }
            }
        }

        $this->_log(
            "GET returned $totalCandidates candidates total; "
            . "$matchingCandidates match project '$lorisProjectName'"
        );

        if ($matchingCandidates === 0 && $totalCandidates > 0) {
            $available = implode(', ', array_keys($seenProjectNames));
            $this->_log(
                "  !! No candidates matched. Available project names in GET response: "
                . "[$available]. Check that candidate_defaults.project in project.json "
                . "matches a LORIS project name exactly (case-sensitive)."
            );
        }

        $this->_log(
            "Indexed " . count($index['by_pscid']) . " candidates by PSCID, "
            . count($index['by_extid']) . " by ExtStudyID"
        );
        return $index;
    }

    // =========================================================================
    //  SOURCE LIST / TYPE DETECTION
    // =========================================================================

    private function _buildEffectiveSources(array $metaConfig, string $projectDir): array
    {
        $effective = [];
        $defaults  = $metaConfig['defaults'] ?? [];

        // Default: BIDS folder
        $bidsDefault = $defaults['bids'] ?? null;
        if ($bidsDefault !== false && is_array($bidsDefault)) {
            $tsvPath = self::DEFAULT_BIDS_DIR . '/' . self::BIDS_PARTICIPANTS_FILENAME;
            if (is_file($projectDir . '/' . $tsvPath)) {
                $effective[] = array_merge($bidsDefault, [
                    'type' => 'TSV',
                    'path' => $tsvPath,
                ]);
            }
        }

        // Default: clinical folder (one entry per *.csv found)
        $clinicalDefault = $defaults['clinical'] ?? null;
        if ($clinicalDefault !== false && is_array($clinicalDefault)) {
            $clinicalDir = $projectDir . '/' . self::DEFAULT_CLINICAL_DIR;
            if (is_dir($clinicalDir)) {
                $csvFiles = glob($clinicalDir . '/*.csv') ?: [];
                sort($csvFiles);
                foreach ($csvFiles as $csvFile) {
                    $relative = self::DEFAULT_CLINICAL_DIR . '/' . basename($csvFile);
                    $effective[] = array_merge($clinicalDefault, [
                        'type' => 'CSV',
                        'path' => $relative,
                    ]);
                }
            }
        }

        // Custom additional sources
        foreach ($metaConfig['sources'] ?? [] as $custom) {
            $effective[] = $custom;
        }

        return $effective;
    }

    /**
     * Normalizes legacy 'dod_fields' to canonical 'fields' shape.
     */
    private function _resolveFieldsConfig(array $source): array
    {
        if (!empty($source['fields']) && is_array($source['fields'])) {
            return $source['fields'];
        }
        if (!empty($source['dod_fields']) && is_array($source['dod_fields'])) {
            return ['DoD' => $source['dod_fields']];
        }
        return [];
    }

    private function _detectType(string $path): ?string
    {
        $ext = strtolower(pathinfo($path, PATHINFO_EXTENSION));
        return match ($ext) {
            'csv'  => 'CSV',
            'tsv'  => 'TSV',
            'json' => 'JSON',
            default => null,
        };
    }

    // =========================================================================
    //  SOURCE READING
    // =========================================================================

    private function _processSource(array $source, string $projectDir, array $candidateIndex, string $projectName): void
    {
        $path = $projectDir . '/' . $source['path'];
        if (!is_file($path)) {
            $this->_logVerbose("Source not found, skipping: $path");
            return;
        }

        // Hash-based change detection (matches Clinical pipeline pattern).
        // Skip processing if the source file hasn't changed since last run.
        // Use --force to bypass. The check is by basename, matching Clinical's
        // tracking-file schema.
        $basename     = basename($path);
        $currentHash  = md5_file($path);
        $previousHash = $this->trackingData[$basename]['hash'] ?? null;

        if (!$this->force && $previousHash !== null && $currentHash === $previousHash) {
            $tracked = $this->trackingData[$basename];
            $lastRun = $tracked['last_uploaded_at'] ?? 'unknown';
            $lastFieldsUpdated = $tracked['fields_updated'] ?? 0;

            $this->_log(
                "  [{$basename}] SKIPPED - no changes since last run (hash match)"
                . " (last run: {$lastRun}, {$lastFieldsUpdated} field(s) updated)"
            );
            $this->stats['sources_skipped_unchanged']++;
            return;
        }

        $type = $source['type'] ?? $this->_detectType($path);
        if ($type === null) {
            $this->_log(
                "  !! Could not determine source type from path "
                . "'{$source['path']}'. Specify 'type' explicitly (CSV, TSV, or JSON)."
            );
            return;
        }

        $this->stats['sources_scanned']++;
        $this->_log("Scanning $type: {$source['path']}");

        // Capture pre-source stats so we can compute per-source deltas for tracking.
        $preStats = [
            'rows_with_data'        => $this->stats['rows_with_data'],
            'rows_without_data'     => $this->stats['rows_without_data'],
            'candidates_resolved'   => $this->stats['candidates_resolved'],
            'candidates_unresolved' => count($this->stats['candidates_unresolved']),
            'fields_updated'        => $this->stats['fields_updated'],
            'updates_failed'        => count($this->stats['updates_failed']),
        ];

        $rows = $this->_readSource($type, $path);
        $this->_logVerbose("  Read " . count($rows) . " rows from source");

        foreach ($rows as $row) {
            $this->stats['rows_scanned']++;
            $this->_processRow($row, $source, $candidateIndex, $projectName);
        }

        // Update tracking for this source if the run was clean for it.
        // Mirrors Clinical's behavior: only mark hash as processed when
        // there were no failures attributable to this source. A run with
        // unresolved IDs or failed PUTs leaves the entry unchanged so the
        // next run retries.
        $thisSourceUnresolved = count($this->stats['candidates_unresolved'])
            - $preStats['candidates_unresolved'];
        $thisSourceFailed     = count($this->stats['updates_failed'])
            - $preStats['updates_failed'];

        if (!$this->dryRun && $thisSourceUnresolved === 0 && $thisSourceFailed === 0) {
            $this->_recordTracking($basename, $currentHash, $preStats, count($rows));
        } elseif ($this->dryRun) {
            $this->_logVerbose("  [{$basename}] tracking not updated (dry run)");
        } else {
            $this->_logVerbose(
                "  [{$basename}] tracking not updated"
                . " ({$thisSourceUnresolved} unresolved, {$thisSourceFailed} failed"
                . " from this source — will retry next run)"
            );
        }
    }

    private function _readSource(string $type, string $path): array
    {
        switch ($type) {
            case 'TSV':
                return $this->_readDelimited($path, "\t");
            case 'CSV':
                return $this->_readDelimited($path, ',');
            case 'JSON':
                $decoded = json_decode(file_get_contents($path), true);
                if (!is_array($decoded)) {
                    return [];
                }
                $isList = array_keys($decoded) === range(0, count($decoded) - 1);
                if ($isList) {
                    return $decoded;
                }
                $rows = [];
                foreach ($decoded as $key => $entry) {
                    if (is_array($entry)) {
                        $entry['__key'] = $key;
                        $rows[] = $entry;
                    }
                }
                return $rows;
            default:
                $this->_log("Unknown source type: $type (expected CSV, TSV, or JSON)");
                return [];
        }
    }

    private function _readDelimited(string $path, string $delim): array
    {
        $fh = fopen($path, 'r');
        if ($fh === false) {
            return [];
        }
        $header = fgetcsv($fh, 0, $delim);
        if ($header === false) {
            fclose($fh);
            return [];
        }
        $rows = [];
        while (($cols = fgetcsv($fh, 0, $delim)) !== false) {
            if (count($cols) !== count($header)) {
                continue;
            }
            $rows[] = array_combine($header, $cols);
        }
        fclose($fh);
        return $rows;
    }

    // =========================================================================
    //  PER-ROW PROCESSING
    // =========================================================================

    private function _processRow(array $row, array $source, array $candidateIndex, string $projectName): void
    {
        $idField   = $source['identifier_field'];
        $idType    = $source['identifier_type'];
        $stripPref = $source['identifier_strip_prefix'] ?? '';
        $fieldsCfg = $this->_resolveFieldsConfig($source);

        if (empty($fieldsCfg)) {
            $this->_logVerbose(
                "  Source {$source['path']} has no 'fields' (or 'dod_fields') "
                . "configured, skipping row"
            );
            return;
        }

        // Resolve identifier
        $rawIdentifier = $row[$idField] ?? $row['__key'] ?? null;
        if (empty($rawIdentifier)) {
            $this->_logVerbose("  Row has no identifier in field '$idField', skipping");
            return;
        }
        $identifier = $stripPref !== ''
            ? preg_replace('/^' . preg_quote($stripPref, '/') . '/', '', $rawIdentifier)
            : $rawIdentifier;

        // Extract values for each configured field. First non-empty source column wins per field.
        $extracted = [];
        foreach ($fieldsCfg as $lorisField => $sourceColumns) {
            foreach ($sourceColumns as $col) {
                if (!empty($row[$col])) {
                    $extracted[$lorisField] = [
                        'value'  => trim((string) $row[$col]),
                        'column' => $col,
                    ];
                    break;
                }
            }
        }

        if (empty($extracted)) {
            $this->stats['rows_without_data']++;
            $this->_logVerbose(
                "  $idType=$identifier: no configured fields had values, skipping"
            );
            return;
        }
        $this->stats['rows_with_data']++;

        // Resolve to candidate
        $candidate = $idType === 'PSCID'
            ? ($candidateIndex['by_pscid'][$identifier] ?? null)
            : ($candidateIndex['by_extid'][$identifier] ?? null);

        if ($candidate === null) {
            $fieldList     = implode(',', array_keys($extracted));
            $unresolvedMsg = "$idType=$identifier (fields=$fieldList, source {$source['path']})";
            $this->stats['candidates_unresolved'][] = $unresolvedMsg;
            $this->_log(
                "  !! UNRESOLVED: no matching candidate in project '$projectName' "
                . "for $idType=$identifier. Cannot update fields [$fieldList]."
            );
            return;
        }
        $this->stats['candidates_resolved']++;

        $candID  = $candidate['CandID'];
        $pscid   = $candidate['PSCID'];
        $current = $candidate['current'];

        $payloadFields   = [];
        $perFieldActions = [];

        if (!isset($this->processedThisRun[$candID])) {
            $this->processedThisRun[$candID] = [];
        }

        foreach ($extracted as $lorisField => $extr) {
            $newValue  = $extr['value'];
            $sourceCol = $extr['column'];

            // In-run duplicate / conflict check (per field)
            if (isset($this->processedThisRun[$candID][$lorisField])) {
                $prev = $this->processedThisRun[$candID][$lorisField];

                if ($prev['value'] === $newValue) {
                    $this->stats['duplicates_skipped']++;
                    $this->_logVerbose(
                        "  Duplicate: CandID=$candID PSCID=$pscid "
                        . "$lorisField=$newValue already from {$prev['source']}, "
                        . "skipping {$source['path']}"
                    );
                    continue;
                } else {
                    $conflictMsg = "CandID=$candID PSCID=$pscid: "
                        . "first applied $lorisField={$prev['value']} from {$prev['source']}, "
                        . "{$source['path']} provides $lorisField=$newValue (ignored)";
                    $this->stats['conflicts'][] = $conflictMsg;
                    $this->_log("  Conflict (first wins): $conflictMsg");
                    continue;
                }
            }

            // Idempotency: skip if LORIS already has this value.
            // If the field isn't present in the current candidate record,
            // (string) null === $newValue is false, so we'll PUT — which
            // is the safe default.
            $currentValue = $current[$lorisField] ?? null;
            if ((string) $currentValue === $newValue) {
                $this->stats['fields_skipped_idempotent']++;
                $this->_logVerbose(
                    "  Skip CandID=$candID PSCID=$pscid "
                    . "$lorisField=$newValue (already in LORIS)"
                );
                $this->processedThisRun[$candID][$lorisField] = [
                    'value'  => $newValue,
                    'source' => $source['path'],
                ];
                continue;
            }

            // Queue this field for update
            $payloadFields[$lorisField] = $newValue;
            $perFieldActions[] = [
                'field'         => $lorisField,
                'old'           => $currentValue ?? 'NULL',
                'new'           => $newValue,
                'source_column' => $sourceCol,
            ];
            $this->processedThisRun[$candID][$lorisField] = [
                'value'  => $newValue,
                'source' => $source['path'],
            ];
        }

        if (empty($payloadFields)) {
            return;
        }

        $this->stats['updates_attempted']++;
        $fieldSummary = [];
        foreach ($perFieldActions as $a) {
            $fieldSummary[] = "{$a['field']}: {$a['old']} -> {$a['new']}";
        }
        $this->_log(
            "  Update CandID=$candID PSCID=$pscid "
            . "[" . implode('; ', $fieldSummary) . "] "
            . "(from $idType=$identifier, source {$source['path']})"
            . ($this->dryRun ? " [DRY RUN]" : '')
        );

        if ($this->dryRun) {
            // Increment counters in dry-run too so the summary accurately
            // predicts what a live run would do.
            $this->stats['updates_succeeded']++;
            foreach (array_keys($payloadFields) as $field) {
                $this->stats['fields_updated_by_name'][$field] =
                    ($this->stats['fields_updated_by_name'][$field] ?? 0) + 1;
            }
            $this->stats['fields_updated'] += count($payloadFields);
            return;
        }

        // ── PUT /cbigr_api/candidatesPlus?CandID=X ───────────────────────────
        $baseUrl = rtrim($this->config['loris']['base_url'], '/');
        $url     = "{$baseUrl}/cbigr_api/candidatesPlus?CandID={$candID}";
        $payload = ['Candidate' => $payloadFields];

        try {
            $response = $this->_http()->request('PUT', $url, [
                'headers'     => [
                    'Authorization' => "Bearer {$this->token}",
                    'Content-Type'  => 'application/json',
                ],
                'json'        => $payload,
                'http_errors' => false,
            ]);
            $statusCode = $response->getStatusCode();
            $rawBody    = (string) $response->getBody();

            if ($statusCode >= 200 && $statusCode < 300) {
                $this->stats['updates_succeeded']++;
                $this->stats['fields_updated'] += count($payloadFields);
                foreach (array_keys($payloadFields) as $field) {
                    $this->stats['fields_updated_by_name'][$field] =
                        ($this->stats['fields_updated_by_name'][$field] ?? 0) + 1;
                }
            } else {
                $errMsg = "CandID=$candID PSCID=$pscid (fields="
                    . implode(',', array_keys($payloadFields))
                    . "): HTTP $statusCode. Body: " . substr($rawBody, 0, 500);
                $this->stats['updates_failed'][] = $errMsg;
                $this->_log("  !! FAILED: $errMsg");
            }
        } catch (\Exception $e) {
            $errMsg = "CandID=$candID PSCID=$pscid (fields="
                . implode(',', array_keys($payloadFields)) . "): "
                . $e->getMessage();
            $this->stats['updates_failed'][] = $errMsg;
            $this->_log("  !! FAILED: $errMsg");
        }
    }

    // =========================================================================
    //  STATS / ACCUMULATION
    // =========================================================================

    // =========================================================================
    //  HASH TRACKING (per-project, matches ClinicalPipeline pattern)
    // =========================================================================

    /**
     * Load the per-project tracking JSON. Schema matches Clinical's:
     *   { "filename.csv": { "hash": "...", "first_uploaded_at": "...",
     *                        "last_uploaded_at": "...", "run_timestamp": "...",
     *                        "upload_count": N, "rows_total": N,
     *                        "rows_with_data": N, "fields_updated": N } }
     *
     * Stored at: {projectDir}/processed/participant_metadata/.participant_metadata_tracking.json
     */
    private function _loadTrackingFile(string $projectDir): void
    {
        $dir = $projectDir . '/processed/participant_metadata';
        $this->trackingPath = $dir . '/.participant_metadata_tracking.json';
        $this->trackingData = [];

        if (!is_file($this->trackingPath)) {
            return;
        }
        $raw = @file_get_contents($this->trackingPath);
        if ($raw === false) {
            return;
        }
        $decoded = json_decode($raw, true);
        if (is_array($decoded)) {
            $this->trackingData = $decoded;
            $this->_logVerbose(
                "Loaded tracking file with " . count($this->trackingData)
                . " entries from {$this->trackingPath}"
            );
        }
    }

    /**
     * Persist tracking data. Skipped entirely in dry-run.
     */
    private function _saveTrackingFile(): void
    {
        if ($this->dryRun) {
            return;
        }
        if ($this->trackingPath === null) {
            return;
        }
        $dir = dirname($this->trackingPath);
        if (!is_dir($dir)) {
            @mkdir($dir, 0755, true);
        }
        $json = json_encode($this->trackingData, JSON_PRETTY_PRINT);
        if (@file_put_contents($this->trackingPath, $json) === false) {
            $this->_log("  WARNING: could not write tracking file at {$this->trackingPath}");
        }
    }

    /**
     * Record this source's hash and per-source stats into trackingData.
     * Caller already decided this source's run was clean (no unresolved,
     * no failed updates from this source).
     */
    private function _recordTracking(string $basename, string $hash, array $preStats, int $rowsRead): void
    {
        $now      = date('c');
        $existing = $this->trackingData[$basename] ?? [];

        $thisSourceFieldsUpdated = $this->stats['fields_updated']    - $preStats['fields_updated'];
        $thisSourceRowsWithData  = $this->stats['rows_with_data']    - $preStats['rows_with_data'];

        $this->trackingData[$basename] = [
            'hash'              => $hash,
            'first_uploaded_at' => $existing['first_uploaded_at'] ?? $now,
            'last_uploaded_at'  => $now,
            'run_timestamp'     => $this->runTimestamp,
            'upload_count'      => (int) ($existing['upload_count'] ?? 0) + 1,
            'rows_total'        => $rowsRead,
            'rows_with_data'    => $thisSourceRowsWithData,
            'fields_updated'    => $thisSourceFieldsUpdated,
        ];
    }

    private function _emptyStats(): array
    {
        return [
            'sources_scanned'           => 0,
            'sources_skipped_unchanged' => 0,
            'rows_scanned'              => 0,
            'rows_with_data'            => 0,
            'rows_without_data'         => 0,
            'candidates_resolved'       => 0,
            'candidates_unresolved'     => [],
            'updates_attempted'         => 0,
            'updates_succeeded'         => 0,
            'updates_failed'            => [],
            'fields_updated'            => 0,
            'fields_updated_by_name'    => [],
            'fields_skipped_idempotent' => 0,
            'duplicates_skipped'        => 0,
            'conflicts'                 => [],
        ];
    }

    private function _resetStats(): void
    {
        $this->stats            = $this->_emptyStats();
        $this->processedThisRun = [];
    }

    private function _accumulate(array $stats): void
    {
        foreach ($stats as $key => $val) {
            if (is_array($val)) {
                if ($key === 'fields_updated_by_name') {
                    foreach ($val as $field => $count) {
                        $this->grandTotal[$key][$field] =
                            ($this->grandTotal[$key][$field] ?? 0) + $count;
                    }
                } else {
                    $this->grandTotal[$key] = array_merge(
                        $this->grandTotal[$key] ?? [],
                        $val
                    );
                }
            } else {
                $this->grandTotal[$key] = ($this->grandTotal[$key] ?? 0) + $val;
            }
        }
    }

    private function _printGrandTotal(int $projectCount): void
    {
        echo "\n";
        echo "════════════════════════════════════════\n";
        echo "GRAND TOTAL across $projectCount projects:\n";
        echo "  Sources scanned:        {$this->grandTotal['sources_scanned']}\n";
        echo "  Sources skipped:        {$this->grandTotal['sources_skipped_unchanged']} (unchanged)\n";
        echo "  Rows scanned:           {$this->grandTotal['rows_scanned']}\n";
        echo "  Rows with field data:   {$this->grandTotal['rows_with_data']}\n";
        echo "  Candidates resolved:    {$this->grandTotal['candidates_resolved']}\n";
        echo "  Candidates unresolved:  " . count($this->grandTotal['candidates_unresolved']) . "\n";
        echo "  Updates attempted:      {$this->grandTotal['updates_attempted']}\n";
        echo "  Updates succeeded:      {$this->grandTotal['updates_succeeded']}\n";
        echo "  Fields updated:         {$this->grandTotal['fields_updated']}\n";

        if (!empty($this->grandTotal['fields_updated_by_name'])) {
            foreach ($this->grandTotal['fields_updated_by_name'] as $field => $count) {
                echo "    - $field:" . str_repeat(' ', max(1, 22 - strlen($field))) . "$count\n";
            }
        }

        echo "  Fields skipped:         {$this->grandTotal['fields_skipped_idempotent']}\n";
        echo "  Duplicates skipped:     {$this->grandTotal['duplicates_skipped']}\n";
        echo "  Conflicts (first won):  " . count($this->grandTotal['conflicts']) . "\n";
        echo "  Updates failed:         " . count($this->grandTotal['updates_failed']) . "\n";
        echo "════════════════════════════════════════\n";
    }

    // =========================================================================
    //  LOGGING
    // =========================================================================

    private function _initLogger(): Logger
    {
        $logger    = new Logger('participant-metadata');
        $formatter = new CleanLogFormatter();
        $console   = new StreamHandler('php://stdout', Logger::INFO);
        $console->setFormatter($formatter);
        $logger->pushHandler($console);
        return $logger;
    }

    private function _openLog(string $projectDir): void
    {
        $this->logDir = $projectDir . '/logs/participant_metadata';
        if (!is_dir($this->logDir)) {
            @mkdir($this->logDir, 0755, true);
        }
        $this->runLogPath = $this->logDir . '/participant_metadata_run_' . date('Y-m-d') . '.log';
        $this->runLogFh   = @fopen($this->runLogPath, 'a');
    }

    private function _log(string $msg): void
    {
        $this->logger->info($msg);
        if ($this->runLogFh) {
            fwrite($this->runLogFh, '[' . date('Y-m-d H:i:s') . "] $msg\n");
        }
    }

    private function _logVerbose(string $msg): void
    {
        if ($this->verbose) {
            $this->_log($msg);
        }
    }

    private function _closeLog(): void
    {
        if ($this->runLogFh) {
            fclose($this->runLogFh);
            $this->runLogFh = null;
        }
    }

    private function _logSummary(string $projectName): void
    {
        $this->_log("--- Summary for $projectName ---");
        $this->_log("Sources scanned:              {$this->stats['sources_scanned']}");
        $this->_log("Sources skipped (unchanged):  {$this->stats['sources_skipped_unchanged']}");
        $this->_log("Rows scanned:                 {$this->stats['rows_scanned']}");
        $this->_log("Rows with field data:         {$this->stats['rows_with_data']}");
        $this->_log("Rows without field data:      {$this->stats['rows_without_data']}");
        $this->_log("Candidates resolved:          {$this->stats['candidates_resolved']}");
        $this->_log("Candidates unresolved:        " . count($this->stats['candidates_unresolved']));
        $this->_log("Candidate updates attempted:  {$this->stats['updates_attempted']}");
        $this->_log("Candidate updates succeeded:  {$this->stats['updates_succeeded']}");
        $this->_log("Fields updated:               {$this->stats['fields_updated']}");

        if (!empty($this->stats['fields_updated_by_name'])) {
            foreach ($this->stats['fields_updated_by_name'] as $field => $count) {
                $this->_log("  - $field:" . str_repeat(' ', max(1, 26 - strlen($field))) . "$count");
            }
        }

        $this->_log("Fields skipped (idempotent):  {$this->stats['fields_skipped_idempotent']}");
        $this->_log("Duplicates skipped:           {$this->stats['duplicates_skipped']}");
        $this->_log("Conflicts (first won):        " . count($this->stats['conflicts']));
        $this->_log("Candidate updates failed:     " . count($this->stats['updates_failed']));
    }

    // =========================================================================
    //  EMAIL NOTIFICATION
    // =========================================================================

    private function _sendNotification(array $projectConfig, string $collectionName, string $projectName): void
    {
        // Dry-run: never send emails. Summary is already in console + log.
        if ($this->dryRun) {
            $this->_log("  Notification skipped (dry run)");
            return;
        }

        // Once-per-project guard so a wrapper that also calls notification
        // can't trigger duplicates.
        if ($this->notificationSent) {
            $this->_log("  Notification already sent this run — skipping duplicate");
            return;
        }

        // Read notification config — project.json takes priority, fall back
        // to global config, then to notification_defaults. Mirrors
        // BidsImportPipeline's lookup chain.
        $notifConfig = $projectConfig['notification_emails']['participant_metadata']
            ?? $this->config['notification_emails']['participant_metadata']
            ?? $this->config['notification_defaults']
            ?? [];

        if (!($notifConfig['enabled'] ?? false)) {
            $this->_log("  Notifications disabled (notification_emails.participant_metadata.enabled = false)");
            return;
        }

        $hasErrors = !empty($this->stats['candidates_unresolved'])
            || !empty($this->stats['updates_failed']);

        $successEmails = $notifConfig['on_success']
            ?? $this->config['notification_defaults']['default_on_success']
            ?? [];
        $errorEmails   = $notifConfig['on_error']
            ?? $this->config['notification_defaults']['default_on_error']
            ?? [];

        $recipients = $hasErrors ? $errorEmails : $successEmails;

        if (empty($recipients)) {
            $this->_log("  No notification recipients configured for participant_metadata");
            return;
        }

        $status  = $hasErrors ? 'ERROR' : 'SUCCESS';
        $subject = "{$status}: {$projectName} Participant Metadata Pipeline";

        $body  = "ParticipantMetadataPipeline run for $collectionName / $projectName\n";
        $body .= "Status: $status\n\n";
        $body .= "Sources scanned:              {$this->stats['sources_scanned']}\n";
        $body .= "Sources skipped (unchanged):  {$this->stats['sources_skipped_unchanged']}\n";
        $body .= "Rows scanned:                 {$this->stats['rows_scanned']}\n";
        $body .= "Rows with field data:         {$this->stats['rows_with_data']}\n";
        $body .= "Rows without field data:      {$this->stats['rows_without_data']}\n";
        $body .= "Candidates resolved:          {$this->stats['candidates_resolved']}\n";
        $body .= "Candidate updates attempted:  {$this->stats['updates_attempted']}\n";
        $body .= "Candidate updates succeeded:  {$this->stats['updates_succeeded']}\n";
        $body .= "Fields updated:               {$this->stats['fields_updated']}\n";

        if (!empty($this->stats['fields_updated_by_name'])) {
            foreach ($this->stats['fields_updated_by_name'] as $field => $count) {
                $body .= "  - $field:" . str_repeat(' ', max(1, 26 - strlen($field))) . "$count\n";
            }
        }

        $body .= "Fields skipped (idempotent):  {$this->stats['fields_skipped_idempotent']}\n";
        $body .= "Duplicates skipped:           {$this->stats['duplicates_skipped']}\n";
        $body .= "Conflicts (first won):        " . count($this->stats['conflicts']) . "\n";

        if (!empty($this->stats['conflicts'])) {
            $body .= "\nConflicts (first wins):\n";
            foreach ($this->stats['conflicts'] as $c) {
                $body .= "  - $c\n";
            }
        }

        if (!empty($this->stats['candidates_unresolved'])) {
            $body .= "\nUnresolved identifiers (" . count($this->stats['candidates_unresolved']) . "):\n";
            foreach ($this->stats['candidates_unresolved'] as $u) {
                $body .= "  - $u\n";
            }
        }

        if (!empty($this->stats['updates_failed'])) {
            $body .= "\nFailed updates (" . count($this->stats['updates_failed']) . "):\n";
            foreach ($this->stats['updates_failed'] as $f) {
                $body .= "  - $f\n";
            }
        }

        // Log file pointer at the bottom of the email so operators
        // can dig into per-row detail when something looks off.
        if ($this->runLogPath) {
            $body .= "\n" . str_repeat('-', 40) . "\n";
            $body .= "Full run log (per-row detail):\n";
            $body .= "  {$this->runLogPath}\n";
            $body .= "\nTo view on the server:\n";
            $body .= "  tail -n 200 {$this->runLogPath}\n";
        }

        $this->_log("  Sending notification to: " . implode(', ', $recipients));
        foreach ($recipients as $to) {
            $this->notification->send($to, $subject, $body);
        }
        $this->notificationSent = true;
    }
}