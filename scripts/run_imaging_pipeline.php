#!/usr/bin/env php
<?php
/**
 * ARCHIMEDES Imaging Pipeline Runner
 * 
 * Processes BIDS imaging data using ScriptApi bidsimport endpoint.
 * Follows same CLI pattern as run_clinical_pipeline.php.
 * 
 * Usage:
 *   Process All Projects:
 *     php scripts/run_imaging_pipeline.php --all
 * 
 *   Process Specific Project:
 *     php scripts/run_imaging_pipeline.php --collection=COLLECTION_NAME --project=PROJECT_NAME
 * 
 * @package LORIS\Scripts
 */

require_once __DIR__ . '/../vendor/autoload.php';

use LORIS\Pipelines\ImagingPipeline;

// Parse command line options
$options = getopt('', [
    'all',
    'collection:',
    'project:',
    'config:',
    'force',
    'async',
    'no-validate',
    'dry-run',
    'verbose',
    'help',
]);

if (isset($options['help'])) {
    showHelp();
    exit(0);
}

// Load configuration
$configPath = $options['config'] ?? __DIR__ . '/../config/loris_client_config.json';
if (!file_exists($configPath)) {
    fwrite(STDERR, "Error: Config file not found: {$configPath}\n");
    exit(1);
}

$config = json_decode(file_get_contents($configPath), true);
if (json_last_error() !== JSON_ERROR_NONE) {
    fwrite(STDERR, "Error: Invalid config JSON: " . json_last_error_msg() . "\n");
    exit(1);
}

// Check if imaging pipeline is enabled
if (!($config['pipelines']['imaging']['enabled'] ?? false)) {
    echo "Imaging pipeline is disabled in config.\n";
    echo "Set pipelines.imaging.enabled = true in loris_client_config.json\n";
    exit(0);
}

$verbose = isset($options['verbose']);
$dryRun = isset($options['dry-run']);

// Determine mode: --all or --collection/--project
$processAll = isset($options['all']);
$collection = $options['collection'] ?? null;
$project = $options['project'] ?? null;

if (!$processAll && (!$collection || !$project)) {
    fwrite(STDERR, "Error: Must specify either --all or both --collection and --project\n");
    showHelp();
    exit(1);
}

// Initialize pipeline
$pipeline = new ImagingPipeline($config, $verbose);

// Authenticate
$username = $config['loris_user'] ?? getenv('LORIS_USER');
$password = $config['loris_pass'] ?? getenv('LORIS_PASS');

if (!$username || !$password) {
    fwrite(STDERR, "Error: LORIS credentials not configured\n");
    exit(1);
}

if (!$dryRun) {
    echo "Authenticating with LORIS...\n";
    if (!$pipeline->authenticate($username, $password)) {
        fwrite(STDERR, "Error: Authentication failed\n");
        exit(1);
    }
    echo "Authentication successful.\n";
}

// Build processing options
$pipelineOptions = [
    'force' => isset($options['force']),
    'async' => isset($options['async']),
    'no_bids_validation' => isset($options['no-validate']),
    'verbose' => $verbose,
    'dry_run' => $dryRun,
    'create_candidate' => true,
    'create_session' => true,
];

// Get data path from config
$dataPath = $config['data_path'] ?? '/data/archimedes';

$totalStats = [
    'projects_processed' => 0,
    'projects_skipped' => 0,
    'scans_found' => 0,
    'scans_processed' => 0,
    'scans_skipped' => 0,
    'scans_failed' => 0,
    'errors' => [],
];

if ($processAll) {
    // Process all projects
    echo "Processing all projects...\n";
    $projects = findAllProjects($dataPath);
    
    if (empty($projects)) {
        echo "No projects found in {$dataPath}\n";
        exit(0);
    }
    
    echo "Found " . count($projects) . " project(s)\n\n";
    
    foreach ($projects as $projectInfo) {
        $result = processProject($pipeline, $projectInfo, $pipelineOptions, $dryRun);
        aggregateStats($totalStats, $result);
    }
} else {
    // Process specific project
    $projectPath = "{$dataPath}/{$collection}/{$project}";
    
    if (!is_dir($projectPath)) {
        fwrite(STDERR, "Error: Project directory not found: {$projectPath}\n");
        exit(1);
    }
    
    $projectInfo = [
        'collection' => $collection,
        'project' => $project,
        'path' => $projectPath,
    ];
    
    $result = processProject($pipeline, $projectInfo, $pipelineOptions, $dryRun);
    aggregateStats($totalStats, $result);
}

// Print summary
printSummary($totalStats, $dryRun);

// Exit code based on failures
$exitCode = ($totalStats['scans_failed'] > 0 || !empty($totalStats['errors'])) ? 1 : 0;
exit($exitCode);

// ============================================================================
// Functions
// ============================================================================

function showHelp(): void
{
    echo <<<HELP
ARCHIMEDES Imaging Pipeline

Usage:
  Process All Projects:
    php scripts/run_imaging_pipeline.php --all

  Process Specific Project:
    php scripts/run_imaging_pipeline.php --collection=COLLECTION_NAME --project=PROJECT_NAME

Options:
  --all                 Process all projects
  --collection=NAME     Specific collection name
  --project=NAME        Specific project name
  --config=PATH         Path to config file (default: config/loris_client_config.json)
  --force               Force reprocess all scans (ignore tracking)
  --async               Run BIDS import asynchronously
  --no-validate         Skip BIDS validation
  --dry-run             Show what would be processed without making changes
  --verbose             Enable detailed output
  --help                Show this help message

Examples:
  php scripts/run_imaging_pipeline.php --all
  php scripts/run_imaging_pipeline.php --all --dry-run
  php scripts/run_imaging_pipeline.php --collection=archimedes --project=FDG-PET
  php scripts/run_imaging_pipeline.php --collection=archimedes --project=CNMDP --force --verbose

HELP;
}

function findAllProjects(string $dataPath): array
{
    $projects = [];
    
    // Scan for collections (first-level directories)
    $collections = glob($dataPath . '/*', GLOB_ONLYDIR);
    
    foreach ($collections as $collectionPath) {
        $collectionName = basename($collectionPath);
        
        // Skip hidden directories
        if (strpos($collectionName, '.') === 0) {
            continue;
        }
        
        // Scan for projects (second-level directories)
        $projectDirs = glob($collectionPath . '/*', GLOB_ONLYDIR);
        
        foreach ($projectDirs as $projectPath) {
            $projectName = basename($projectPath);
            
            // Skip hidden directories
            if (strpos($projectName, '.') === 0) {
                continue;
            }
            
            // Check if project has project.json
            if (!file_exists($projectPath . '/project.json')) {
                continue;
            }
            
            $projects[] = [
                'collection' => $collectionName,
                'project' => $projectName,
                'path' => $projectPath,
            ];
        }
    }
    
    return $projects;
}

function processProject(ImagingPipeline $pipeline, array $projectInfo, array $options, bool $dryRun): array
{
    $label = "{$projectInfo['collection']}/{$projectInfo['project']}";
    echo "----------------------------------------\n";
    echo "Processing: {$label}\n";
    echo "Path: {$projectInfo['path']}\n";
    
    if ($dryRun) {
        echo "[DRY RUN] Would process project: {$label}\n";
        return dryRunProject($projectInfo['path']);
    }
    
    $stats = $pipeline->run($projectInfo['path'], $options);
    
    // Send notification
    $success = $stats['scans_failed'] === 0 && empty($stats['errors']);
    $pipeline->sendNotification($success);
    
    echo "  Scans Found: {$stats['scans_found']}\n";
    echo "  Scans Processed: {$stats['scans_processed']}\n";
    echo "  Scans Skipped: {$stats['scans_skipped']}\n";
    echo "  Scans Failed: {$stats['scans_failed']}\n";
    
    return $stats;
}

function dryRunProject(string $projectPath): array
{
    $stats = [
        'scans_found' => 0,
        'scans_processed' => 0,
        'scans_skipped' => 0,
        'scans_failed' => 0,
        'errors' => [],
    ];
    
    // Find BIDS directory
    $bidsLocations = [
        $projectPath . '/deidentified-lorisid/bids',
        $projectPath . '/deidentified-lorisid/imaging/bids',
        $projectPath . '/bids',
    ];
    
    $bidsPath = null;
    foreach ($bidsLocations as $loc) {
        if (is_dir($loc)) {
            $bidsPath = $loc;
            break;
        }
    }
    
    if (!$bidsPath) {
        echo "  [DRY RUN] No BIDS directory found\n";
        return $stats;
    }
    
    echo "  [DRY RUN] BIDS directory: {$bidsPath}\n";
    echo "  [DRY RUN] Logs: {$projectPath}/logs/\n";
    echo "  [DRY RUN] Tracking: {$projectPath}/processed/.imaging_processed.json\n";
    
    // Count subjects/sessions
    $subjects = glob($bidsPath . '/sub-*', GLOB_ONLYDIR);
    foreach ($subjects as $subjectDir) {
        $sessions = glob($subjectDir . '/ses-*', GLOB_ONLYDIR);
        if (empty($sessions)) {
            $sessions = [$subjectDir];
        }
        $stats['scans_found'] += count($sessions);
    }
    
    echo "  [DRY RUN] Would process {$stats['scans_found']} scan(s)\n";
    
    return $stats;
}

function aggregateStats(array &$total, array $stats): void
{
    $total['projects_processed']++;
    $total['scans_found'] += $stats['scans_found'];
    $total['scans_processed'] += $stats['scans_processed'];
    $total['scans_skipped'] += $stats['scans_skipped'];
    $total['scans_failed'] += $stats['scans_failed'];
    $total['errors'] = array_merge($total['errors'], $stats['errors'] ?? []);
}

function printSummary(array $stats, bool $dryRun): void
{
    echo "\n========================================\n";
    echo $dryRun ? "DRY RUN SUMMARY\n" : "IMAGING PIPELINE SUMMARY\n";
    echo "========================================\n";
    echo "Projects Processed: {$stats['projects_processed']}\n";
    echo "Total Scans Found: {$stats['scans_found']}\n";
    echo "Total Scans Processed: {$stats['scans_processed']}\n";
    echo "Total Scans Skipped: {$stats['scans_skipped']}\n";
    echo "Total Scans Failed: {$stats['scans_failed']}\n";
    
    if (!empty($stats['errors'])) {
        echo "\nErrors:\n";
        foreach ($stats['errors'] as $error) {
            echo "  - {$error}\n";
        }
    }
    
    echo "========================================\n";
}
