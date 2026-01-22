<?php
/**
 * ARCHIMEDES Imaging Pipeline
 * 
 * Processes BIDS imaging data from deidentified-lorisid/bids/.
 * Uses ScriptApi bidsimport endpoint. Only processes new scans.
 * Config-based with enable/disable flag (same as ClinicalPipeline).
 * 
 * @package LORIS\Pipelines
 */

namespace LORIS\Pipelines;

use LORIS\Endpoints\ImagingClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use LORIS\Utils\CleanLogFormatter;
use PHPMailer\PHPMailer\PHPMailer;

class ImagingPipeline
{
    private Logger $logger;
    private ImagingClient $client;
    private array $config;
    private array $projectConfig = [];
    
    private array $stats = [
        'scans_found' => 0,
        'scans_processed' => 0,
        'scans_skipped' => 0,
        'scans_failed' => 0,
        'errors' => [],
    ];

    public function __construct(array $config = [], bool $verbose = false)
    {
        $this->config = $config;
        $this->initLogger($verbose);
        
        $this->client = new ImagingClient(
            $config['loris_url'] ?? '',
            $config['verify_ssl'] ?? true
        );
    }

    private string $projectPath = '';

    private function initLogger(bool $verbose): void
    {
        $logLevel = $verbose ? Logger::DEBUG : Logger::INFO;
        $this->logger = new Logger('imaging');
        
        $formatter = new CleanLogFormatter();
        
        $consoleHandler = new StreamHandler('php://stdout', $logLevel);
        $consoleHandler->setFormatter($formatter);
        $this->logger->pushHandler($consoleHandler);
    }

    /**
     * Initialize project-specific log file in logs/ directory
     */
    private function initProjectLogger(string $projectPath): void
    {
        $logPath = $projectPath . '/logs';
        if (!is_dir($logPath)) {
            mkdir($logPath, 0755, true);
        }
        
        $formatter = new CleanLogFormatter();
        $logFile = $logPath . '/imaging_pipeline_' . date('Y-m-d') . '.log';
        $fileHandler = new StreamHandler($logFile, Logger::DEBUG);
        $fileHandler->setFormatter($formatter);
        $this->logger->pushHandler($fileHandler);
    }

    public function authenticate(string $username, string $password): bool
    {
        return $this->client->authenticate($username, $password);
    }

    /**
     * Check if imaging pipeline is enabled in config
     */
    public function isEnabled(): bool
    {
        return $this->config['pipelines']['imaging']['enabled'] ?? false;
    }

    /**
     * Run the imaging pipeline
     */
    public function run(string $projectPath, array $options = []): array
    {
        $this->projectPath = $projectPath;
        
        // Initialize project-specific logger in logs/ directory
        $this->initProjectLogger($projectPath);
        
        $this->logger->info("Starting Imaging Pipeline: {$projectPath}");
        
        // Check if enabled
        if (!$this->isEnabled()) {
            $this->logger->info("Imaging pipeline is disabled in config");
            return $this->stats;
        }
        
        // Load project config
        $this->projectConfig = $this->loadProjectConfig($projectPath);
        if (!$this->projectConfig) {
            $this->stats['errors'][] = "Failed to load project config";
            return $this->stats;
        }
        
        $projectName = $this->projectConfig['project_common_name'] ?? basename($projectPath);
        $this->logger->info("Project: {$projectName}");
        
        // Find BIDS directory (deidentified-lorisid/bids/)
        $bidsPath = $this->findBidsDirectory($projectPath);
        if (!$bidsPath) {
            $this->logger->warning("No BIDS directory found");
            return $this->stats;
        }
        
        $this->logger->info("BIDS directory: {$bidsPath}");
        
        // Find new scans
        $newScans = $this->findNewScans($bidsPath, $options);
        $this->stats['scans_found'] = count($newScans);
        
        $this->logger->info("Found {$this->stats['scans_found']} new scan(s)");
        
        if (empty($newScans)) {
            return $this->stats;
        }
        
        // Process scans
        foreach ($newScans as $scan) {
            $this->processScan($scan, $bidsPath, $options);
        }
        
        return $this->stats;
    }

    private function loadProjectConfig(string $projectPath): ?array
    {
        $configFile = $projectPath . '/project.json';
        if (!file_exists($configFile)) {
            $this->logger->error("Project config not found: {$configFile}");
            return null;
        }
        
        $config = json_decode(file_get_contents($configFile), true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->logger->error("Invalid JSON: " . json_last_error_msg());
            return null;
        }
        
        return $config;
    }

    /**
     * Find BIDS directory (deidentified-lorisid/bids/)
     */
    private function findBidsDirectory(string $projectPath): ?string
    {
        if (!empty($this->projectConfig['bids_path']) && is_dir($this->projectConfig['bids_path'])) {
            return $this->projectConfig['bids_path'];
        }
        
        $locations = [
            $projectPath . '/deidentified-lorisid/bids',
            $projectPath . '/deidentified-lorisid/imaging/bids',
            $projectPath . '/bids',
            $projectPath . '/BIDS',
        ];
        
        foreach ($locations as $path) {
            if (is_dir($path)) {
                return $path;
            }
        }
        
        return null;
    }

    /**
     * Find new scans that haven't been processed
     */
    private function findNewScans(string $bidsPath, array $options): array
    {
        $newScans = [];
        
        // Tracking file in processed/ directory
        $processedDir = $this->projectPath . '/processed';
        if (!is_dir($processedDir)) {
            mkdir($processedDir, 0755, true);
        }
        $processedFile = $processedDir . '/.imaging_processed.json';
        
        $processed = [];
        if (file_exists($processedFile)) {
            $processed = json_decode(file_get_contents($processedFile), true) ?? [];
        }
        
        $subjects = glob($bidsPath . '/sub-*', GLOB_ONLYDIR);
        
        foreach ($subjects as $subjectDir) {
            $subjectId = basename($subjectDir);
            $sessions = glob($subjectDir . '/ses-*', GLOB_ONLYDIR);
            
            if (empty($sessions)) {
                $sessions = [$subjectDir];
            }
            
            foreach ($sessions as $sessionDir) {
                $sessionId = basename($sessionDir);
                $scanKey = "{$subjectId}/{$sessionId}";
                
                $lastModified = $this->getDirectoryMtime($sessionDir);
                
                if (isset($processed[$scanKey]) && $processed[$scanKey] >= $lastModified) {
                    if (empty($options['force'])) {
                        $this->logger->debug("Skipping: {$scanKey}");
                        $this->stats['scans_skipped']++;
                        continue;
                    }
                }
                
                if (!$this->hasImagingData($sessionDir)) {
                    continue;
                }
                
                $newScans[] = [
                    'subject_id' => $subjectId,
                    'session_id' => $sessionId,
                    'path' => $sessionDir,
                    'scan_key' => $scanKey,
                    'last_modified' => $lastModified,
                ];
            }
        }
        
        return $newScans;
    }

    private function getDirectoryMtime(string $dir): int
    {
        $maxTime = filemtime($dir);
        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($dir, \RecursiveDirectoryIterator::SKIP_DOTS)
        );
        foreach ($iterator as $file) {
            $mtime = $file->getMTime();
            if ($mtime > $maxTime) $maxTime = $mtime;
        }
        return $maxTime;
    }

    private function hasImagingData(string $dir): bool
    {
        foreach (['anat', 'func', 'dwi', 'fmap', 'pet', 'eeg', 'meg'] as $mod) {
            if (is_dir($dir . '/' . $mod)) return true;
        }
        return !empty(glob($dir . '/*.nii*'));
    }

    /**
     * Process scan using BIDS import API
     */
    private function processScan(array $scan, string $bidsPath, array $options): void
    {
        $this->logger->info("Processing: {$scan['scan_key']}");
        
        try {
            $result = $this->client->runBidsImport($bidsPath, [
                'profile' => $this->config['loris_profile'] ?? 'prod',
                'create_candidate' => $options['create_candidate'] ?? true,
                'create_session' => $options['create_session'] ?? true,
                'verbose' => $options['verbose'] ?? false,
                'no_bids_validation' => $options['no_bids_validation'] ?? false,
                'async' => $options['async'] ?? false,
            ]);
            
            if ($result['success']) {
                $this->stats['scans_processed']++;
                $this->markAsProcessed($scan, $bidsPath);
                $this->logger->info("Success: {$scan['scan_key']}");
            } else {
                $this->stats['scans_failed']++;
                $error = $result['error'] ?? 'Unknown error';
                $this->stats['errors'][] = "{$scan['scan_key']}: {$error}";
                $this->logger->error("Failed: {$scan['scan_key']} - {$error}");
            }
        } catch (\Exception $e) {
            $this->stats['scans_failed']++;
            $this->stats['errors'][] = "{$scan['scan_key']}: " . $e->getMessage();
            $this->logger->error("Exception: " . $e->getMessage());
        }
    }

    private function markAsProcessed(array $scan, string $bidsPath): void
    {
        $processedDir = $this->projectPath . '/processed';
        if (!is_dir($processedDir)) {
            mkdir($processedDir, 0755, true);
        }
        $processedFile = $processedDir . '/.imaging_processed.json';
        
        $processed = [];
        if (file_exists($processedFile)) {
            $processed = json_decode(file_get_contents($processedFile), true) ?? [];
        }
        $processed[$scan['scan_key']] = time();
        file_put_contents($processedFile, json_encode($processed, JSON_PRETTY_PRINT));
    }

    /**
     * Send notification email (uses config settings like ClinicalPipeline)
     */
    public function sendNotification(bool $success): void
    {
        // Check if notifications enabled in config
        $notificationsEnabled = $this->config['notifications']['enabled'] ?? false;
        if (!$notificationsEnabled) {
            $this->logger->debug("Notifications disabled in config");
            return;
        }
        
        // Check project-level notification settings
        $notifyConfig = $this->projectConfig['notification_emails']['imaging'] ?? null;
        if (!$notifyConfig || !($notifyConfig['enabled'] ?? false)) {
            $this->logger->debug("Imaging notifications disabled for project");
            return;
        }
        
        $recipients = $success
            ? ($notifyConfig['on_success'] ?? [])
            : ($notifyConfig['on_error'] ?? []);
        
        if (empty($recipients)) {
            return;
        }
        
        $projectName = $this->projectConfig['project_common_name'] ?? 'Unknown';
        $subject = $success
            ? "[ARCHIMEDES] Imaging Pipeline Success - {$projectName}"
            : "[ARCHIMEDES] Imaging Pipeline FAILED - {$projectName}";
        
        $body = $this->buildEmailBody($success);
        $this->sendEmail($recipients, $subject, $body);
    }

    private function buildEmailBody(bool $success): string
    {
        $projectName = $this->projectConfig['project_common_name'] ?? 'Unknown';
        $timestamp = date('Y-m-d H:i:s');
        
        $body = "ARCHIMEDES Imaging Pipeline Report\n";
        $body .= "===================================\n\n";
        $body .= "Project: {$projectName}\n";
        $body .= "Timestamp: {$timestamp}\n";
        $body .= "Status: " . ($success ? "SUCCESS" : "FAILED") . "\n\n";
        $body .= "Statistics:\n";
        $body .= "-----------\n";
        $body .= "Scans Found: {$this->stats['scans_found']}\n";
        $body .= "Scans Processed: {$this->stats['scans_processed']}\n";
        $body .= "Scans Skipped: {$this->stats['scans_skipped']}\n";
        $body .= "Scans Failed: {$this->stats['scans_failed']}\n";
        
        if (!empty($this->stats['errors'])) {
            $body .= "\nErrors:\n-------\n";
            foreach ($this->stats['errors'] as $error) {
                $body .= "- {$error}\n";
            }
        }
        
        return $body;
    }

    /**
     * Send email using config SMTP settings (same as ClinicalPipeline)
     */
    private function sendEmail(array $recipients, string $subject, string $body): void
    {
        try {
            $mail = new PHPMailer(true);
            
            // SMTP settings from config
            $smtpConfig = $this->config['smtp'] ?? [];
            
            if (!empty($smtpConfig['host'])) {
                $mail->isSMTP();
                $mail->Host = $smtpConfig['host'];
                $mail->Port = $smtpConfig['port'] ?? 587;
                $mail->SMTPSecure = $smtpConfig['secure'] ?? PHPMailer::ENCRYPTION_STARTTLS;
                
                if (!empty($smtpConfig['username'])) {
                    $mail->SMTPAuth = true;
                    $mail->Username = $smtpConfig['username'];
                    $mail->Password = $smtpConfig['password'] ?? '';
                }
            }
            
            // From address from config
            $mail->setFrom(
                $this->config['notifications']['from_email'] ?? 'archimedes@localhost',
                $this->config['notifications']['from_name'] ?? 'ARCHIMEDES Pipeline'
            );
            
            foreach ($recipients as $recipient) {
                $mail->addAddress($recipient);
            }
            
            $mail->Subject = $subject;
            $mail->Body = $body;
            $mail->send();
            
            $this->logger->info("Notification sent to: " . implode(', ', $recipients));
        } catch (\Exception $e) {
            $this->logger->error("Failed to send notification: " . $e->getMessage());
        }
    }

    public function getStats(): array
    {
        return $this->stats;
    }

    public function getProjectConfig(): array
    {
        return $this->projectConfig;
    }
}
