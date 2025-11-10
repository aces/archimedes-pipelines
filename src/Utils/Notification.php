<?php
declare(strict_types=1);

namespace LORIS\Client\Utils;

use PHPMailer\PHPMailer\PHPMailer;
use PHPMailer\PHPMailer\SMTP;
use PHPMailer\PHPMailer\Exception as PHPMailerException;
use Psr\Log\LoggerInterface;

/**
 * Email Notification System
 * Sends email notifications for pipeline success/failure per modality
 */
class Notification
{
    private array $smtpConfig;
    private array $defaultRecipients;
    private LoggerInterface $logger;

    public function __construct(array $config, LoggerInterface $logger)
    {
        $this->smtpConfig = $config['notification_defaults']['smtp'] ?? [];
        $this->defaultRecipients = [
            'success' => $config['notification_defaults']['default_on_success'] ?? [],
            'error' => $config['notification_defaults']['default_on_error'] ?? []
        ];
        $this->logger = $logger;
    }

    /**
     * Send notification email
     * 
     * @param string $modality Modality name (clinical, imaging, etc.)
     * @param string $type success or error
     * @param array $recipients Email addresses
     * @param string $subject Email subject
     * @param string $body Email body (HTML)
     * @return bool Success
     */
    public function send(
        string $modality,
        string $type,
        array $recipients,
        string $subject,
        string $body
    ): bool {
        // Skip if no SMTP config
        if (empty($this->smtpConfig)) {
            $this->logger->debug("SMTP not configured, skipping notification");
            return true;
        }

        // Use default recipients if none provided
        if (empty($recipients)) {
            $recipients = $this->defaultRecipients[$type] ?? [];
        }

        if (empty($recipients)) {
            $this->logger->debug("No recipients configured for {$modality} {$type}");
            return true;
        }

        try {
            $mail = new PHPMailer(true);

            // SMTP Configuration
            $mail->isSMTP();
            $mail->Host = $this->smtpConfig['host'];
            $mail->SMTPAuth = true;
            $mail->Username = $this->smtpConfig['username'];
            $mail->Password = $this->smtpConfig['password'];
            $mail->SMTPSecure = PHPMailer::ENCRYPTION_STARTTLS;
            $mail->Port = $this->smtpConfig['port'];

            // Email content
            $mail->setFrom(
                $this->smtpConfig['from'],
                'LORIS Data Ingestion'
            );

            foreach ($recipients as $email) {
                $mail->addAddress($email);
            }

            $mail->isHTML(true);
            $mail->Subject = $subject;
            $mail->Body = $body;

            $mail->send();
            
            $this->logger->info("✓ Notification sent to: " . implode(', ', $recipients));
            return true;
            
        } catch (PHPMailerException $e) {
            $this->logger->error("Failed to send notification: {$mail->ErrorInfo}");
            return false;
        }
    }

    /**
     * Send success notification
     */
    public function sendSuccess(
        string $modality,
        string $projectName,
        array $stats,
        array $recipients = []
    ): bool {
        $subject = "[LORIS] {$projectName} - {$modality} Ingestion Success";
        
        $body = $this->buildSuccessEmail($modality, $projectName, $stats);
        
        return $this->send($modality, 'success', $recipients, $subject, $body);
    }

    /**
     * Send error notification
     */
    public function sendError(
        string $modality,
        string $projectName,
        string $errorMessage,
        array $stats,
        array $recipients = []
    ): bool {
        $subject = "[LORIS] {$projectName} - {$modality} Ingestion ERROR";
        
        $body = $this->buildErrorEmail($modality, $projectName, $errorMessage, $stats);
        
        return $this->send($modality, 'error', $recipients, $subject, $body);
    }

    /**
     * Build success email HTML
     */
    private function buildSuccessEmail(string $modality, string $projectName, array $stats): string
    {
        return <<<HTML
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #28a745; color: white; padding: 20px; text-align: center; }
        .content { padding: 20px; background: #f8f9fa; }
        .stats { margin: 20px 0; }
        .stat-item { padding: 10px; background: white; margin: 5px 0; border-left: 4px solid #28a745; }
        .footer { padding: 20px; text-align: center; color: #6c757d; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>✓ LORIS Data Ingestion Success</h2>
        </div>
        <div class="content">
            <p><strong>Project:</strong> {$projectName}</p>
            <p><strong>Modality:</strong> {$modality}</p>
            <p><strong>Time:</strong> %s</p>
            
            <div class="stats">
                <h3>Statistics</h3>
                <div class="stat-item"><strong>Total Files:</strong> {$stats['total']}</div>
                <div class="stat-item"><strong>Successful:</strong> {$stats['success']}</div>
                <div class="stat-item"><strong>Failed:</strong> {$stats['failed']}</div>
                <div class="stat-item"><strong>Skipped:</strong> {$stats['skipped']}</div>
            </div>
        </div>
        <div class="footer">
            LORIS Automated Data Ingestion System
        </div>
    </div>
</body>
</html>
HTML;
    }

    /**
     * Build error email HTML
     */
    private function buildErrorEmail(string $modality, string $projectName, string $error, array $stats): string
    {
        $errorHtml = htmlspecialchars($error);
        
        return <<<HTML
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #dc3545; color: white; padding: 20px; text-align: center; }
        .content { padding: 20px; background: #f8f9fa; }
        .error { background: #fff3cd; border-left: 4px solid #dc3545; padding: 15px; margin: 20px 0; }
        .stats { margin: 20px 0; }
        .stat-item { padding: 10px; background: white; margin: 5px 0; border-left: 4px solid #dc3545; }
        .footer { padding: 20px; text-align: center; color: #6c757d; font-size: 12px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>✗ LORIS Data Ingestion Error</h2>
        </div>
        <div class="content">
            <p><strong>Project:</strong> {$projectName}</p>
            <p><strong>Modality:</strong> {$modality}</p>
            <p><strong>Time:</strong> %s</p>
            
            <div class="error">
                <h3>Error Details</h3>
                <pre>{$errorHtml}</pre>
            </div>
            
            <div class="stats">
                <h3>Statistics</h3>
                <div class="stat-item"><strong>Total Files:</strong> {$stats['total']}</div>
                <div class="stat-item"><strong>Successful:</strong> {$stats['success']}</div>
                <div class="stat-item"><strong>Failed:</strong> {$stats['failed']}</div>
                <div class="stat-item"><strong>Skipped:</strong> {$stats['skipped']}</div>
            </div>
        </div>
        <div class="footer">
            LORIS Automated Data Ingestion System
        </div>
    </div>
</body>
</html>
HTML;
    }
}
