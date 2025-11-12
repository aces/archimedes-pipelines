<?php
declare(strict_types=1);

namespace LORIS\Client\Utils;

use PHPMailer\PHPMailer\PHPMailer;
use PHPMailer\PHPMailer\Exception;
use Psr\Log\LoggerInterface;

/**
 * Notification System with Enable/Disable Controls
 */
class Notification
{
    private array $config;
    private LoggerInterface $logger;
    private bool $globalEnabled;
    private array $modalityEnabled;
    private array $smtpConfig;
    private array $defaults;

    public function __construct(array $config, LoggerInterface $logger)
    {
        $this->config = $config;
        $this->logger = $logger;

        // Parse notification configuration
        $notifConfig = $config['notifications'] ?? [];

        $this->globalEnabled = $notifConfig['enabled'] ?? true;
        $this->modalityEnabled = $notifConfig['modalities'] ?? [];
        $this->smtpConfig = $notifConfig['smtp'] ?? [];
        $this->defaults = $notifConfig['defaults'] ?? [
            'send_on_success' => true,
            'send_on_error' => true,
            'include_stats' => true,
            'include_error_details' => true
        ];

        // Log notification status
        if (!$this->globalEnabled) {
            $this->logger->info("Notifications: DISABLED globally");
        } else {
            $enabledModalities = [];
            foreach ($this->modalityEnabled as $modality => $modalityConfig) {
                if ($modalityConfig['enabled'] ?? true) {
                    $enabledModalities[] = $modality;
                }
            }
            $this->logger->info("Notifications: ENABLED for [" . implode(', ', $enabledModalities) . "]");
        }
    }

    /**
     * Check if notifications are enabled for a modality
     */
    public function isEnabled(string $modality): bool
    {
        if (!$this->globalEnabled) {
            return false;
        }

        $modalityConfig = $this->modalityEnabled[$modality] ?? null;
        if ($modalityConfig === null) {
            // If modality not explicitly configured, use global setting
            return true;
        }

        return $modalityConfig['enabled'] ?? true;
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
        // Check if enabled
        if (!$this->isEnabled($modality)) {
            $this->logger->debug("Notifications disabled for {$modality}, skipping success notification");
            return true;
        }

        if (!$this->defaults['send_on_success']) {
            $this->logger->debug("Success notifications disabled, skipping");
            return true;
        }

        if (empty($recipients)) {
            $this->logger->debug("No recipients configured for success notification");
            return true;
        }

        $subject = "[LORIS] ✓ {$projectName} - {$modality} Processing Completed";

        $body = $this->buildSuccessEmail($projectName, $modality, $stats);

        return $this->sendEmail($recipients, $subject, $body);
    }

    /**
     * Send error notification
     */
    public function sendError(
        string $modality,
        string $projectName,
        string $message,
        array $stats,
        array $recipients = []
    ): bool {
        // Check if enabled
        if (!$this->isEnabled($modality)) {
            $this->logger->debug("Notifications disabled for {$modality}, skipping error notification");
            return true;
        }

        if (!$this->defaults['send_on_error']) {
            $this->logger->debug("Error notifications disabled, skipping");
            return true;
        }

        if (empty($recipients)) {
            $this->logger->debug("No recipients configured for error notification");
            return true;
        }

        $subject = "[LORIS] ✗ {$projectName} - {$modality} Processing Failed";

        $body = $this->buildErrorEmail($projectName, $modality, $message, $stats);

        return $this->sendEmail($recipients, $subject, $body, true);
    }

    /**
     * Build success email body
     */
    private function buildSuccessEmail(string $projectName, string $modality, array $stats): string
    {
        $html = "
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; }
                .header { background-color: #4CAF50; color: white; padding: 15px; }
                .content { padding: 20px; }
                .stats { background-color: #f5f5f5; padding: 15px; margin: 15px 0; }
                .stats-table { width: 100%; border-collapse: collapse; }
                .stats-table td { padding: 8px; border-bottom: 1px solid #ddd; }
                .stats-table td:first-child { font-weight: bold; width: 200px; }
                .footer { color: #666; font-size: 12px; margin-top: 20px; }
            </style>
        </head>
        <body>
            <div class='header'>
                <h2>✓ {$modality} Processing Completed Successfully</h2>
            </div>
            <div class='content'>
                <p><strong>Project:</strong> {$projectName}</p>
                <p><strong>Modality:</strong> {$modality}</p>
                <p><strong>Time:</strong> " . date('Y-m-d H:i:s') . "</p>
        ";

        if ($this->defaults['include_stats'] && !empty($stats)) {
            $html .= "
                <div class='stats'>
                    <h3>Processing Statistics</h3>
                    <table class='stats-table'>
            ";

            foreach ($stats as $key => $value) {
                $label = ucfirst(str_replace('_', ' ', $key));
                $html .= "<tr><td>{$label}</td><td>{$value}</td></tr>";
            }

            $html .= "
                    </table>
                </div>
            ";
        }

        $html .= "
                <div class='footer'>
                    <p>This is an automated notification from the LORIS data ingestion pipeline.</p>
                </div>
            </div>
        </body>
        </html>
        ";

        return $html;
    }

    /**
     * Build error email body
     */
    private function buildErrorEmail(
        string $projectName,
        string $modality,
        string $message,
        array $stats
    ): string {
        $html = "
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; }
                .header { background-color: #f44336; color: white; padding: 15px; }
                .content { padding: 20px; }
                .error-box { background-color: #ffebee; border-left: 4px solid #f44336; padding: 15px; margin: 15px 0; }
                .stats { background-color: #f5f5f5; padding: 15px; margin: 15px 0; }
                .stats-table { width: 100%; border-collapse: collapse; }
                .stats-table td { padding: 8px; border-bottom: 1px solid #ddd; }
                .stats-table td:first-child { font-weight: bold; width: 200px; }
                .footer { color: #666; font-size: 12px; margin-top: 20px; }
            </style>
        </head>
        <body>
            <div class='header'>
                <h2>✗ {$modality} Processing Failed</h2>
            </div>
            <div class='content'>
                <p><strong>Project:</strong> {$projectName}</p>
                <p><strong>Modality:</strong> {$modality}</p>
                <p><strong>Time:</strong> " . date('Y-m-d H:i:s') . "</p>
                
                <div class='error-box'>
                    <h3>Error Message</h3>
                    <p>{$message}</p>
                </div>
        ";

        if ($this->defaults['include_stats'] && !empty($stats)) {
            $html .= "
                <div class='stats'>
                    <h3>Processing Statistics</h3>
                    <table class='stats-table'>
            ";

            foreach ($stats as $key => $value) {
                $label = ucfirst(str_replace('_', ' ', $key));
                $html .= "<tr><td>{$label}</td><td>{$value}</td></tr>";
            }

            $html .= "
                    </table>
                </div>
            ";
        }

        $html .= "
                <div class='footer'>
                    <p>This is an automated notification from the LORIS data ingestion pipeline.</p>
                    <p><strong>Action Required:</strong> Please investigate the errors and re-run the pipeline.</p>
                </div>
            </div>
        </body>
        </html>
        ";

        return $html;
    }

    /**
     * Send email via SMTP or native mail()
     */
    private function sendEmail(
        array $recipients,
        string $subject,
        string $body,
        bool $isError = false
    ): bool {
        // Check if we should use native PHP mail() function
        $useNativeMail = $this->smtpConfig['use_native_mail'] ?? false;

        if ($useNativeMail || empty($this->smtpConfig['host'])) {
            return $this->sendEmailNative($recipients, $subject, $body, $isError);
        } else {
            return $this->sendEmailSMTP($recipients, $subject, $body, $isError);
        }
    }

    /**
     * Send email using native PHP mail() function (uses server's mail config)
     */
    private function sendEmailNative(
        array $recipients,
        string $subject,
        string $body,
        bool $isError = false
    ): bool {
        try {
            // Get from address
            $fromEmail = $this->smtpConfig['from']['email'] ?? $this->smtpConfig['from'] ?? 'noreply@localhost';
            $fromName = $this->smtpConfig['from']['name'] ?? 'LORIS Pipeline';

            // Build headers
            $headers = [];
            $headers[] = "MIME-Version: 1.0";
            $headers[] = "Content-Type: text/html; charset=UTF-8";
            $headers[] = "From: {$fromName} <{$fromEmail}>";
            $headers[] = "Reply-To: {$fromEmail}";
            $headers[] = "X-Mailer: PHP/" . phpversion();

            if ($isError) {
                $headers[] = "X-Priority: 1"; // High priority
                $headers[] = "Importance: High";
            }

            $headersString = implode("\r\n", $headers);

            // Send to each recipient
            $allSent = true;
            foreach ($recipients as $recipient) {
                $sent = mail($recipient, $subject, $body, $headersString);
                if (!$sent) {
                    $this->logger->error("Failed to send email to: {$recipient}");
                    $allSent = false;
                } else {
                    $this->logger->debug("Email sent to: {$recipient}");
                }
            }

            if ($allSent) {
                $this->logger->info("Email sent successfully to: " . implode(', ', $recipients));
                return true;
            } else {
                $this->logger->error("Some emails failed to send");
                return false;
            }

        } catch (\Exception $e) {
            $this->logger->error("Failed to send notification via mail(): " . $e->getMessage());
            return false;
        }
    }

    /**
     * Send email via SMTP (using PHPMailer)
     */
    private function sendEmailSMTP(
        array $recipients,
        string $subject,
        string $body,
        bool $isError = false
    ): bool {
        if (empty($this->smtpConfig)) {
            $this->logger->error("SMTP configuration not found");
            return false;
        }

        try {
            $mail = new PHPMailer(true);

            // Server settings
            $mail->isSMTP();
            $mail->Host       = $this->smtpConfig['host'];
            $mail->SMTPAuth   = !empty($this->smtpConfig['username']);
            $mail->Username   = $this->smtpConfig['username'] ?? '';
            $mail->Password   = $this->smtpConfig['password'] ?? '';

            // Set encryption
            $secure = strtolower($this->smtpConfig['secure'] ?? 'tls');
            if ($secure === 'ssl') {
                $mail->SMTPSecure = PHPMailer::ENCRYPTION_SMTPS;
            } elseif ($secure === 'tls') {
                $mail->SMTPSecure = PHPMailer::ENCRYPTION_STARTTLS;
            }

            $mail->Port = $this->smtpConfig['port'] ?? 587;
            $mail->Timeout = $this->smtpConfig['timeout'] ?? 30;

            // Recipients
            $fromEmail = $this->smtpConfig['from']['email'] ?? $this->smtpConfig['from'] ?? 'noreply@example.org';
            $fromName = $this->smtpConfig['from']['name'] ?? 'LORIS Pipeline';
            $mail->setFrom($fromEmail, $fromName);

            foreach ($recipients as $recipient) {
                $mail->addAddress($recipient);
            }

            // Content
            $mail->isHTML(true);
            $mail->Subject = $subject;
            $mail->Body    = $body;
            $mail->AltBody = strip_tags($body);

            // Priority for error emails
            if ($isError) {
                $mail->Priority = 1; // High priority
            }

            $mail->send();
            $this->logger->info("Email sent successfully to: " . implode(', ', $recipients));
            return true;

        } catch (Exception $e) {
            $this->logger->error("Failed to send notification via SMTP: " . $mail->ErrorInfo);
            return false;
        }
    }

    /**
     * Test SMTP configuration or native mail setup
     */
    public function testConnection(): bool
    {
        // Check if using native mail
        $useNativeMail = $this->smtpConfig['use_native_mail'] ?? false;

        if ($useNativeMail || empty($this->smtpConfig['host'])) {
            $this->logger->info("✓ Using native PHP mail() function (server email config)");
            return true;
        }

        // Test SMTP connection
        try {
            $mail = new PHPMailer(true);
            $mail->isSMTP();
            $mail->Host = $this->smtpConfig['host'];
            $mail->SMTPAuth = !empty($this->smtpConfig['username']);
            $mail->Username = $this->smtpConfig['username'] ?? '';
            $mail->Password = $this->smtpConfig['password'] ?? '';

            $secure = strtolower($this->smtpConfig['secure'] ?? 'tls');
            if ($secure === 'ssl') {
                $mail->SMTPSecure = PHPMailer::ENCRYPTION_SMTPS;
            } elseif ($secure === 'tls') {
                $mail->SMTPSecure = PHPMailer::ENCRYPTION_STARTTLS;
            }

            $mail->Port = $this->smtpConfig['port'] ?? 587;
            $mail->Timeout = $this->smtpConfig['timeout'] ?? 30;

            // Try to connect
            $mail->smtpConnect();
            $this->logger->info("✓ SMTP connection successful");
            return true;

        } catch (Exception $e) {
            $this->logger->error("✗ SMTP connection failed: " . $e->getMessage());
            return false;
        }
    }
}
