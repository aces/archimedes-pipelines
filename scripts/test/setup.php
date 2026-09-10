<?php
/**
 * Test SMTP Connection
 *
 * Usage: php test_smtp.php
 */

require 'vendor/autoload.php';

use LORIS\Client\Utils\Notification;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

// Load configuration
$configFile = 'config/loris_client_config.json';
if (!file_exists($configFile)) {
    echo "❌ config.json not found!\n";
    echo "Please create config.json with SMTP settings.\n";
    exit(1);
}

$config = json_decode(file_get_contents($configFile), true);
if (json_last_error() !== JSON_ERROR_NONE) {
    echo "❌ Invalid JSON in config.json: " . json_last_error_msg() . "\n";
    exit(1);
}

// Check if notifications are configured
if (!isset($config['notifications'])) {
    echo "❌ No 'notifications' section in config.json\n";
    exit(1);
}

if (!isset($config['notifications']['smtp'])) {
    echo "❌ No 'smtp' configuration in config.json\n";
    exit(1);
}

// Create logger
$logger = new Logger('smtp-test');
$logger->pushHandler(new StreamHandler('php://stdout', Logger::DEBUG));

echo "=====================================\n";
echo "SMTP Connection Test\n";
echo "=====================================\n\n";

echo "Configuration:\n";

// Check if using native mail
$useNativeMail = $config['notifications']['smtp']['use_native_mail'] ?? false;
$hasHost = !empty($config['notifications']['smtp']['host'] ?? '');

if ($useNativeMail || !$hasHost) {
    echo "  Method: Native PHP mail() function (server email)\n";
    echo "  From: " . (
        isset($config['notifications']['smtp']['from']['email'])
            ? $config['notifications']['smtp']['from']['email']
            : ($config['notifications']['smtp']['from'] ?? 'NOT SET')
        ) . "\n\n";
} else {
    echo "  Method: SMTP\n";
    echo "  Host: " . ($config['notifications']['smtp']['host'] ?? 'NOT SET') . "\n";
    echo "  Port: " . ($config['notifications']['smtp']['port'] ?? 'NOT SET') . "\n";
    echo "  Secure: " . ($config['notifications']['smtp']['secure'] ?? 'NOT SET') . "\n";
    echo "  Username: " . ($config['notifications']['smtp']['username'] ?? 'NOT SET') . "\n";
    echo "  From: " . (
        isset($config['notifications']['smtp']['from']['email'])
            ? $config['notifications']['smtp']['from']['email']
            : ($config['notifications']['smtp']['from'] ?? 'NOT SET')
        ) . "\n\n";
}

// Test connection
$notification = new Notification($config, $logger);

echo "Testing SMTP connection...\n";
if ($notification->testConnection()) {
    echo "\n✅ SUCCESS! SMTP configuration is working!\n\n";
    echo "You can now send test emails with: php send_test_email.php\n";
    exit(0);
} else {
    echo "\n❌ FAILED! SMTP connection failed!\n\n";
    echo "Common issues:\n";
    echo "  - Check username/password (use app password for Gmail)\n";
    echo "  - Verify port is correct (587 for TLS, 465 for SSL)\n";
    echo "  - Check if firewall blocks SMTP ports\n";
    echo "  - For Gmail: Enable 2-Step Verification and create App Password\n";
    exit(1);
}