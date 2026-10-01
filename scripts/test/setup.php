<?php
/**
 * Mail smoke test for this host.
 *
 * Every pipeline sends email with PHP mail(), so this host needs a
 * working local MTA (postfix/sendmail, see `php -i | grep sendmail_path`).
 * notifications.smtp in loris_client_config.json is NOT used by the
 * pipelines.
 *
 * Usage:
 *   php scripts/test/setup.php                 # mails notification_defaults.default_on_error
 *   php scripts/test/setup.php you@example.org # mails one address
 */

require __DIR__ . '/../../vendor/autoload.php';

use LORIS\Utils\Notification;

$configFile = __DIR__ . '/../../config/loris_client_config.json';
$config     = is_file($configFile) ? (json_decode((string) file_get_contents($configFile), true) ?: []) : [];

$to = isset($argv[1])
    ? [$argv[1]]
    : ($config['notification_defaults']['default_on_error'] ?? []);

if ($to === []) {
    fwrite(STDERR, "No recipient: pass an address, or set notification_defaults.default_on_error\n");
    exit(1);
}

$sendmail = ini_get('sendmail_path') ?: '(not set)';
echo "sendmail_path: {$sendmail}\n";

$n  = new Notification();
$ok = true;
foreach ($to as $addr) {
    $sent = $n->send($addr, 'ARCHIMEDES pipelines: mail test from ' . gethostname(),
        "If you can read this, PHP mail() on this host works.\n");
    echo ($sent ? 'SENT   ' : 'FAILED ') . $addr . "\n";
    $ok = $ok && $sent;
}

echo $ok
    ? "\nmail() accepted the message(s). Check the inbox (and spam).\n"
    : "\nmail() refused. Check the MTA and sendmail_path.\n";
exit($ok ? 0 : 1);
