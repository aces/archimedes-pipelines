<?php
/**
 * Bare mail() check, no autoloader needed.
 *
 * Usage: php scripts/test/validate_projects.php you@example.org
 */
$to = $argv[1] ?? '';
if (!filter_var($to, FILTER_VALIDATE_EMAIL)) {
    fwrite(STDERR, "Usage: php {$argv[0]} <recipient-email>\n");
    exit(1);
}
$ok = mail($to, 'Mail Test', 'This is a test mail using native mail().', 'From: noreply@' . (gethostname() ?: 'localhost'));
echo $ok ? "mail() says: SENT\n" : "mail() says: FAILED\n";
exit($ok ? 0 : 1);
