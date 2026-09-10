#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * Diagnostic: can we email an attachment from this host?
 *
 * Sends TWO emails so you can compare what arrives:
 *   1. Via LORIS\Utils\Notification::send() — the existing helper
 *   2. Via PHP mail() directly              — hand-rolled multipart
 *
 * Email #1 is the control — shows whatever Notification does today.
 * Email #2 is the proposed path — confirms mail() can carry an
 * attachment on this host before we wire it into ClinicalPipeline
 * for the EviData failure-notification flow.
 *
 * Usage:
 *   php scripts/test_mail_attachment.php you@example.com
 */

require_once __DIR__ . '/../vendor/autoload.php';

// ── Args ─────────────────────────────────────────────────────────────
if ($argc < 2 || !filter_var($argv[1], FILTER_VALIDATE_EMAIL)) {
    fwrite(STDERR, "Usage: php {$argv[0]} <recipient-email>\n");
    exit(1);
}
$recipient = $argv[1];
$marker    = 'attach_test_' . date('YmdHis') . '_' . bin2hex(random_bytes(3));

echo "Recipient    : {$recipient}\n";
echo "Hostname     : " . gethostname() . "\n";
echo "PHP version  : " . PHP_VERSION . "\n";
echo "Time         : " . date('Y-m-d H:i:s T') . "\n";
echo "Marker       : {$marker}\n";
echo "\n";

// ── Build a tiny test ZIP to attach ──────────────────────────────────
$zipPath = sys_get_temp_dir() . "/notif_test_{$marker}.zip";
$zip     = new ZipArchive();
if ($zip->open($zipPath, ZipArchive::CREATE | ZipArchive::OVERWRITE) !== true) {
    fwrite(STDERR, "Could not create test ZIP at {$zipPath}\n");
    exit(1);
}
$zip->addFromString(
    'hello.txt',
    "Notification attachment test\n"
    . "Marker: {$marker}\n"
    . "If you see this, the ZIP round-tripped intact.\n"
);
$zip->close();

$zipSize = filesize($zipPath);
echo "Test ZIP     : {$zipPath} ({$zipSize} bytes)\n";
echo "\n";

// ════════════════════════════════════════════════════════════════════
//  Test 1 — LORIS\Utils\Notification::send()
// ════════════════════════════════════════════════════════════════════

echo "── Test 1: LORIS\\Utils\\Notification::send() ──\n";
try {
    $notification = new \LORIS\Utils\Notification();
    $ok = $notification->send(
        $recipient,
        "TEST 1 (Notification): {$marker}",
        "This was sent via LORIS\\Utils\\Notification::send().\n\n"
        . "Marker: {$marker}\n\n"
        . "If this email has no attachment, Notification doesn't support\n"
        . "them today, which means we need the direct-mail() bypass\n"
        . "shown in Test 2 to attach EviData report ZIPs."
    );
    echo "  send() returned: " . var_export($ok, true) . "\n";
} catch (\Throwable $e) {
    echo "  EXCEPTION: " . $e->getMessage() . "\n";
}
echo "\n";

// ════════════════════════════════════════════════════════════════════
//  Test 2 — direct mail() with a multipart attachment
// ════════════════════════════════════════════════════════════════════

echo "── Test 2: PHP mail() with multipart attachment ──\n";

$boundary = '=_test_' . md5(uniqid('', true));
$from     = 'pipeline-test@' . (gethostname() ?: 'localhost');

$headers  = "From: {$from}\r\n"
    . "MIME-Version: 1.0\r\n"
    . "Content-Type: multipart/mixed; boundary=\"{$boundary}\"\r\n";

$body = "This was sent via PHP mail() with a hand-rolled multipart body.\n\n"
    . "Marker: {$marker}\n\n"
    . "If this email has an attachment named 'test.zip' and the ZIP opens\n"
    . "to show hello.txt with the marker above, mail() can carry\n"
    . "attachments from this host. That's all we need to wire EviData\n"
    . "report ZIPs into the failure-notification path.";

// Part 1: plain-text body.
$message  = "--{$boundary}\r\n"
    . "Content-Type: text/plain; charset=UTF-8\r\n"
    . "Content-Transfer-Encoding: 8bit\r\n\r\n"
    . $body . "\r\n";

// Part 2: the ZIP attachment, base64-encoded with 76-char line wrap.
$zipBytes = file_get_contents($zipPath);
if ($zipBytes === false) {
    fwrite(STDERR, "Could not read test ZIP back from disk\n");
    @unlink($zipPath);
    exit(1);
}
$encoded  = chunk_split(base64_encode($zipBytes), 76, "\r\n");

$message .= "--{$boundary}\r\n"
    . "Content-Type: application/zip; name=\"test.zip\"\r\n"
    . "Content-Transfer-Encoding: base64\r\n"
    . "Content-Disposition: attachment; filename=\"test.zip\"\r\n\r\n"
    . $encoded . "\r\n";

$message .= "--{$boundary}--\r\n";

$ok = mail(
    $recipient,
    "TEST 2 (mail+attach): {$marker}",
    $message,
    $headers
);

echo "  mail() returned: " . var_export($ok, true) . "\n";
echo "  Boundary       : {$boundary}\n";
echo "  Body bytes     : " . strlen($body) . "\n";
echo "  ZIP bytes      : {$zipSize} (base64: " . strlen($encoded) . ")\n";
echo "  Total bytes    : " . strlen($message) . "\n";
echo "\n";

// ── Cleanup ─────────────────────────────────────────────────────────
@unlink($zipPath);

// ── Summary ─────────────────────────────────────────────────────────
echo "════════════════════════════════════════════════════════════════\n";
echo "Both messages dispatched. Check {$recipient} (and spam) for:\n";
echo "  - TEST 1 (Notification): {$marker}\n";
echo "  - TEST 2 (mail+attach):  {$marker}\n";
echo "════════════════════════════════════════════════════════════════\n";
