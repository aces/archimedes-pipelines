#!/usr/bin/env php
<?php
declare(strict_types=1);

/**
 * Quick sanity-test for the MountHealthCheck utility.
 *
 * Usage:
 *   php scripts/test_mount_check.php /data/archimedes
 *   php scripts/test_mount_check.php /data/nonexistent
 *
 * Useful to:
 *   - Confirm timeout(1) is available on this host
 *   - See what a real "responsive" / "missing" outcome looks like
 *   - If you can simulate a hung NFS mount, see the SIGKILL path
 *
 * Exits 0 if responsive, 1 otherwise.
 */

require __DIR__ . '/../vendor/autoload.php';

use LORIS\Utils\MountHealthCheck;

$path = $argv[1] ?? '/data';

echo "── Mount health check ──\n";
echo "Path     : {$path}\n";
echo "Timeout  : " . MountHealthCheck::DEFAULT_TIMEOUT_SECONDS . "s\n";
echo str_repeat('─', 64) . "\n\n";

$detail = MountHealthCheck::checkDetailed($path);

echo "Responsive : " . ($detail['responsive'] ? 'YES' : 'NO') . "\n";
echo "Reason     : {$detail['reason']}\n";
echo "Exit code  : {$detail['exit_code']}\n";
echo "Elapsed    : {$detail['elapsed']}s\n";

if (!$detail['responsive']) {
    echo "\nFailure message:\n  ";
    echo MountHealthCheck::formatFailureMessage($path, $detail) . "\n";
    exit(1);
}

echo "\n✓ Mount responded cleanly.\n";
exit(0);
