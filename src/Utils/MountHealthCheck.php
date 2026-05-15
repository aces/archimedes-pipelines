<?php
declare(strict_types=1);

namespace LORIS\Utils;

use Psr\Log\LoggerInterface;
use LORIS\Utils\Notification;

/**
 * Bounded filesystem reachability check with optional failure
 * notification.
 *
 * Why this exists:
 *   When an NFS or remote mount becomes unresponsive (server down,
 *   network glitch, stale handle), filesystem syscalls block at the
 *   KERNEL level — uninterruptibly. PHP-level timeouts and signal
 *   handlers cannot escape them. The only reliable way to bound a
 *   filesystem call is to run it in a child process and kill that
 *   process from outside.
 *
 *   `timeout 10 stat /path` runs stat in a child, SIGKILLs it after
 *   10 seconds if it has not returned, and surfaces exit code 124.
 *   The hung syscall in the child stays the kernel's problem; the
 *   pipeline process stays free.
 *
 * Public API:
 *
 *   guardOrReport($path, $config, $logger, $context)
 *     One-shot for pipelines: check, on failure log + email + dedup.
 *     Returns true if responsive (caller continues), false if not
 *     (caller bails). Recipients come from
 *     $config['notification_defaults']['default_on_error'].
 *
 *   isResponsive($path)         — bool yes/no
 *   checkDetailed($path)        — {responsive, reason, exit_code, elapsed}
 *   formatFailureMessage($p, $d)— standardized message string
 *
 * Usage from a pipeline:
 *
 *   use LORIS\Utils\MountHealthCheck;
 *
 *   if (!MountHealthCheck::guardOrReport(
 *       $mountPath,
 *       $this->config,
 *       $this->logger,
 *       "Clinical pipeline / project {$name}"
 *   )) {
 *       return;
 *   }
 *
 * All pipelines (Clinical, BIDS, ParticipantMetadata, DicomImport)
 * use the same call shape so timeout, error semantics, log wording,
 * and email format stay consistent across the codebase.
 *
 * @package LORIS\Utils
 */
final class MountHealthCheck
{
    /** Default seconds to wait before declaring the mount hung. */
    public const DEFAULT_TIMEOUT_SECONDS = 10;

    /** Exit code returned by timeout(1) when it had to SIGKILL the child. */
    private const TIMEOUT_KILLED = 124;

    /**
     * Per-process registry of mount paths we've already emailed about.
     * Keeps one hung mount affecting N collections to ONE email per
     * pipeline run, not N.
     */
    private static array $emailedFailures = [];

    // ──────────────────────────────────────────────────────────────────
    // High-level entry point
    // ──────────────────────────────────────────────────────────────────

    /**
     * Check the mount; on failure, log + email + dedup; return whether
     * the caller should proceed.
     *
     * Email behaviour:
     *   - Recipients read from
     *     $config['notification_defaults']['default_on_error'].
     *   - If recipient list is empty, email is skipped with warning.
     *   - Notifications are sent using LORIS\Utils\Notification.
     *   - One email per unique mount path per process.
     *
     * @param string $mountPath Mount path to check.
     * @param array $config Full config array.
     * @param LoggerInterface $logger Logger instance.
     * @param string $context Pipeline/project context label.
     *
     * @return bool
     */
    public static function guardOrReport(
        string $mountPath,
        array $config,
        LoggerInterface $logger,
        string $context
    ): bool {
        $detail = self::checkDetailed($mountPath);

        if ($detail['responsive']) {
            return true;
        }

        $logger->error(
            "{$context} — "
            . self::formatFailureMessage($mountPath, $detail)
        );

        // De-dup: one email per mount per run.
        if (isset(self::$emailedFailures[$mountPath])) {
            $logger->info(
                "{$context} — mount {$mountPath} already emailed earlier "
                . "in this run, skipping duplicate notification"
            );

            return false;
        }

        self::sendFailureEmail(
            $mountPath,
            $detail,
            $config,
            $context,
            $logger
        );

        self::$emailedFailures[$mountPath] = true;

        return false;
    }

    /**
     * Reset dedup registry (mainly useful for tests).
     */
    public static function resetDedup(): void
    {
        self::$emailedFailures = [];
    }

    // ──────────────────────────────────────────────────────────────────
    // Low-level check API
    // ──────────────────────────────────────────────────────────────────

    /**
     * Simple yes/no responsiveness check.
     */
    public static function isResponsive(
        string $path,
        int $timeoutSeconds = self::DEFAULT_TIMEOUT_SECONDS
    ): bool {
        return self::checkDetailed(
            $path,
            $timeoutSeconds
        )['responsive'];
    }

    /**
     * Detailed mount check.
     *
     * @return array{
     *     responsive: bool,
     *     reason: string,
     *     exit_code: int,
     *     elapsed: float
     * }
     */
    public static function checkDetailed(
        string $path,
        int $timeoutSeconds = self::DEFAULT_TIMEOUT_SECONDS
    ): array {
        $cmd = sprintf(
            'timeout %d stat %s > /dev/null 2>&1',
            $timeoutSeconds,
            escapeshellarg($path)
        );

        $t0       = microtime(true);
        $exitCode = 0;

        exec($cmd, $_, $exitCode);

        $elapsed = round(microtime(true) - $t0, 2);

        return [
            'responsive' => $exitCode === 0,
            'reason'     => self::reasonFromExitCode($exitCode),
            'exit_code'  => $exitCode,
            'elapsed'    => $elapsed,
        ];
    }

    /**
     * Standardized failure message.
     */
    public static function formatFailureMessage(
        string $path,
        array $detail
    ): string {
        $mountTag = basename(dirname($path)) ?: 'mount';
        $reason   = $detail['reason'];
        $elapsed  = $detail['elapsed'];
        $code     = $detail['exit_code'];

        $headline = match ($reason) {
            'hung' => "MOUNT HUNG: {$path} did not respond within "
                . "{$elapsed}s (SIGKILLed by timeout(1), exit 124)",

            'missing' => "MOUNT INACCESSIBLE: {$path} responded but "
                . "the path does not exist (stat exit 1)",

            default => "MOUNT FAILED: {$path} returned unexpected "
                . "exit code {$code} after {$elapsed}s",
        };

        $remedy = match ($reason) {
            'hung' => "Check NFS server and `mount | grep "
                . escapeshellarg($mountTag)
                . "`. If the server is up, the mount may be stale — "
                . "umount -f -l "
                . escapeshellarg($path)
                . " and remount.",

            'missing' => "Verify the project path exists and the "
                . "pipeline user has read access.",

            default => "Check `dmesg` and `/var/log/syslog` for "
                . "filesystem or network errors.",
        };

        return "{$headline}. {$remedy}";
    }

    // ──────────────────────────────────────────────────────────────────
    // Internals
    // ──────────────────────────────────────────────────────────────────

    /**
     * Send mount failure notification email.
     */
    private static function sendFailureEmail(
        string $mountPath,
        array $detail,
        array $config,
        string $context,
        LoggerInterface $logger
    ): void {
        $recipients = $config['notification_defaults']['default_on_error']
            ?? [];

        if (empty($recipients)) {
            $logger->warning(
                "Mount {$mountPath} failed but "
                . "notification_defaults.default_on_error "
                . "is not configured — no email will be sent. "
                . "Operator must read stderr/cron output to see this."
            );

            return;
        }

        $host    = gethostname() ?: 'unknown-host';
        $subject = "MOUNT FAILURE on {$host}: {$mountPath}";

        $body  = "A pipeline could not access a data mount.\n\n";

        $body .= "Host         : {$host}\n";
        $body .= "Context      : {$context}\n";
        $body .= "Mount path   : {$mountPath}\n";
        $body .= "Reason       : {$detail['reason']}\n";
        $body .= "Exit code    : {$detail['exit_code']}\n";
        $body .= "Elapsed      : {$detail['elapsed']}s\n";
        $body .= "Detected at  : "
            . date('Y-m-d H:i:s T')
            . "\n\n";

        $body .= "Details:\n";
        $body .= "  "
            . self::formatFailureMessage($mountPath, $detail)
            . "\n\n";

        $body .= "Impact:\n";
        $body .= "  - The pipeline aborted processing for this "
            . "collection/project.\n";
        $body .= "  - No LORIS writes were attempted. "
            . "Tracking state untouched.\n";
        $body .= "  - Other collections on different mounts "
            . "may have processed normally.\n\n";

        $body .= "Action required:\n";
        $body .= "  1. SSH to {$host}\n";
        $body .= "  2. Check the mount:\n";
        $body .= "       mount | grep "
            . escapeshellarg(
                basename(dirname($mountPath))
            )
            . "\n";

        $body .= "  3. If stale, force-unmount and remount:\n";
        $body .= "       sudo umount -f -l "
            . escapeshellarg($mountPath)
            . "\n";

        $body .= "       sudo mount "
            . escapeshellarg($mountPath)
            . "\n";

        $body .= "  4. Verify with diagnostic before re-running:\n";
        $body .= "       php scripts/test_mount_check.php "
            . escapeshellarg($mountPath)
            . "\n";

        $logger->info(
            "Sending mount-failure notification to: "
            . implode(', ', $recipients)
        );

        $notification = new Notification();

        foreach ($recipients as $to) {
            $ok = $notification->send(
                $to,
                $subject,
                $body
            );

            if (!$ok) {
                $logger->error(
                    "Local MTA rejected mount-failure message "
                    . "for {$to}"
                );
            }
        }
    }

    /**
     * Map shell exit code to stable reason labels.
     */
    private static function reasonFromExitCode(int $code): string
    {
        return match ($code) {
            0                    => 'ok',
            self::TIMEOUT_KILLED => 'hung',
            1                    => 'missing',
            default              => 'unknown_error',
        };
    }
}