<?php

/**
 * BundleSupport
 *
 * What every modality's bundle script does the same way (DICOM, PNG, and the
 * next modality):
 *
 *   runStep()              run one step script as a subprocess: stream its
 *                          output to the console and the bundle log, treat a
 *                          printed abort (unresolvable host, refused
 *                          connection, TLS, failed authentication, "aborting")
 *                          as a failure even when it exits 0, and pass the
 *                          project lock on to it (ProjectLock, fd 3)
 *   stopBeforeIngestion()  the stop rule: a preparation step failed, nothing
 *                          is ingested for the project; FAILED email to the
 *                          project's notification_emails.<modality>.on_error
 *   confirmPrompt()        ask yes/no on a terminal, never assume yes unattended
 *   masterLog()            append to the bundle log
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

final class BundleSupport
{
    /**
     * Output that means a step aborted, even when it exits 0: some steps stop
     * before doing any work (unresolvable host, failed auth, unreachable
     * mount) and their failure counters only cover what they examined.
     */
    private const ABORT_SIGNALS = [
        'Could not resolve host'  => 'the LORIS host name does not resolve',
        'Connection refused'      => 'the LORIS host refused the connection',
        'Connection timed out'    => 'the LORIS host did not respond',
        'SSL certificate problem' => 'the LORIS TLS certificate was rejected',
        'Authentication failed'   => 'LORIS authentication failed',
        'aborting'                => 'the step aborted',
    ];

    /**
     * Append a line to the bundle log (null: no log).
     *
     * The bundle keeps its own log of the orchestration - which steps ran,
     * their exit codes, how prompts were answered - and a copy of each step's
     * output: the one file that shows a whole run.
     */
    public static function masterLog(?string $path, string $line): void
    {
        if ($path === null) {
            return;
        }
        @file_put_contents($path, sprintf("[%s] %s\n", date('Y-m-d H:i:s'), rtrim($line, "\r\n")), FILE_APPEND);
    }

    /**
     * Run one step script, streaming its output to the console and the bundle
     * log. A step that printed an abort signature but exited 0 counts as
     * failed. stdin is /dev/null: a step never waits for input in a cron run.
     *
     * @param string       $scriptDir Folder of the step scripts.
     * @param string       $script    Step script file name.
     * @param string[]     $args      Arguments.
     * @param string|null  $log       Bundle log, or null to only stream.
     * @param ?ProjectLock $lock      Project lock to hand to the step (fd 3).
     *
     * @return int Exit code.
     */
    public static function runStep(string $scriptDir, string $script, array $args, ?string $log, ?ProjectLock $lock = null): int
    {
        $path = rtrim($scriptDir, '/') . '/' . $script;
        if (!is_file($path)) {
            fwrite(STDERR, "  ERROR step script not found: {$path}\n");
            self::masterLog($log, "ERROR step script not found: {$path}");
            return PipelineSetup::EXIT_FAILURE;
        }

        $cmd = 'php ' . escapeshellarg($path);
        foreach ($args as $a) {
            $cmd .= ' ' . escapeshellarg($a);
        }
        fwrite(STDERR, "--- {$script}\n");
        self::masterLog($log, "=== BEGIN {$script}");
        self::masterLog($log, "    {$cmd}");

        // The step works under the bundle's project lock, passed on as fd 3.
        [$extra, $env] = $lock !== null ? $lock->childDescriptor() : [[], null];
        $proc = proc_open($cmd, [0 => ['file', '/dev/null', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']] + $extra, $pipes, null, $env);
        if (!is_resource($proc)) {
            fwrite(STDERR, "  ERROR could not start {$script}\n");
            self::masterLog($log, "ERROR could not start {$script}");
            return PipelineSetup::EXIT_FAILURE;
        }

        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);
        $abort = null;
        $buf   = [1 => '', 2 => ''];
        while (!feof($pipes[1]) || !feof($pipes[2])) {
            $read = [$pipes[1], $pipes[2]];
            $w = $e = null;
            if (stream_select($read, $w, $e, 1) === false) {
                break;
            }
            foreach ($read as $p) {
                $i     = $p === $pipes[1] ? 1 : 2;
                $chunk = fread($p, 8192);
                if ($chunk === false || $chunk === '') {
                    continue;
                }
                $buf[$i] .= $chunk;
                // Whole lines only, so the bundle log is not fragmented.
                while (($nl = strpos($buf[$i], "\n")) !== false) {
                    $line    = substr($buf[$i], 0, $nl);
                    $buf[$i] = substr($buf[$i], $nl + 1);
                    fwrite(STDERR, $line . "\n");
                    self::masterLog($log, '  ' . $line);
                    foreach (self::ABORT_SIGNALS as $needle => $meaning) {
                        if ($abort === null && stripos($line, $needle) !== false) {
                            $abort = $meaning;
                        }
                    }
                }
            }
        }
        foreach ($buf as $rest) {
            if ($rest !== '') {
                fwrite(STDERR, $rest . "\n");
                self::masterLog($log, '  ' . $rest);
            }
        }
        fclose($pipes[1]);
        fclose($pipes[2]);
        $code = proc_close($proc);
        self::masterLog($log, "=== END {$script} (exit {$code})");

        // The printed abort is authoritative over exit 0: otherwise the chain
        // goes on and the run reports success having ingested nothing.
        if ($code === PipelineSetup::EXIT_OK && $abort !== null) {
            fwrite(STDERR, "\n  {$script} reported an abort ({$abort}) but exited 0 - treating as failed.\n");
            self::masterLog($log, "OVERRIDE {$script} exited 0 but aborted: {$abort}");
            return PipelineSetup::EXIT_FAILURE;
        }
        return $code;
    }

    /**
     * Stop a project before ingestion because a preparation step failed.
     *
     * Participant sync and reidentification are all-or-nothing gates: when
     * one of them fails, NOTHING is ingested for the project - not even what
     * did succeed - so a participant never ends up half in LORIS while the run
     * looks finished. Everything before the import is reversible, so the next
     * run picks up exactly here. On a --confirm run the project's error
     * recipients get a FAILED email (the import, which sends the summary,
     * does not run).
     *
     * @param string $modality dicom, png ... (notification_emails key; upper-cased in the email)
     * @param string $items    What the modality ingests, plural: "studies", "images".
     */
    public static function stopBeforeIngestion(
        string $label,
        string $step,
        string $projectPath,
        array $config,
        ?string $log,
        bool $confirm,
        string $modality,
        string $items
    ): void {
        $msg = "{$step} failed - INGESTION ABORTED for {$label}. Nothing was sent to LORIS for this project, "
            . "including the {$items} that did succeed. " . ucfirst($items) . ' already in LORIS are unchanged. '
            . 'Fix the error above and re-run the bundle; only what is still missing will be processed.';
        fwrite(STDERR, "\n  STOP {$msg}\n");
        self::masterLog($log, "STOP {$msg}");
        if (!$confirm) {
            return;
        }

        $project = PipelineSetup::projectConfig($projectPath) ?? [];
        $notif   = $project['notification_emails'][$modality] ?? $config['notification_emails'][$modality]
            ?? $config['notification_defaults'] ?? [];
        if (!($notif['enabled'] ?? false)) {
            return;
        }
        $to = $notif['on_error'] ?? $notif['default_on_error'] ?? $config['notification_defaults']['default_on_error'] ?? [];
        $name = basename($projectPath);
        $Mod  = strtoupper($modality);
        $body = "Project: {$name} ({$modality})\n"
            . "Status:  FAILED - stopped before ingestion\n"
            . "Step:    {$step}\n\n"
            . "Nothing was ingested for this project in this run.\n"
            . ucfirst($items) . " already in LORIS are unchanged. Fix the error and re-run.\n"
            . ($log ? "\nBundle log: {$log}\n" : '');
        $mailer = new \LORIS\Utils\Notification();
        foreach ((array) $to as $address) {
            $mailer->send((string) $address, "FAILED: {$name} {$Mod} Import (stopped at {$step})", $body);
        }
    }

    /**
     * Ask yes/no on a terminal. Never assumes yes when stdin is not a
     * terminal: an unattended run must pass $flag explicitly.
     *
     * @param string $question  The question.
     * @param bool   $assumeYes $flag was given.
     * @param string $flag      The option that answers yes, e.g. --continue-good.
     */
    public static function confirmPrompt(string $question, bool $assumeYes, string $flag): bool
    {
        if ($assumeYes) {
            fwrite(STDERR, "{$question} [yes - answered by {$flag}]\n");
            return true;
        }
        if (!stream_isatty(STDIN)) {
            fwrite(STDERR, "{$question}\n  stdin is not a terminal and {$flag} was not given - not continued.\n");
            return false;
        }
        fwrite(STDERR, "{$question} [y/N]: ");
        $answer = strtolower(trim((string) fgets(STDIN)));
        return $answer === 'y' || $answer === 'yes';
    }
}
