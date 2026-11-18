<?php

/**
 * ProjectLock
 *
 * One run per project folder at a time, for every pipeline: a lock file in the
 * project folder, .<modality>_pipeline.lock (fileFor()), non-blocking flock, so a second
 * run is skipped at once instead of waiting; the OS releases the lock when the
 * process ends, even on a crash, so it is never left stale. The file holds a
 * note of who has it (script, pid, host, since) for the "skipped" message.
 *
 *   enter()            the per-project step every script runs: release the
 *                      previous project's lock, take this one's (real runs
 *                      only), with the mount pre-flight first for a bundle
 *   acquire()          take the lock, or say who has it
 *   childDescriptor()  hand the lock to a step script a bundle starts (fd 3,
 *                      checked to be the same file by device + inode, so
 *                      nothing else can borrow it)
 *   probe()            pre-flight: does locking really protect this folder?
 *                      (creatable, takeable, ENFORCED, and the mount type:
 *                      local-only locks on NFS nolock/local_lock, CIFS nobrl,
 *                      FUSE are a warning)
 *
 * Used by every pipeline: the clinical pipeline, and the DICOM, BIDS and PNG
 * scripts and bundles (.clinical_pipeline.lock, .dicom_pipeline.lock ...).
 *
 * PHP Version 8.1
 *
 * @package LORIS\Pipelines
 */

declare(strict_types=1);

namespace LORIS\Pipelines\Shared;

final class ProjectLock
{

    /** Environment variable a bundle sets for its step scripts: the lock file path. */
    private const ENV_PATH = 'ARCHIMEDES_PROJECT_LOCK_PATH';

    /** File descriptor the lock handle is passed on to a step script. */
    private const CHILD_FD = 3;

    /** @var resource */
    private $handle;

    private bool $inherited;

    /**
     * @param resource $handle
     */
    private function __construct($handle, private string $path, bool $inherited)
    {
        $this->handle    = $handle;
        $this->inherited = $inherited;
    }

    public function __destruct()
    {
        $this->release();
    }

    /**
     * Lock file of a modality's pipeline in a project folder.
     */
    public static function fileFor(string $modality): string
    {
        return '.' . strtolower($modality) . '_pipeline.lock';
    }

    /**
     * The per-project lock step of a script's project loop, in one call:
     * release the lock held for the previous project, then - on a real run
     * only, a dry run changes nothing and takes no lock - take this project's.
     * A bundle passes $preflight: the mount is checked first (probe()), a
     * FAIL stops the project, a WARN is printed.
     *
     * Prints why a project is skipped; the caller counts it as failed.
     *
     * @param ?ProjectLock $held        Lock held for the previous project (updated).
     * @param string       $projectPath Project folder.
     * @param string       $modality    dicom, bids, png ...
     * @param string       $script      Script name, for the "held by" note.
     * @param bool         $realRun     False for a dry run.
     * @param bool         $preflight   Check the mount first (bundles).
     * @param string       $label       Prefix for the messages, e.g. collection/project.
     *
     * @return bool True: go on with the project. False: skip it.
     */
    public static function enter(
        ?ProjectLock &$held,
        string $projectPath,
        string $modality,
        string $script,
        bool $realRun,
        bool $preflight = false,
        string $label = ''
    ): bool {
        $held?->release();
        $held = null;
        if (!$realRun) {
            return true;
        }

        $who = $label !== '' ? "{$label}: " : '';
        if ($preflight) {
            $probe = self::probe($projectPath, $modality);
            if ($probe['status'] === 'FAIL') {
                fwrite(STDERR, "  STOP {$who}project lock pre-flight failed [{$probe['mount']}]: {$probe['message']}\n");
                return false;
            }
            if ($probe['status'] === 'WARN') {
                fwrite(STDERR, "  WARN {$who}{$probe['message']}\n");
            }
        }

        [$held, $busy] = self::acquire($projectPath, $modality, $script);
        if ($held === null) {
            fwrite(STDERR, "  SKIP {$who}{$busy}\n");
            return false;
        }
        return true;
    }

    /**
     * Take the project lock, or say who has it.
     *
     * @return array{0: ?ProjectLock, 1: ?string} [lock, null] or [null, why it is busy]
     */
    public static function acquire(string $projectPath, string $modality, string $script): array
    {
        $path = rtrim($projectPath, '/') . '/' . self::fileFor($modality);
        $what = strtoupper($modality) . ' run';

        // Started by the bundle for this project: use the handle it passed on.
        if (getenv(self::ENV_PATH) === $path) {
            $h = @fopen('php://fd/' . self::CHILD_FD, 'r+');
            // Only the project's own lock file counts (same device + inode):
            // any other descriptor on fd 3 (/dev/null, a pipe, another file)
            // could be "locked" too and would let a run in unguarded.
            $mine = $h !== false && ($fs = @fstat($h)) !== false && ($ps = @stat($path)) !== false
                && $fs['dev'] === $ps['dev'] && $fs['ino'] === $ps['ino'];
            if ($mine && flock($h, LOCK_EX | LOCK_NB)) {
                return [new self($h, $path, true), null];
            }
            // Not really handed over (fd 3 is not the lock): fall through and
            // try for the lock like any other run.
            if ($h !== false) {
                fclose($h);
            }
        }

        $h = @fopen($path, 'c+');
        if ($h === false) {
            return [null, "cannot open {$path} (permissions?) - not run, so it cannot overlap another run"];
        }
        if (!flock($h, LOCK_EX | LOCK_NB)) {
            $holder = trim((string) stream_get_contents($h, 500, 0));
            fclose($h);
            return [null, $holder !== ''
                ? "another {$what} is in progress for this project ({$holder}) - skipped, nothing done; "
                    . 're-run when it has finished'
                : "the project lock cannot be taken: another {$what} is starting, or file locking does not "
                    . 'work on this mount (ProjectLock::probe() checks it) - skipped, '
                    . 'nothing done'];
        }

        // Who holds it, for the message the next run prints.
        ftruncate($h, 0);
        fwrite($h, sprintf('%s, pid %d, host %s, since %s', $script, getmypid(), gethostname() ?: '?', date('c')));
        fflush($h);

        return [new self($h, $path, false), null];
    }

    /**
     * Pre-flight: does locking really protect this project folder?
     *
     * Three things are checked, without disturbing a run in progress:
     *   1. the lock file can be created in the project folder;
     *   2. a lock can be taken (a mount without lock support fails here, e.g.
     *      NFS "nolock" without a local fallback answers ENOLCK);
     *   3. it is ENFORCED: a second, separate handle on the same file must be
     *      refused while the first holds it (a mount that silently accepts
     *      every lock passes 2 and fails here - then two runs would overlap);
     * plus what the mount is (from /proc/mounts), because a lock that works
     * only on this host is fine when every run happens here (one cron host)
     * and useless when two hosts run the pipeline on the same share.
     *
     * @return array{status: string, message: string, mount: string}
     *         status: OK | WARN (works, read the message) | FAIL (do not run)
     */
    public static function probe(string $projectPath, string $modality): array
    {
        $path  = rtrim($projectPath, '/') . '/' . self::fileFor($modality);
        $mount = self::mountOf($projectPath);
        $desc  = $mount === null ? 'mount not identified'
            : "{$mount['type']} mount {$mount['point']}" . ($mount['source'] !== '' ? " ({$mount['source']})" : '');

        $h1 = @fopen($path, 'c+');
        if ($h1 === false) {
            return ['status' => 'FAIL', 'mount' => $desc, 'message' => "cannot open or create {$path} - the pipeline "
                . 'user needs write access to the project folder (every run that writes takes this lock)'];
        }
        if (!flock($h1, LOCK_EX | LOCK_NB)) {
            $holder = trim((string) stream_get_contents($h1, 500, 0));
            fclose($h1);
            return $holder !== ''
                ? ['status' => 'OK', 'mount' => $desc, 'message' => "locking works: held right now by {$holder}"]
                : ['status' => 'FAIL', 'mount' => $desc, 'message' => 'a lock cannot be taken on this mount '
                    . '(no lock support, e.g. NFS "nolock" without local_lock) - runs would be skipped. Fix the '
                    . 'mount options, or run from a filesystem with locking'];
        }
        $h2 = @fopen($path, 'c+');
        $enforced = $h2 !== false && !flock($h2, LOCK_EX | LOCK_NB);
        if ($h2 !== false) {
            fclose($h2);
        }
        flock($h1, LOCK_UN);
        fclose($h1);

        if (!$enforced) {
            return ['status' => 'FAIL', 'mount' => $desc, 'message' => 'locks are accepted but NOT enforced on '
                . 'this mount: a second lock on the same file was granted - two runs could overlap. Fix the '
                . 'mount (lock support), or make sure only one run can ever start for this project'];
        }

        $type = $mount['type'] ?? '';
        $opts = $mount['options'] ?? '';
        $localOnly = match (true) {
            str_starts_with($type, 'nfs') => (bool) preg_match('/(^|,)(nolock|local_lock=(all|flock))(,|$)/', $opts),
            in_array($type, ['cifs', 'smb3', 'smbfs'], true) => (bool) preg_match('/(^|,)nobrl(,|$)/', $opts),
            str_starts_with($type, 'fuse') => true,
            default => false,
        };
        if ($localOnly) {
            return ['status' => 'WARN', 'mount' => $desc, 'message' => "locking works, but on this {$type} mount "
                . "({$opts}) it is local to this host only: safe as long as every run for this project starts "
                . 'on this host (one cron machine); two hosts running the pipeline on this share would not see '
                . "each other's lock"];
        }
        return ['status' => 'OK', 'mount' => $desc, 'message' => 'locking works and is enforced'
            . (str_starts_with($type, 'nfs') ? ' (NFS: locks go to the server, so they hold across hosts)' : '')];
    }

    /**
     * The mount holding a path, from /proc/mounts (Linux); null elsewhere.
     *
     * @return array{point: string, type: string, options: string, source: string}|null
     */
    private static function mountOf(string $path): ?array
    {
        $real  = realpath($path) ?: $path;
        $lines = @file('/proc/mounts', FILE_IGNORE_NEW_LINES) ?: [];
        $best  = null;
        foreach ($lines as $line) {
            $f = preg_split('/\s+/', $line);
            if (count($f) < 4) {
                continue;
            }
            $point = str_replace('\\040', ' ', $f[1]);
            if (($real === $point || str_starts_with($real, rtrim($point, '/') . '/'))
                && ($best === null || strlen($point) > strlen($best['point']))
            ) {
                $best = ['point' => $point, 'type' => $f[2], 'options' => $f[3], 'source' => $f[0]];
            }
        }
        return $best;
    }

    /**
     * proc_open() descriptor and environment that hand this lock to a step
     * script started by the bundle.
     *
     * @return array{0: array<int, mixed>, 1: array<string, string>} [extra descriptors, environment]
     */
    public function childDescriptor(): array
    {
        $env = getenv();
        $env[self::ENV_PATH] = $this->path;
        return [[self::CHILD_FD => $this->handle], $env];
    }

    public function release(): void
    {
        if (is_resource($this->handle)) {
            if (!$this->inherited) {
                // Leave the file (other runs open it); clear who held it.
                ftruncate($this->handle, 0);
                flock($this->handle, LOCK_UN);
            }
            fclose($this->handle);
        }
    }
}
