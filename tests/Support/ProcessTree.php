<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

/**
 * Reads the local process tree from /proc.
 *
 * The driver starts one OS process per connection. These helpers find that process
 * and the short-lived shell wrapper that amphp/parallel creates for it.
 */
final class ProcessTree
{
    /** Path fragment that identifies the persistence worker script in a command line. */
    public const WORKER_SCRIPT = 'amphp-sqlite3/src/Internal/worker.php';

    /** Process title that the persistence worker sets for itself. */
    public const WORKER_TITLE = 'amp-process';

    public static function available(): bool
    {
        return \is_dir('/proc/self');
    }

    /**
     * @return array<int, array{ppid: ?int, cmd: string}>
     */
    public static function snapshot(): array
    {
        $processes = [];

        foreach (\glob('/proc/[0-9]*') ?: [] as $directory) {
            $pid = (int) \basename($directory);
            $command = @\file_get_contents($directory . '/cmdline');
            if ($command === false) {
                continue;
            }

            $command = \trim(\str_replace("\0", ' ', $command));
            if ($command === '') {
                continue;
            }

            $ppid = null;
            $status = @\file_get_contents($directory . '/status');
            if (\is_string($status) && \preg_match('/^PPid:\s+(\d+)/m', $status, $match) === 1) {
                $ppid = (int) $match[1];
            }

            $processes[$pid] = ['ppid' => $ppid, 'cmd' => $command];
        }

        return $processes;
    }

    /**
     * @param array<int, array{ppid: ?int, cmd: string}> $snapshot
     */
    public static function isDescendantOf(int $pid, int $ancestor, array $snapshot): bool
    {
        $guard = 0;

        while (isset($snapshot[$pid]) && $guard++ < 128) {
            $ppid = $snapshot[$pid]['ppid'];
            if ($ppid === null || $ppid === 0 || $ppid === 1) {
                return false;
            }
            if ($ppid === $ancestor) {
                return true;
            }
            $pid = $ppid;
        }

        return false;
    }

    /**
     * PIDs of the shell wrappers that amphp/parallel starts for a connection.
     *
     * @param array<int, array{ppid: ?int, cmd: string}> $snapshot
     * @return list<int>
     */
    public static function workerLaunchers(array $snapshot): array
    {
        $pids = [];

        foreach ($snapshot as $pid => $process) {
            if (\str_contains($process['cmd'], self::WORKER_SCRIPT)) {
                $pids[] = $pid;
            }
        }

        return $pids;
    }

    /**
     * PIDs of the persistence worker processes owned by $ancestor.
     *
     * @param array<int, array{ppid: ?int, cmd: string}> $snapshot
     * @return list<int>
     */
    public static function persistenceWorkers(int $ancestor, array $snapshot): array
    {
        $pids = [];

        foreach ($snapshot as $pid => $process) {
            if ($process['cmd'] !== self::WORKER_TITLE) {
                continue;
            }
            if (self::isDescendantOf($pid, $ancestor, $snapshot)) {
                $pids[] = $pid;
            }
        }

        return $pids;
    }

    /**
     * Every launcher and persistence worker process owned by $ancestor.
     *
     * @return array{launchers: list<int>, workers: list<int>}
     */
    public static function ownedBy(int $ancestor): array
    {
        $snapshot = self::snapshot();

        $launchers = \array_values(\array_filter(
            self::workerLaunchers($snapshot),
            static fn (int $pid): bool => self::isDescendantOf($pid, $ancestor, $snapshot),
        ));

        return [
            'launchers' => $launchers,
            'workers' => self::persistenceWorkers($ancestor, $snapshot),
        ];
    }
}
