<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

/**
 * Reads the local process tree from /proc.
 *
 * Identifies broker processes and their descendants, including driver-owned children.
 */
final class ProcessTree
{
    public static function available(): bool
    {
        return is_dir('/proc/self');
    }

    /**
     * @return array<int, array{ppid: ?int, cmd: string}>
     */
    public static function snapshot(): array
    {
        $processes = [];

        $directories = glob('/proc/[0-9]*');
        foreach (false === $directories ? [] : $directories as $directory) {
            $pid = (int) basename($directory);
            $command = @file_get_contents($directory.'/cmdline');
            if (false === $command) {
                continue;
            }

            $command = trim(str_replace("\0", ' ', $command));
            if ('' === $command) {
                continue;
            }

            $ppid = null;
            $status = @file_get_contents($directory.'/status');
            if (\is_string($status) && 1 === preg_match('/^PPid:\s+(\d+)/m', $status, $match)) {
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
            if (null === $ppid || 0 === $ppid || 1 === $ppid) {
                return false;
            }
            if ($ppid === $ancestor) {
                return true;
            }
            $pid = $ppid;
        }

        return false;
    }
}
