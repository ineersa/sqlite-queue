<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * Reads the owned process tree from /proc.
 *
 * The benchmark never signals a process it did not start: membership is established by
 * walking parent links from the coordinator's own children, so a stray process with a
 * similar name cannot be adopted or killed.
 */
final class ProcessTree
{
    private static int $ticksPerSecond = 100;

    private static int $pageSizeBytes = 4096;

    public static function available(): bool
    {
        return \is_dir('/proc/self');
    }

    public static function setTicksPerSecond(int $ticks): void
    {
        if ($ticks > 0) {
            self::$ticksPerSecond = $ticks;
        }
    }

    public static function ticksPerSecond(): int
    {
        return self::$ticksPerSecond;
    }

    public static function setPageSizeBytes(int $bytes): void
    {
        if ($bytes > 0) {
            self::$pageSizeBytes = $bytes;
        }
    }

    /**
     * @return array<int, array{ppid: ?int, state: string, cmd: string, cpu_seconds: float, rss_kb: int}>
     */
    public static function snapshot(): array
    {
        $processes = [];

        foreach (\glob('/proc/[0-9]*') ?: [] as $directory) {
            $pid = (int) \basename($directory);
            $stat = @\file_get_contents($directory . '/stat');
            if (!\is_string($stat)) {
                continue;
            }

            if (\preg_match('/^(\d+) \((.*)\) (.*)$/s', $stat, $match) !== 1) {
                continue;
            }

            $fields = \preg_split('/\s+/', \trim($match[3])) ?: [];
            if (\count($fields) < 22) {
                continue;
            }

            $command = @\file_get_contents($directory . '/cmdline');
            $command = \is_string($command) ? \trim(\str_replace("\0", ' ', $command)) : $match[2];
            if ('' === $command) {
                $command = '[' . $match[2] . ']';
            }

            $processes[$pid] = [
                'ppid' => (int) $fields[1],
                'state' => $fields[0],
                'cmd' => \substr($command, 0, 300),
                'cpu_seconds' => ((int) $fields[11] + (int) $fields[12]) / self::$ticksPerSecond,
                'rss_kb' => \intdiv((int) $fields[21] * self::$pageSizeBytes, 1024),
            ];
        }

        return $processes;
    }

    /**
     * PIDs reachable from $root through parent links, excluding $root itself.
     *
     * @param array<int, array{ppid: ?int, state: string, cmd: string, cpu_seconds: float, rss_kb: int}> $snapshot
     * @return list<int>
     */
    public static function descendants(int $root, array $snapshot): array
    {
        $children = [];
        foreach ($snapshot as $pid => $process) {
            $ppid = $process['ppid'];
            if (null === $ppid) {
                continue;
            }
            $children[$ppid][] = $pid;
        }

        $found = [];
        $queue = [$root];

        while ([] !== $queue) {
            $current = \array_shift($queue);
            foreach ($children[$current] ?? [] as $child) {
                if (isset($found[$child])) {
                    continue;
                }
                // A zombie holds no resources and disappears once its parent reaps it.
                if ('Z' === ($snapshot[$child]['state'] ?? 'Z')) {
                    continue;
                }
                $found[$child] = true;
                $queue[] = $child;
            }
        }

        return \array_map('intval', \array_keys($found));
    }

    /**
     * @return list<int>
     */
    public static function tree(int $root): array
    {
        return self::descendants($root, self::snapshot());
    }

    /**
     * @param list<int> $pids
     * @param array<int, array{ppid: ?int, state: string, cmd: string, cpu_seconds: float, rss_kb: int}> $snapshot
     * @return array<int, array{pid: int, cmd: string}>
     */
    public static function describe(array $pids, array $snapshot): array
    {
        $described = [];
        foreach ($pids as $pid) {
            if (isset($snapshot[$pid])) {
                $described[] = ['pid' => $pid, 'cmd' => $snapshot[$pid]['cmd']];
            }
        }

        return $described;
    }
}
