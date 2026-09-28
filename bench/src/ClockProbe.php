<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class ClockProbe
{
    /** Bounds possible child clock offset by parent request/reply timestamps. */
    public static function run(): array
    {
        $process = proc_open([PHP_BINARY, Config::rootDir() . '/bench/clock-probe.php'],
            [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes,
            Config::rootDir(), ['PATH' => '/usr/bin:/bin']);
        if (!is_resource($process)) { throw new \RuntimeException('Clock probe failed to start'); }
        stream_set_timeout($pipes[1], 2);
        $samples = [];
        try {
            for ($i = 0; $i < 20; ++$i) {
                $before = hrtime(true);
                fwrite($pipes[0], "probe\n");
                fflush($pipes[0]);
                $line = fgets($pipes[1]);
                $after = hrtime(true);
                if ($line === false || !ctype_digit(trim($line))) { throw new \RuntimeException('Clock probe timed out or returned invalid timestamp'); }
                $child = (int) trim($line);
                $samples[] = ['before_ns' => $before, 'child_ns' => $child, 'after_ns' => $after,
                    'offset_lower_ns' => $child - $after, 'offset_upper_ns' => $child - $before, 'roundtrip_ns' => $after - $before];
            }
        } finally {
            fclose($pipes[0]);
            fclose($pipes[1]);
            proc_terminate($process, 9);
            proc_close($process);
        }
        $lower = max(array_column($samples, 'offset_lower_ns'));
        $upper = min(array_column($samples, 'offset_upper_ns'));
        return ['verified' => $lower <= 0 && $upper >= 0 && $upper - $lower < 2_000_000,
            'method' => 'Linux same-host CLOCK_MONOTONIC, 20 bounded parent/child request-reply brackets; interval must include zero with width under 2 ms',
            'offset_lower_ns' => $lower, 'offset_upper_ns' => $upper, 'samples' => $samples];
    }
}
