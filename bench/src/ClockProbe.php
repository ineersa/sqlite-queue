<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Symfony\Component\Process\InputStream;
use Symfony\Component\Process\Process as SymfonyProcess;

final class ClockProbe
{
    /**
     * Bounds the child's clock offset by parent request/reply timestamps.
     *
     * @return array{verified: bool, method: string, offset_lower_ns: int, offset_upper_ns: int, samples: list<array{before_ns: int, child_ns: int, after_ns: int, offset_lower_ns: int, offset_upper_ns: int, roundtrip_ns: int}>}
     */
    public static function run(): array
    {
        $input = new InputStream();
        $process = new SymfonyProcess(
            [\PHP_BINARY, Config::rootDir().'/bin/benchmark', 'clock-probe', '--no-ansi'],
            Config::rootDir(),
            Process::environment(),
            $input,
            5,
        );
        $samples = [];
        $process->start();

        try {
            for ($round = 0; $round < 20; ++$round) {
                $before = hrtime(true);
                $input->write("probe\n");
                $reply = '';
                $received = $process->waitUntil(static function (string $type, string $buffer) use (&$reply): bool {
                    if (SymfonyProcess::OUT === $type) {
                        $reply .= $buffer;
                    }

                    return str_contains($reply, "\n");
                });
                $after = hrtime(true);
                if (!$received || !ctype_digit(trim($reply))) {
                    throw new \RuntimeException('Clock probe returned an invalid timestamp.');
                }

                $child = (int) trim($reply);
                $samples[] = [
                    'before_ns' => $before,
                    'child_ns' => $child,
                    'after_ns' => $after,
                    'offset_lower_ns' => $child - $after,
                    'offset_upper_ns' => $child - $before,
                    'roundtrip_ns' => $after - $before,
                ];
            }
        } finally {
            $input->close();
            $process->stop(0.1);
        }

        $lower = max(array_column($samples, 'offset_lower_ns'));
        $upper = min(array_column($samples, 'offset_upper_ns'));

        return [
            'verified' => $lower <= 0 && $upper >= 0 && $upper - $lower < 2_000_000,
            'method' => 'Linux CLOCK_MONOTONIC; 20 request/reply brackets must include zero offset with width below 2 ms.',
            'offset_lower_ns' => $lower,
            'offset_upper_ns' => $upper,
            'samples' => $samples,
        ];
    }
}
