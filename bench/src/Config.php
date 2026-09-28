<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * Frozen benchmark budgets and workload descriptors.
 *
 * The budgets below were fixed before any candidate existed. Changing them invalidates
 * comparisons with previously recorded baselines, so change them only together with the
 * method note in `bench/README.md` and a new baseline run.
 */
final class Config
{
    public const SCHEMA_VERSION = 1;

    /** Small payload size in bytes, representative of a local application message. */
    public const SMALL_PAYLOAD_BYTES = 256;

    /** Larger payload size in bytes, representative of a local application message. */
    public const LARGE_PAYLOAD_BYTES = 16_384;

    /** Repetitions included in the summary. */
    public const MEASURED_REPETITIONS = 3;

    /** Repetitions kept in the raw artifacts and excluded from the summary. */
    public const WARMUP_REPETITIONS = 1;

    /** A metric with fewer samples than this cannot support a p99 claim. */
    public const MIN_TAIL_SAMPLES = 1000;

    /** Worker polling sleep when no message is available, in microseconds. Recorded, not inferred. */
    public const POLL_SLEEP_US = 1000;

    /** Bounded SQLite lock wait for the baseline, in milliseconds. The DBAL/PDO default is 60000. */
    public const BUSY_TIMEOUT_MS = 5000;

    /** Doctrine transport redelivery timeout in seconds. Symfony's default, recorded explicitly. */
    public const REDELIVER_TIMEOUT_S = 3600;

    /** Seconds a child process may take to report readiness. */
    public const STARTUP_TIMEOUT_S = 20;

    /** Seconds between SIGTERM and SIGKILL when a child exceeds its bound. */
    public const KILL_GRACE_S = 5;

    /** Coordinator progress/resource sampling and post-publication inventory cadence. */
    public const PROGRESS_INTERVAL_US = 20_000;

    /** Upper bound on raw samples one child writes for one repetition. */
    public const SAMPLE_BUDGET_PER_PROCESS = 20_000;

    /** Messages per publisher in smoke mode; keeps an end-to-end check short. */
    public const SMOKE_PUBLISHER_COUNT = 4;

    /** Messages per queue in smoke mode. */
    public const SMOKE_PREFILL_PER_QUEUE = 4;

    /** Effective durability the baseline must report for every connection. */
    public const EXPECTED_JOURNAL_MODE = 'wal';

    public const EXPECTED_SYNCHRONOUS = 2;

    /** Queue that receives the subsecond quantization probe message. */
    public const PROBE_QUEUE = 'bench_delayed_probe';

    /**
     * Delay used for the delayed-eligibility workload, in milliseconds.
     *
     * A whole number of seconds on purpose: the Doctrine transport stores `available_at` with
     * one-second resolution, so only whole-second delays survive the round trip. The subsecond
     * probe below measures that quantization; it never mixes into the delayed metric.
     */
    public const DELAY_MS = 2000;

    /** Delay used for the subsecond probe message, in milliseconds. */
    public const SUBSECOND_PROBE_DELAY_MS = 300;

    public const MESSENGER_TABLE = 'messenger_messages';

    /** Percentile definition used in every report. */
    public const PERCENTILE_METHOD = 'nearest-rank';

    /**
     * @return array<string, array<string, mixed>>
     */
    public static function workloads(): array
    {
        return [
            'roundtrip' => [
                'name' => 'roundtrip',
                'title' => 'Send to receive to ACK, one publisher and one consumer',
                'queues' => ['bench_roundtrip'],
                'publishers' => [['count' => 150]],
                'consumers' => 1,
                'pacing_us' => 0,
                'timeout_s' => 60,
            ],
            'concurrent' => [
                'name' => 'concurrent',
                'title' => 'Concurrent publishers and consumers on one queue',
                'queues' => ['bench_concurrent'],
                'publishers' => [['count' => 50], ['count' => 50], ['count' => 50]],
                'consumers' => 2,
                'pacing_us' => 0,
                'timeout_s' => 90,
            ],
            'many-to-one' => [
                'name' => 'many-to-one',
                'title' => 'Four publishers feeding one consumer',
                'queues' => ['bench_many_to_one'],
                'publishers' => [['count' => 40], ['count' => 40], ['count' => 40], ['count' => 40]],
                'consumers' => 1,
                'pacing_us' => 0,
                'timeout_s' => 90,
            ],
            'backlog' => [
                'name' => 'backlog',
                'title' => 'Bounded backlog across three named queues, drained by two consumers',
                'queues' => ['bench_backlog_a', 'bench_backlog_b', 'bench_backlog_c'],
                'publishers' => [],
                'consumers' => 2,
                'prefill' => ['per_queue' => 80],
                'pacing_us' => 0,
                'timeout_s' => 120,
            ],
            'idle' => [
                'name' => 'idle',
                'title' => 'Ready but idle consumer, then publications',
                'queues' => ['bench_idle'],
                'publishers' => [['count' => 20]],
                'consumers' => 1,
                'pacing_us' => 100_000,
                'idle_ms' => 1500,
                'timeout_s' => 60,
            ],
            'delayed' => [
                'name' => 'delayed',
                'title' => 'Delayed messages becoming eligible on an otherwise idle queue',
                'queues' => ['bench_delayed', self::PROBE_QUEUE],
                'publishers' => [],
                'consumers' => 1,
                'prefill' => [
                    'per_queue' => 40,
                    'queues' => ['bench_delayed'],
                    'delay_ms' => self::DELAY_MS,
                    'stagger_us' => 50_000,
                ],
                'probes' => [
                    [
                        'argument' => 'probe-subsecond',
                        'queue' => self::PROBE_QUEUE,
                        'delay_ms' => self::SUBSECOND_PROBE_DELAY_MS,
                    ],
                ],
                'measure_lateness' => true,
                'pacing_us' => 0,
                'timeout_s' => 120,
            ],
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public static function workload(string $name): array
    {
        $workloads = self::workloads();

        if (!isset($workloads[$name])) {
            throw new \InvalidArgumentException(\sprintf('Unknown workload "%s". Known workloads: %s.', $name, implode(', ', array_keys($workloads))));
        }

        return $workloads[$name];
    }

    /**
     * Messages the workload sends in total, including prefill and probe messages.
     *
     * @param array<string, mixed> $workload
     */
    public static function expectedSends(array $workload): int
    {
        $prefill = $workload['prefill'] ?? null;
        if (\is_array($prefill)) {
            $queues = $prefill['queues'] ?? $workload['queues'];
            $sends = $prefill['per_queue'] * \count($queues);
        } else {
            $sends = 0;
            foreach ($workload['publishers'] as $publisher) {
                $sends += $publisher['count'];
            }
        }

        $sends += self::probeSends($workload);

        return $sends;
    }

    /**
     * Probe sends, which evidence a transport property instead of measuring it.
     *
     * @param array<string, mixed> $workload
     */
    public static function probeSends(array $workload): int
    {
        return \count($workload['probes'] ?? []);
    }

    /**
     * Measured sends, excluding messages sent only to evidence a transport quirk.
     *
     * @param array<string, mixed> $workload
     */
    public static function expectedMeasuredSends(array $workload): int
    {
        return self::expectedSends($workload) - self::probeSends($workload);
    }

    /**
     * Payload size for a message index: small and larger payloads alternate.
     */
    public static function payloadSize(int $index): int
    {
        return 0 === $index % 2 ? self::SMALL_PAYLOAD_BYTES : self::LARGE_PAYLOAD_BYTES;
    }

    public static function rootDir(): string
    {
        return \dirname(__DIR__, 2);
    }

    public static function varDir(): string
    {
        return self::rootDir().'/var/bench';
    }
}
