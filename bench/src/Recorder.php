<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Buffers at most capacity bytes. Writes occur on capacity boundaries and explicit phase flushes, never fflush per record. */
final class Recorder
{
    public const BUFFER_CAPACITY_BYTES = 65536;
    /** Inclusive upper bounds in nanoseconds; final bin is overflow. */
    public const EMPTY_DURATION_BINS_NS = [1000, 10000, 100000, 1000000, 10000000, 100000000];

    private string $buffer = '';
    private int $lost = 0;
    private int $writeFailures = 0;
    private int $sequence = 0;
    /** @var array<string, array<string, mixed>> */
    private array $emptyReceives = [];

    /** @param \Closure(string): bool $write */
    public function __construct(private readonly \Closure $write, private readonly int $capacity, private readonly string $run, private readonly string $role)
    {
        if ($capacity < 1) {
            throw new \InvalidArgumentException('Telemetry capacity must be positive.');
        }
    }

    public function nextId(): string
    {
        return $this->role.':'.getmypid().':'.++$this->sequence;
    }

    // Empty/error receive attempts have no payload. Active receive duration excludes yielded consumer work;
    // other operations do not need that separate duration. Null means unavailable in both cases.
    /** @param array<string, mixed> $details
     */
    public function record(Operation $operation, Outcome $outcome, Phase $phase, string $id, string $correlation, int $started, int $ended, string $error, ?int $payloadBytes = null, ?int $activeNanoseconds = null, array $details = []): void
    {
        if (Operation::Receive === $operation && Outcome::Empty === $outcome) {
            $duration = $activeNanoseconds ?? ($ended - $started);
            $key = $phase->value;
            $row = $this->emptyReceives[$key] ?? ['count' => 0, 'sum_active_ns' => 0, 'max_active_ns' => 0, 'first_started_ns' => $started, 'last_ended_ns' => $ended, 'histogram' => array_fill(0, \count(self::EMPTY_DURATION_BINS_NS) + 1, 0)];
            ++$row['count'];
            $row['sum_active_ns'] += $duration;
            $row['max_active_ns'] = max($row['max_active_ns'], $duration);
            $row['last_ended_ns'] = $ended;
            $bin = 0;
            foreach (self::EMPTY_DURATION_BINS_NS as $upper) {
                if ($duration <= $upper) {
                    break;
                }
                ++$bin;
            }
            ++$row['histogram'][$bin];
            $this->emptyReceives[$key] = $row;

            return;
        }
        $line = json_encode(['run' => $this->run, 'role' => $this->role, 'pid' => getmypid(), 'operation_id' => $id, 'attempt' => 1, 'operation' => $operation->value, 'outcome' => $outcome->value, 'phase' => $phase->value, 'correlation' => $correlation, 'payload_bytes' => $payloadBytes, 'started_ns' => $started, 'ended_ns' => $ended, 'active_ns' => $activeNanoseconds, 'error_details' => $details, 'span_scope' => Operation::Receive === $operation ? 'iterable-lifetime' : 'public-boundary', 'error' => $error, 'unknown_commit' => Outcome::Error === $outcome && \in_array($operation, [Operation::Send, Operation::Ack, Operation::Reject], true)], \JSON_THROW_ON_ERROR)."\n";
        if (\strlen($line) > $this->capacity) {
            ++$this->lost;

            return;
        }
        if (\strlen($this->buffer) + \strlen($line) > $this->capacity) {
            $this->flush();
        }
        $this->buffer .= $line;
    }

    public function flush(): void
    {
        if ('' === $this->buffer) {
            return;
        }
        try {
            $ok = ($this->write)($this->buffer);
        } catch (\Throwable) {
            $ok = false;
        }
        if (!$ok) {
            ++$this->writeFailures;
            $this->lost += substr_count($this->buffer, "\n");
        }
        $this->buffer = '';
    }

    /** @return array<string, mixed> */
    public function counters(): array
    {
        return ['lost_records' => $this->lost, 'write_failures' => $this->writeFailures, 'buffered_bytes' => \strlen($this->buffer), 'empty_receives_by_phase' => $this->emptyReceives, 'empty_duration_bin_upper_ns' => self::EMPTY_DURATION_BINS_NS, 'empty_duration_bin_policy' => 'inclusive upper bounds; final bin is overflow; active receive duration; phase at receive entry'];
    }

    /** @return \Generator<int, array<string, mixed>> */
    public static function read(string $path): \Generator
    {
        $file = new \SplFileObject($path, 'rb');
        while (!$file->eof()) {
            $line = $file->fgets();
            if ('' === trim($line)) {
                continue;
            }
            $record = json_decode($line, true, flags: \JSON_THROW_ON_ERROR);
            if (!\is_array($record)) {
                throw new \RuntimeException('Telemetry record is not an object.');
            }
            yield $record;
        }
    }
}
