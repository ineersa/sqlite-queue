<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Buffers at most capacity bytes. Writes occur on capacity boundaries and explicit phase flushes, never fflush per record. */
final class Recorder
{
    public const BUFFER_CAPACITY_BYTES = 65536;

    private string $buffer = '';
    private int $lost = 0;
    private int $writeFailures = 0;
    private int $sequence = 0;
    /** @var array<string, int> */
    private array $suppressedEmpty = [];

    /** @param \Closure(string): bool $write */
    // Full is the normal trace. Essential preserves every message identity and failure, aggregating empty attempts.
    public function __construct(private readonly \Closure $write, private readonly int $capacity, private readonly string $run, private readonly string $role, private readonly TelemetryLevel $level = TelemetryLevel::Detailed)
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
     * @param array<string, int> $eligibility empty for operations without a requested delay
     */
    public function record(Operation $operation, Outcome $outcome, Phase $phase, string $id, string $correlation, int $started, int $ended, string $error, ?int $payloadBytes = null, ?int $activeNanoseconds = null, ?int $scheduledNs = null, array $details = [], array $eligibility = []): void
    {
        if (TelemetryLevel::Essential === $this->level && Operation::Receive === $operation && Outcome::Empty === $outcome) {
            $this->suppressedEmpty[$phase->value] = ($this->suppressedEmpty[$phase->value] ?? 0) + 1;

            return;
        }
        $line = json_encode(['run' => $this->run, 'role' => $this->role, 'pid' => getmypid(), 'operation_id' => $id, 'attempt' => 1, 'operation' => $operation->value, 'outcome' => $outcome->value, 'phase' => $phase->value, 'correlation' => $correlation, 'payload_bytes' => $payloadBytes, 'started_ns' => $started, 'ended_ns' => $ended, 'active_ns' => $activeNanoseconds, 'scheduled_ns' => $scheduledNs, 'error_details' => $details, 'eligibility' => $eligibility, 'span_scope' => Operation::Receive === $operation ? 'iterable-lifetime' : 'public-boundary', 'error' => $error, 'unknown_commit' => Outcome::Error === $outcome && \in_array($operation, [Operation::Send, Operation::Ack, Operation::Reject], true)], \JSON_THROW_ON_ERROR)."\n";
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
        return ['lost_records' => $this->lost, 'write_failures' => $this->writeFailures, 'buffered_bytes' => \strlen($this->buffer), 'aggregated_empty_attempts_by_phase' => $this->suppressedEmpty];
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
