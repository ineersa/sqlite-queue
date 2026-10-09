<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\DTO;

use Ineersa\SqliteQueue\Bench\Backend;
use Ineersa\SqliteQueue\Bench\ConcurrentCohort;
use Ineersa\SqliteQueue\Bench\Config;
use Ineersa\SqliteQueue\Bench\Runner;
use Ineersa\SqliteQueue\Bench\Scenario;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;

final readonly class RunOptionsDTO
{
    public const DEFAULT_DURATION_SECONDS = 60.0;
    public const DEFAULT_DOCTRINE_POLLING_MILLISECONDS = 50;
    public const MAX_DOCTRINE_POLLING_MILLISECONDS = 1_000;
    public const DRAIN_TIMEOUT_SECONDS = 15;
    public const WARMUP_TIMEOUT_SECONDS = 20;
    public const APPLICATION_RATE = 5;
    public const APPLICATION_CAPACITY = 16;
    public const HANDLER_MILLISECONDS = 100;
    public const SETTLING_SECONDS = 0.1;

    /** Doctrine defaults to Hatfield's 50 ms poll; broker consumers keep notification WAIT. */
    public function __construct(
        public Scenario $scenario,
        public bool $smoke,
        public float $durationSeconds,
        public SqliteSynchronousMode $synchronous,
        public int $doctrinePollingMilliseconds = self::DEFAULT_DOCTRINE_POLLING_MILLISECONDS,
    ) {
        if ($doctrinePollingMilliseconds <= 0) {
            throw new \InvalidArgumentException('Doctrine polling interval must be positive milliseconds.');
        }
        if ($doctrinePollingMilliseconds > self::MAX_DOCTRINE_POLLING_MILLISECONDS) {
            throw new \InvalidArgumentException('Doctrine polling interval must not exceed 1000 milliseconds.');
        }
        if (!is_finite($durationSeconds)) {
            throw new \InvalidArgumentException('Duration must be finite seconds.');
        }
        if ($durationSeconds <= 0) {
            throw new \InvalidArgumentException('Duration must be positive seconds.');
        }
        if ($durationSeconds > 3600) {
            throw new \InvalidArgumentException('Duration must not exceed 3600 seconds.');
        }
        if ((int) ($durationSeconds * 1e9) < 1) {
            throw new \InvalidArgumentException('Duration must span at least one monotonic nanosecond.');
        }
    }

    /** @return list<array{id: string, backend: string, scenario: string}> */
    public function schedule(): array
    {
        return array_map(fn (Backend $backend): array => ['id' => $backend->value, 'backend' => $backend->value, 'scenario' => $this->scenario->value], [Backend::Doctrine, Backend::Broker]);
    }

    /** @return array<string, mixed> */
    public function configuration(): array
    {
        $settings = ['workload' => $this->scenario->value, 'mode' => $this->smoke ? 'smoke' : 'measured', 'smoke' => $this->smoke, 'duration_seconds' => $this->durationSeconds, 'synchronous_desired' => $this->synchronous->value, 'doctrine_polling_milliseconds' => $this->doctrinePollingMilliseconds, 'clock_basis' => 'same-host hrtime nanoseconds; elapsed spans, not wall time'];

        return $settings + match ($this->scenario) {
            Scenario::MultiQueue => ['queues' => Scenario::MULTI_QUEUE_RECEIVERS, 'consumers' => 1, 'pickup_modes' => [Backend::Broker->value => 'notification-wait-any', Backend::Doctrine->value => 'polling'], 'method_change' => 'two selected queues on one stock worker; not comparable to earlier single-queue polling captures'],
            Scenario::Application => ['rate' => self::APPLICATION_RATE, 'handler_milliseconds' => self::HANDLER_MILLISECONDS, 'capacity' => self::APPLICATION_CAPACITY],
            Scenario::Concurrent => ['publishers' => ConcurrentCohort::PUBLISHERS, 'consumers' => ConcurrentCohort::CONSUMERS, 'measured_messages' => ConcurrentCohort::PUBLISHERS * ($this->smoke ? ConcurrentCohort::SMOKE_PER_PUBLISHER : ConcurrentCohort::MESSAGES_PER_PUBLISHER), 'duration_scope' => 'finite cohort through final required ACK, not sustained capacity'],
            Scenario::Retention => ['cycles' => $this->effectiveCycles(), 'cycle_messages' => $this->effectiveCycleMessages(), 'retention_evidence' => $this->smoke ? 'smoke control-path check only' : 'matched drained-state screen, not leak-free proof'],
            default => [],
        };
    }

    public function effectiveCycles(): int
    {
        return $this->smoke ? 2 : 20;
    }

    public function effectiveCycleMessages(): int
    {
        return $this->smoke ? 2 : 100;
    }

    public function warmupTimeoutSeconds(): float
    {
        return self::WARMUP_TIMEOUT_SECONDS;
    }

    public function drainTimeoutSeconds(): float
    {
        return self::DRAIN_TIMEOUT_SECONDS;
    }

    public function idleSeconds(): float
    {
        return $this->smoke ? 0.1 : $this->durationSeconds;
    }

    public function applicationSeconds(): float
    {
        return $this->smoke ? 0.4 : $this->durationSeconds;
    }

    public function processTimeoutSeconds(): float
    {
        if (Scenario::Concurrent === $this->scenario) {
            return ConcurrentCohort::RUN_TIMEOUT_SECONDS;
        }
        if (Scenario::Retention === $this->scenario) {
            return 3 * Config::STARTUP_TIMEOUT_S + self::WARMUP_TIMEOUT_SECONDS + $this->effectiveCycles() * ($this->effectiveCycleMessages() * Config::STARTUP_TIMEOUT_S + self::DRAIN_TIMEOUT_SECONDS + self::SETTLING_SECONDS + Config::STARTUP_TIMEOUT_S) + Config::KILL_GRACE_S;
        }

        return $this->durationSeconds + 3 * Config::STARTUP_TIMEOUT_S + self::WARMUP_TIMEOUT_SECONDS + self::DRAIN_TIMEOUT_SECONDS + Config::KILL_GRACE_S;
    }

    /** Null means roundtrip runs until its measured window ends. */
    public function finiteCohortMessages(): ?int
    {
        if ($this->scenario->isPickup()) {
            return Runner::WAKEUP_MESSAGES;
        }

        return $this->smoke ? Runner::MEASURED_MESSAGES : null;
    }
}
