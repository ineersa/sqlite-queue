<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\DTO;

use Ineersa\SqliteQueue\Bench\Backend;
use Ineersa\SqliteQueue\Bench\Scenario;

final readonly class RunOptionsDTO
{
    public const DEFAULT_DURATION_SECONDS = 60.0;
    public const DEFAULT_REPETITIONS = 1;
    public const DRAIN_TIMEOUT_SECONDS = 15;
    public const WARMUP_TIMEOUT_SECONDS = 20;
    public const DEFAULT_DELAY_MILLISECONDS = 2000;
    public const MAX_DELAY_MILLISECONDS = 30000;
    public const DEFAULT_RATE = 20.0;
    public const MAX_RATE = 100000.0;
    public const DEFAULT_CAPACITY = 64;
    public const MAX_CAPACITY = 10000;
    public const DEFAULT_HANDLER_MILLISECONDS = 100;
    public const MAX_HANDLER_MILLISECONDS = 10000;
    public const SMOKE_FIXED_RATE_SECONDS = 0.2;
    public const DEFAULT_CYCLES = 20;
    public const MAX_CYCLES = 1000;
    public const DEFAULT_CYCLE_MESSAGES = 2;
    public const MAX_CYCLE_MESSAGES = 10000;
    public const DEFAULT_SETTLING_SECONDS = 0.1;
    public const MAX_SETTLING_SECONDS = 60.0;
    public const SMOKE_IDLE_SECONDS = 0.1;

    // Defaults define the standard delayed, fixed-rate and application settings; other scenarios do not apply them.
    public function __construct(public Scenario $scenario, public bool $smoke, public bool $pilot, public float $durationSeconds, public int $repetitions, public int $delayMilliseconds = self::DEFAULT_DELAY_MILLISECONDS, public float $rate = self::DEFAULT_RATE, public int $capacity = self::DEFAULT_CAPACITY, public int $handlerMilliseconds = self::DEFAULT_HANDLER_MILLISECONDS, public int $cycles = self::DEFAULT_CYCLES, public int $cycleMessages = self::DEFAULT_CYCLE_MESSAGES, public float $settlingSeconds = self::DEFAULT_SETTLING_SECONDS)
    {
        if (!\in_array($scenario, [Scenario::Roundtrip, Scenario::Idle, Scenario::Delayed, Scenario::FixedRate, Scenario::Application, Scenario::Retention], true)) {
            throw new \InvalidArgumentException('Only roundtrip, idle, delayed, fixed-rate, application and retention are implemented.');
        }
        if ($cycles < self::DEFAULT_CYCLES) {
            throw new \InvalidArgumentException('Retention requires at least 20 cycles.');
        }
        if ($cycles > self::MAX_CYCLES) {
            throw new \InvalidArgumentException('Cycles must not exceed 1000.');
        }
        if ($cycleMessages < 1) {
            throw new \InvalidArgumentException('Cycle messages must be positive.');
        }
        if ($cycleMessages > self::MAX_CYCLE_MESSAGES) {
            throw new \InvalidArgumentException('Cycle messages must not exceed 10000.');
        }
        if (!is_finite($settlingSeconds)) {
            throw new \InvalidArgumentException('Settling must be finite seconds.');
        }
        if ($settlingSeconds <= 0) {
            throw new \InvalidArgumentException('Settling must be positive seconds.');
        }
        if ($settlingSeconds > self::MAX_SETTLING_SECONDS) {
            throw new \InvalidArgumentException('Settling must not exceed 60 seconds.');
        }
        if ($handlerMilliseconds < 0) {
            throw new \InvalidArgumentException('Handler work must be nonnegative milliseconds.');
        }
        if ($handlerMilliseconds > self::MAX_HANDLER_MILLISECONDS) {
            throw new \InvalidArgumentException('Handler work must not exceed 10000 milliseconds.');
        }
        if (!is_finite($rate)) {
            throw new \InvalidArgumentException('Rate must be finite.');
        }
        if ($rate <= 0) {
            throw new \InvalidArgumentException('Rate must be positive.');
        }
        if ($rate > self::MAX_RATE) {
            throw new \InvalidArgumentException('Rate must not exceed 100000 arrivals per second.');
        }
        if ($capacity < 1) {
            throw new \InvalidArgumentException('Capacity must be positive.');
        }
        if ($capacity > self::MAX_CAPACITY) {
            throw new \InvalidArgumentException('Capacity must not exceed 10000 outstanding messages.');
        }
        if (!is_finite($durationSeconds)) {
            throw new \InvalidArgumentException('Duration must be finite.');
        }
        if ($durationSeconds <= 0) {
            throw new \InvalidArgumentException('Duration must be positive.');
        }
        if ($durationSeconds > 3600) {
            throw new \InvalidArgumentException('Duration must not exceed 3600 seconds.');
        }
        if ((int) ($durationSeconds * 1e9) < 1) {
            throw new \InvalidArgumentException('Duration must span at least one monotonic clock nanosecond.');
        }
        if ($repetitions < 1 || $repetitions > 20) {
            throw new \InvalidArgumentException('Repetitions must be within 1..20.');
        }
        if ($delayMilliseconds < 1) {
            throw new \InvalidArgumentException('Delay must be at least one millisecond.');
        }
        if ($delayMilliseconds > self::MAX_DELAY_MILLISECONDS) {
            throw new \InvalidArgumentException('Delay must not exceed 30000 milliseconds.');
        }
    }

    /** @return list<array{id: string, backend: string, repetition: int, scenario: string}> */
    public function schedule(): array
    {
        $schedule = [];
        $repetitions = $this->smoke ? 1 : $this->repetitions;
        for ($repetition = 1; $repetition <= $repetitions; ++$repetition) {
            $backends = 1 === $repetition % 2 ? [Backend::Doctrine, Backend::Broker] : [Backend::Broker, Backend::Doctrine];
            foreach ($backends as $backend) {
                $id = $this->smoke ? $backend->value : $backend->value.'-'.$repetition;
                $schedule[] = ['id' => $id, 'backend' => $backend->value, 'repetition' => $repetition, 'scenario' => $this->scenario->value];
            }
        }

        return $schedule;
    }

    /** @return array<string, mixed> */
    public function configuration(): array
    {
        if (Scenario::Retention === $this->scenario) {
            return ['scenario' => $this->scenario->value, 'mode' => $this->smoke ? 'smoke' : ($this->pilot ? 'pilot' : 'formal'), 'repetitions' => $this->smoke ? 1 : $this->repetitions, 'cycles' => $this->cycles, 'cycle_messages' => $this->cycleMessages, 'settling_seconds' => $this->settlingSeconds, 'topology' => 'one publisher operations connection, one consumer operations and notification connection; same broker and persistence lifetime', 'in_flight_limit' => 1, 'observation' => 'matched-empty-state retention characterization; not leak-free proof'];
        }
        if (\in_array($this->scenario, [Scenario::FixedRate, Scenario::Application], true)) {
            return ['scenario' => $this->scenario->value, 'mode' => $this->smoke ? 'smoke' : ($this->pilot ? 'pilot' : 'formal'), 'duration_seconds' => $this->fixedRateSeconds(), 'repetitions' => $this->smoke ? 1 : $this->repetitions, 'rate' => $this->rate, 'capacity' => $this->capacity, 'handler_milliseconds' => $this->handlerMilliseconds, 'application_interpretation' => Scenario::Application === $this->scenario ? 'synthetic synchronous external-wait followed by Messenger result dispatch; not production or keepalive reproduction' : null, 'topology' => Scenario::Application === $this->scenario ? 'one synchronous publisher, one execution consumer, one result/control consumer' : 'one synchronous publisher in coordinator, one native consumer', 'arrival_schedule' => 'start + floor(index * 1e9 / rate), exclusive window end', 'capacity_unit' => Scenario::Application === $this->scenario ? 'workflows awaiting result ACK; execution ACK verified after drain' : 'messages awaiting ACK', 'overflow_policy' => 'drop overdue arrivals beyond available outstanding capacity; never shift schedule'];
        }

        return ['scenario' => $this->scenario->value, 'mode' => $this->smoke ? 'smoke' : ($this->pilot ? 'pilot' : 'formal'), 'duration_seconds' => $this->durationSeconds, 'repetitions' => $this->smoke ? 1 : $this->repetitions, 'in_flight_limit' => 1, 'delay_milliseconds' => $this->delayMilliseconds, 'delay_applies' => Scenario::Delayed === $this->scenario, 'idle_seconds' => Scenario::Idle === $this->scenario ? $this->idleSeconds() : null, 'finite_cohort_messages' => $this->finiteCohortMessages(), 'duration_scope' => Scenario::Idle === $this->scenario ? 'no-publication interval, followed by separate finite pickup cohort' : 'fixed measurement window outside smoke', 'telemetry_policy' => 'all operation records, including empty receive attempts; bounded buffering', 'phase_budgets_seconds' => ['startup' => \Ineersa\SqliteQueue\Bench\Config::STARTUP_TIMEOUT_S, 'warmup' => $this->warmupTimeoutSeconds(), 'measure' => $this->durationSeconds, 'drain' => $this->drainTimeoutSeconds(), 'audit' => \Ineersa\SqliteQueue\Bench\Config::STARTUP_TIMEOUT_S, 'shutdown' => \Ineersa\SqliteQueue\Bench\Config::KILL_GRACE_S], 'measurement_deadline_policy' => 'no new publish after the fixed window; synchronous API calls retain their own communication or busy timeouts; an outstanding completion drains separately'];
    }

    public function processTimeoutSeconds(): float
    {
        if (Scenario::Retention === $this->scenario) {
            return 3 * \Ineersa\SqliteQueue\Bench\Config::STARTUP_TIMEOUT_S + self::WARMUP_TIMEOUT_SECONDS + $this->cycles * ($this->cycleMessages * \Ineersa\SqliteQueue\Bench\Config::STARTUP_TIMEOUT_S + self::DRAIN_TIMEOUT_SECONDS + $this->settlingSeconds + \Ineersa\SqliteQueue\Bench\Config::STARTUP_TIMEOUT_S) + \Ineersa\SqliteQueue\Bench\Config::KILL_GRACE_S;
        }
        // Includes fixed-window work plus separate acquisition, warmup, drain, audit and shutdown budgets.
        $delayedBudget = Scenario::Delayed === $this->scenario ? (\Ineersa\SqliteQueue\Bench\Runner::WARMUP_MESSAGES + \Ineersa\SqliteQueue\Bench\Runner::WAKEUP_MESSAGES + 1) * $this->delayMilliseconds / 1000 + 3 : 0;

        return $this->durationSeconds + 2 * \Ineersa\SqliteQueue\Bench\Config::STARTUP_TIMEOUT_S + self::WARMUP_TIMEOUT_SECONDS + self::DRAIN_TIMEOUT_SECONDS + \Ineersa\SqliteQueue\Bench\Config::STARTUP_TIMEOUT_S + \Ineersa\SqliteQueue\Bench\Config::KILL_GRACE_S + $delayedBudget;
    }

    public function warmupTimeoutSeconds(): float
    {
        return self::WARMUP_TIMEOUT_SECONDS + (Scenario::Delayed === $this->scenario ? \Ineersa\SqliteQueue\Bench\Runner::WARMUP_MESSAGES * ($this->delayMilliseconds / 1000 + 1) : 0);
    }

    public function drainTimeoutSeconds(): float
    {
        return self::DRAIN_TIMEOUT_SECONDS + (Scenario::Delayed === $this->scenario ? $this->delayMilliseconds / 1000 + 1 : 0);
    }

    public function idleSeconds(): float
    {
        return $this->smoke ? self::SMOKE_IDLE_SECONDS : $this->durationSeconds;
    }

    public function fixedRateSeconds(): float
    {
        return $this->smoke ? self::SMOKE_FIXED_RATE_SECONDS : $this->durationSeconds;
    }

    /** Null means an open-ended message count released only until the fixed window ends. */
    public function finiteCohortMessages(): ?int
    {
        if (Scenario::Idle === $this->scenario) {
            return \Ineersa\SqliteQueue\Bench\Runner::WAKEUP_MESSAGES;
        }
        if (!$this->smoke) {
            return null;
        }

        return Scenario::Delayed === $this->scenario ? \Ineersa\SqliteQueue\Bench\Runner::WAKEUP_MESSAGES : \Ineersa\SqliteQueue\Bench\Runner::MEASURED_MESSAGES;
    }
}
