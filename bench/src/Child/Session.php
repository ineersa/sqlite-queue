<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Child;

use Doctrine\DBAL\Connection;
use Ineersa\SqliteQueue\Bench\Baseline;
use Ineersa\SqliteQueue\Bench\Config;

/** Owns one worker's connection, start barrier, and evidence lifecycle. */
final class Session
{
    public readonly Recorder $recorder;
    public private(set) Connection $connection;
    public private(set) int $startedAt;
    public array $pollTotals = [];

    public function __construct(public readonly Assignment $assignment)
    {
        $this->recorder = new Recorder(
            $assignment->path('samples/' . $assignment->label() . '.jsonl'),
            $assignment->role->value,
            (string) $assignment->index,
        );
    }

    /** @param callable(): void $work */
    public function execute(callable $work): void
    {
        $exitCode = 0;
        try {
            $this->connection = Baseline::connect($this->assignment->database);
            $durability = Baseline::durability($this->connection);
            if (!Baseline::isDurabilityEquivalent($durability)) {
                throw new \RuntimeException('Worker durability mismatch.');
            }
            $this->recorder->header(['durability' => $durability]);
            $this->awaitStart();
            $work();
        } catch (\Throwable $error) {
            $exitCode = 1;
            $this->recorder->error($this->assignment->role->value, $error);
            throw $error;
        } finally {
            if (isset($this->connection)) {
                $this->connection->close();
            }
            $this->recorder->foot([
                'poll_totals' => $this->pollTotals,
                'php_peak_bytes' => memory_get_peak_usage(true),
                'exit_code' => $exitCode,
            ]);
        }
    }

    public function isStopped(): bool
    {
        return is_file($this->assignment->path('stop'));
    }

    public function assertWithinRunBound(): void
    {
        if ($this->isStopped()) {
            throw new \RuntimeException('Run stopped.');
        }
        if (hrtime(true) - $this->startedAt > $this->assignment->workload['timeout_s'] * 1e9) {
            throw new \RuntimeException('Worker exceeded run bound.');
        }
    }

    public function awaitFile(string $path, float $seconds): void
    {
        $deadline = hrtime(true) + (int) ($seconds * 1e9);
        while (!is_file($path)) {
            if (hrtime(true) > $deadline || $this->isStopped()) {
                throw new \RuntimeException('Barrier stopped or timed out: ' . basename($path));
            }
            usleep(1000);
        }
    }

    private function awaitStart(): void
    {
        $assignment = $this->assignment;
        $beforeReady = hrtime(true);
        file_put_contents($assignment->path('ready/' . $assignment->label() . '.ready'), json_encode([
            'event' => 'ready',
            'pid' => getmypid(),
            'clock_ns' => $beforeReady,
        ], JSON_THROW_ON_ERROR));

        $goPath = $assignment->path('go/' . $assignment->label());
        $this->awaitFile($goPath, Config::STARTUP_TIMEOUT_S + $assignment->workload['timeout_s']);
        $parentTime = (int) file_get_contents($goPath);
        $this->startedAt = hrtime(true);
        $valid = $beforeReady <= $parentTime && $parentTime <= $this->startedAt;
        $this->recorder->sample([
            'kind' => 'clock_check',
            'before_ready_ns' => $beforeReady,
            'parent_go_ns' => $parentTime,
            'after_go_ns' => $this->startedAt,
            'valid' => $valid,
        ]);
        if (!$valid) {
            throw new \RuntimeException('Cross-process monotonic clock ordering failed.');
        }
    }
}
