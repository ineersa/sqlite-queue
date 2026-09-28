<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Doctrine\DBAL\Connection;
use Ineersa\SqliteQueue\Bench\Child\Role;

/** Runs and tears down one isolated repetition. */
final class Runner
{
    /** @var list<Process> */
    private array $children = [];
    /** @var list<Process> */
    private array $producers = [];
    /** @var list<Process> */
    private array $consumers = [];
    private ?Connection $connection = null;
    private readonly Resources $resources;
    private int $deadline;
    private array $facts = [];

    public function __construct(
        private readonly string $directory,
        private readonly array $workload,
        private readonly Cancellation $cancellation,
    ) {
        $this->resources = new Resources(getmypid());
    }

    public function run(string $name, bool $warmup, array $clockProbe): array
    {
        $this->prepareDirectory();
        $this->deadline = hrtime(true) + (int) (($this->workload['timeout_s'] + Config::STARTUP_TIMEOUT_S) * 1e9);
        $this->facts = [
            'warmup' => $warmup,
            'inventory' => [],
            'coordinator_errors' => [],
            'clock_probe' => $clockProbe,
        ];

        try {
            $this->initializeDatabase();
            $this->startChildren();
            $this->releaseWorkload();
            $this->awaitPublication();
            $this->awaitDrain();
        } catch (\Throwable $error) {
            $this->recordError($error);
        } finally {
            $this->shutdown();
        }

        return $this->report($name);
    }

    private function prepareDirectory(): void
    {
        foreach (['', '/ready', '/go', '/logs', '/samples', '/acks'] as $subdirectory) {
            if (!mkdir($this->directory . $subdirectory, 0700, true)) {
                throw new \RuntimeException('Cannot create repetition directory: ' . $this->directory . $subdirectory);
            }
        }
        $config = [
            'directory' => $this->directory,
            'database' => $this->directory . '/queue.sqlite',
            'workload' => $this->workload,
        ];
        file_put_contents($this->directory . '/config.json', json_encode($config, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR));
    }

    private function initializeDatabase(): void
    {
        $this->connection = Baseline::connect($this->directory . '/queue.sqlite');
        Baseline::transport($this->connection, $this->workload['queues'][0])->setup();
        $this->facts['setup_durability'] = Baseline::durability($this->connection);
    }

    private function startChildren(): void
    {
        for ($index = 0; $index < $this->workload['consumers']; ++$index) {
            $this->consumers[] = $this->spawn(Role::Consumer, $index);
        }
        if (isset($this->workload['prefill'])) {
            $this->producers[] = $this->spawn(Role::Prefill, 0);
        } else {
            foreach (array_keys($this->workload['publishers']) as $index) {
                $this->producers[] = $this->spawn(Role::Publisher, $index);
            }
        }
        $this->resources->sample();
    }

    private function spawn(Role $role, int $index): Process
    {
        $this->cancellation->throwIfRequested();
        $process = Process::spawn($role->value, (string) $index, [
            PHP_BINARY,
            '-d', 'date.timezone=UTC',
            Config::rootDir() . '/bin/benchmark',
            'worker',
            $this->directory . '/config.json',
            $role->value,
            (string) $index,
            '--no-ansi',
            '--no-interaction',
        ], [], $this->directory);
        $this->children[] = $process;
        $process->waitForReady(Config::STARTUP_TIMEOUT_S);

        return $process;
    }

    private function releaseWorkload(): void
    {
        if ($this->workload['name'] === 'backlog') {
            $this->release($this->producers);
            while ($this->anyRunning($this->producers)) {
                $this->tick();
            }
            $this->release($this->consumers);

            return;
        }

        $this->release($this->consumers);
        $this->awaitIdleConsumers();
        $this->measureIdleWindow();
        $this->release($this->producers);
    }

    /** @param list<Process> $processes */
    private function release(array $processes): void
    {
        foreach ($processes as $process) {
            $path = $this->directory . '/go/' . $process->role() . '-' . $process->argument();
            file_put_contents($path, (string) hrtime(true));
        }
    }

    private function awaitIdleConsumers(): void
    {
        foreach ($this->consumers as $index => $consumer) {
            while (!is_file($this->directory . '/idle-' . $index)) {
                if (!$consumer->isRunning()) {
                    throw new \RuntimeException('Consumer exited before idle readiness.');
                }
                $this->tick();
            }
        }
    }

    private function measureIdleWindow(): void
    {
        $startWall = microtime(true);
        $end = hrtime(true) + (int) (($this->workload['idle_ms'] ?? 0) * 1e6);
        while (hrtime(true) < $end) {
            $this->tick();
        }
        if (isset($this->workload['idle_ms'])) {
            $this->facts['idle_window'] = $this->resources->cpuBetween($startWall, microtime(true));
        }
    }

    private function awaitPublication(): void
    {
        while ($this->anyRunning($this->producers)) {
            if (!$this->anyRunning($this->consumers)) {
                throw new \RuntimeException('All consumers exited while publishing.');
            }
            $this->tick();
        }
    }

    private function awaitDrain(): void
    {
        // Only inspect inventory after publication. Do not add DB reads to the publishing window.
        while (array_sum(Baseline::inventory($this->connection)) !== 0) {
            if (!$this->anyRunning($this->consumers)) {
                throw new \RuntimeException('All consumers exited with work pending.');
            }
            $this->tick();
        }
    }

    private function tick(): void
    {
        $this->resources->sample();
        $this->cancellation->throwIfRequested();
        if (hrtime(true) > $this->deadline) {
            throw new \RuntimeException('Repetition exceeded hard deadline.');
        }
        usleep(Config::PROGRESS_INTERVAL_US);
    }

    private function anyRunning(array $processes): bool
    {
        return array_any($processes, static fn (Process $process): bool => $process->isRunning());
    }

    private function shutdown(): void
    {
        file_put_contents($this->directory . '/stop', 'stop');
        foreach ($this->children as $process) {
            $process->wait(3.0);
        }
        $this->resources->sample();
        if ($this->connection === null) {
            return;
        }

        try {
            $this->facts['inventory'] = Baseline::inventory($this->connection);
            $this->facts['checkpoint'] = Baseline::checkpoint($this->connection);
        } catch (\Throwable $error) {
            $this->recordError($error);
        } finally {
            $this->connection->close();
        }
    }

    private function report(string $name): array
    {
        $this->facts['resources'] = $this->resources->summary();
        $this->facts['resources']['accounting'] = 'Sampled child-tree RSS/process count; final child getrusage is in foot records. Coordinator reported separately.';
        $snapshot = ProcessTree::snapshot();
        $this->facts['survivors'] = array_values(array_filter(
            $this->resources->observedPids(),
            static fn (int $pid): bool => isset($snapshot[$pid]) && $pid !== getmypid(),
        ));

        $sources = array_map($this->source(...), $this->children);
        $result = Stats::analyze($this->workload, $name, $sources, $this->facts);
        $result['facts'] = $this->facts;
        $result['workload'] = $this->workload;
        $result['directory'] = $this->directory;
        file_put_contents($this->directory . '/result.json', json_encode($result, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR));

        foreach (['queue.sqlite', 'queue.sqlite-wal', 'queue.sqlite-shm'] as $file) {
            $path = $this->directory . '/' . $file;
            if (is_file($path)) {
                unlink($path);
            }
        }

        return $result;
    }

    private function source(Process $process): array
    {
        return [
            'role' => $process->role(),
            'argument' => $process->argument(),
            'path' => $this->directory . '/samples/' . $process->role() . '-' . $process->argument() . '.jsonl',
            'exit_code' => $process->exitCode(),
            'timed_out' => $process->timedOut(),
            'killed' => $process->killed(),
            'stderr' => $process->errorTail(),
        ];
    }

    private function recordError(\Throwable $error): void
    {
        $this->facts['coordinator_errors'][] = $error::class . ': ' . $error->getMessage();
    }
}
