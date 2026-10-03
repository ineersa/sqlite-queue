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
    /** @var list<Process> Broker infrastructure is accounted but has no message sample stream. */
    private array $infrastructure = [];
    /** @var list<string> Only successfully created private directories may be removed. */
    private array $socketDirectories = [];
    private ?Connection $connection = null;
    private readonly Resources $resources;
    private int $deadline;
    /**
     * @var array<string, mixed>
     */
    private array $facts = [];

    /**
     * @param array<string, mixed> $workload
     */
    public function __construct(
        private readonly string $directory,
        private readonly array $workload,
        private readonly Cancellation $cancellation,
    ) {
        $this->resources = new Resources(getmypid());
    }

    /**
     * @param array<string, mixed> $clockProbe
     *
     * @return array<string, mixed>
     */
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
            $startup = hrtime(true);
            $this->initializeDatabase();
            $this->startChildren();
            $this->facts['startup_ms'] = (hrtime(true) - $startup) / 1e6;
            $this->facts['pickup'] = Backend::Broker === $this->backend() && 1 === \count($this->workload['queues']) ? 'notification_wait_1000ms' : 'poll_1000us';
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
            if (!mkdir($this->directory.$subdirectory, 0700, true)) {
                throw new \RuntimeException('Cannot create repetition directory: '.$this->directory.$subdirectory);
            }
        }
        $config = [
            'directory' => $this->directory,
            'database' => $this->directory.'/queue.sqlite',
            'workload' => $this->workload,
        ];
        file_put_contents($this->directory.'/config.json', json_encode($config, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR));
    }

    private function initializeDatabase(): void
    {
        if (Backend::Broker === $this->backend()) {
            if (!mkdir(\dirname(Backend::endpoint($this->directory)), 0700)) {
                throw new \RuntimeException('Cannot create private benchmark socket directory.');
            }
            $this->socketDirectories[] = \dirname(Backend::endpoint($this->directory));
            $broker = Process::spawn('broker', '0', [\PHP_BINARY, Config::rootDir().'/bin/benchmark', 'broker', $this->directory], [], $this->directory);
            $this->infrastructure[] = $broker;
            $this->facts['broker_readiness'] = $broker->waitForReady(Config::STARTUP_TIMEOUT_S);
            $this->facts['broker_runtime_profiles'] = json_decode(file_get_contents($this->directory.'/broker-runtime.json'), true, 512, \JSON_THROW_ON_ERROR);
            $durability = json_decode(file_get_contents($this->directory.'/broker-durability.json'), true, 512, \JSON_THROW_ON_ERROR);
            $this->facts['broker_durability'] = $durability;
            if (!\is_array($durability) || !Baseline::isDurabilityEquivalent($durability)) {
                throw new \RuntimeException('Broker worker durability mismatch.');
            }
            if (Config::BUSY_TIMEOUT_MS !== $durability['busy_timeout'] || 1000 !== $durability['wal_autocheckpoint']) {
                throw new \RuntimeException('Broker worker checkpoint/lock policy mismatch.');
            }
            $this->resources->sample();
        }
        $this->connection = Baseline::connect($this->directory.'/queue.sqlite');
        if (Backend::Doctrine === $this->backend()) {
            Baseline::transport($this->connection, $this->workload['queues'][0])->setup();
        }
        $this->facts['setup_durability'] = Baseline::durability($this->connection);
    }

    private function backend(): Backend
    {
        return Backend::from($this->workload['backend'] ?? Backend::Doctrine->value);
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
            \PHP_BINARY,
            '-d', 'date.timezone=UTC',
            Config::rootDir().'/bin/benchmark',
            'worker',
            $this->directory.'/config.json',
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
        if ('backlog' === $this->workload['name']) {
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
            $path = $this->directory.'/go/'.$process->role().'-'.$process->argument();
            file_put_contents($path, (string) hrtime(true));
        }
    }

    private function awaitIdleConsumers(): void
    {
        foreach ($this->consumers as $index => $consumer) {
            while (!is_file($this->directory.'/idle-'.$index)) {
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
        while (0 !== array_sum($this->backend()->inventory($this->connection))) {
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

    /**
     * @param list<Process> $processes
     */
    private function anyRunning(array $processes): bool
    {
        return array_any($processes, static fn (Process $process): bool => $process->isRunning());
    }

    private function shutdown(): void
    {
        file_put_contents($this->directory.'/stop', 'stop');
        foreach ($this->children as $process) {
            $process->wait(3.0);
        }
        $this->resources->sample();
        foreach ($this->infrastructure as $process) {
            $process->terminate();
            if (0 !== $process->wait(Config::KILL_GRACE_S + 1)) {
                $this->recordError(new \RuntimeException('Broker infrastructure failed: '.$process->errorTail()));
            }
            if (is_file($this->directory.'/broker-final.json')) {
                $this->facts['broker_final_resources'] = json_decode(file_get_contents($this->directory.'/broker-final.json'), true, 512, \JSON_THROW_ON_ERROR);
            } else {
                $this->recordError(new \RuntimeException('Missing broker final resource accounting.'));
            }
        }
        if (null === $this->connection) {
            return;
        }

        try {
            $this->facts['inventory'] = $this->backend()->inventory($this->connection);
            $this->facts['checkpoint'] = Baseline::checkpoint($this->connection);
        } catch (\Throwable $error) {
            $this->recordError($error);
        } finally {
            $this->connection->close();
        }
    }

    /**
     * @return array<string, mixed>
     */
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
        file_put_contents($this->directory.'/result.json', json_encode($result, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR));

        if ([] === $this->facts['survivors']) {
            (new \Symfony\Component\Filesystem\Filesystem())->remove($this->socketDirectories);
        }

        foreach (['queue.sqlite', 'queue.sqlite-wal', 'queue.sqlite-shm'] as $file) {
            $path = $this->directory.'/'.$file;
            if (is_file($path)) {
                unlink($path);
            }
        }

        return $result;
    }

    /**
     * @return array{role: string, argument: string, path: string, exit_code: int|null, timed_out: bool, killed: bool, stderr: string}
     */
    private function source(Process $process): array
    {
        return [
            'role' => $process->role(),
            'argument' => $process->argument(),
            'path' => $this->directory.'/samples/'.$process->role().'-'.$process->argument().'.jsonl',
            'exit_code' => $process->exitCode(),
            'timed_out' => $process->timedOut(),
            'killed' => $process->killed(),
            'stderr' => $process->errorTail(),
        ];
    }

    private function recordError(\Throwable $error): void
    {
        $this->facts['coordinator_errors'][] = $error::class.': '.$error->getMessage();
    }
}
