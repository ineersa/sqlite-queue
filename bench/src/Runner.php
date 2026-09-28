<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class Runner
{
    public static function repetition(string $directory, array $workload, string $name, bool $warmup, array $clockProbe): array
    {
        foreach (['', '/ready', '/go', '/logs', '/samples', '/acks'] as $sub) {
            if (!mkdir($directory . $sub, 0700, true) && !is_dir($directory . $sub)) {
                throw new \RuntimeException('Cannot create run directory');
            }
        }
        $config = ['directory' => $directory, 'database' => $directory . '/queue.sqlite', 'workload' => $workload];
        file_put_contents($directory . '/config.json', json_encode($config, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR));
        $children = [];
        $producers = [];
        $consumers = [];
        $resources = new Resources(getmypid());
        $facts = ['warmup' => $warmup, 'inventory' => [], 'coordinator_errors' => [], 'clock_probe' => $clockProbe];
        $connection = null;
        $deadline = hrtime(true) + (int) (($workload['timeout_s'] + Config::STARTUP_TIMEOUT_S) * 1e9);
        $tick = static function () use ($resources, $deadline): void {
            $resources->sample();
            if (!empty($GLOBALS['benchmark_interrupted'])) { throw new \RuntimeException('Benchmark interrupted'); }
            if (hrtime(true) > $deadline) {
                throw new \RuntimeException('Repetition exceeded hard deadline');
            }
            usleep(Config::PROGRESS_INTERVAL_US);
        };
        $spawn = static function (string $role, int $index) use ($directory, &$children): Process {
            $process = Process::spawn($role, (string) $index,
                [PHP_BINARY, '-d', 'date.timezone=UTC', Config::rootDir() . '/bench/child.php', $directory . '/config.json', $role, (string) $index],
                ['PATH' => '/usr/bin:/bin', 'LANG' => 'C', 'TZ' => 'UTC'], $directory);
            $children[] = $process;
            $process->waitForReady(Config::STARTUP_TIMEOUT_S);
            return $process;
        };
        $go = static function (Process $process) use ($directory): void {
            file_put_contents($directory . '/go/' . $process->role() . '-' . $process->argument(), (string) hrtime(true));
        };
        try {
            $connection = Baseline::connect($config['database']);
            Baseline::transport($connection, $workload['queues'][0])->setup();
            $facts['setup_durability'] = Baseline::durability($connection);
            for ($i = 0; $i < $workload['consumers']; ++$i) {
                $consumers[] = $spawn('consumer', $i);
            }
            if (isset($workload['prefill'])) {
                $producers[] = $spawn('prefill', 0);
            } else {
                foreach ($workload['publishers'] as $i => $_) {
                    $producers[] = $spawn('publisher', $i);
                }
            }
            $resources->sample();
            if ($workload['name'] === 'backlog') {
                foreach ($producers as $process) { $go($process); }
                while (array_any($producers, static fn (Process $p): bool => $p->isRunning())) { $tick(); }
            }
            foreach ($consumers as $process) { $go($process); }
            if ($workload['name'] !== 'backlog') {
                // Observe a real empty receive before releasing any publisher.
                for ($i = 0; $i < count($consumers); ++$i) {
                    while (!is_file($directory . '/idle-' . $i)) {
                        if (!$consumers[$i]->isRunning()) { throw new \RuntimeException('Consumer exited before idle readiness'); }
                        $tick();
                    }
                }
                $idleStart = microtime(true);
                $idleEnd = hrtime(true) + (int) (($workload['idle_ms'] ?? 0) * 1e6);
                while (hrtime(true) < $idleEnd) { $tick(); }
                if (isset($workload['idle_ms'])) {
                    $facts['idle_window'] = $resources->cpuBetween($idleStart, microtime(true));
                }
                foreach ($producers as $process) { $go($process); }
            }
            while (array_any($producers, static fn (Process $p): bool => $p->isRunning())) {
                if (!array_any($consumers, static fn (Process $p): bool => $p->isRunning())) {
                    throw new \RuntimeException('All consumers exited while publishing');
                }
                $tick();
            }
            // All sends finished; zero persisted rows means ACK/reject calls have completed.
            while (array_sum(Baseline::inventory($connection)) !== 0) {
                if (!array_any($consumers, static fn (Process $p): bool => $p->isRunning())) {
                    throw new \RuntimeException('All consumers exited with work pending');
                }
                $tick();
            }
        } catch (\Throwable $e) {
            $facts['coordinator_errors'][] = $e::class . ': ' . $e->getMessage();
        } finally {
            file_put_contents($directory . '/stop', 'stop');
            foreach ($children as $process) {
                $process->wait(3.0);
            }
            $resources->sample();
            if ($connection !== null) {
                try {
                    $facts['inventory'] = Baseline::inventory($connection);
                    $facts['checkpoint'] = Baseline::checkpoint($connection);
                } catch (\Throwable $e) {
                    $facts['coordinator_errors'][] = $e->getMessage();
                } finally {
                    $connection->close();
                }
            }
        }
        $facts['resources'] = $resources->summary();
        $facts['resources']['accounting'] = 'Child tree sampled RSS/process count; final per-child getrusage CPU/high-water RSS is in raw foot records. Coordinator reported separately.';
        $snapshot = ProcessTree::snapshot();
        $facts['survivors'] = array_values(array_filter($resources->observedPids(), static fn (int $pid): bool => isset($snapshot[$pid]) && $pid !== getmypid()));
        $sources = [];
        foreach ($children as $process) {
            $sources[] = ['role' => $process->role(), 'argument' => $process->argument(),
                'path' => $directory . '/samples/' . $process->role() . '-' . $process->argument() . '.jsonl',
                'exit_code' => $process->exitCode(), 'timed_out' => $process->timedOut(), 'killed' => $process->killed(), 'stderr' => $process->errorTail()];
        }
        $result = Stats::analyze($workload, $name, $sources, $facts);
        $result['facts'] = $facts;
        $result['workload'] = $workload;
        $result['directory'] = $directory;
        file_put_contents($directory . '/result.json', json_encode($result, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR));
        // Retain only this repetition's evidence. No user-supplied database paths are accepted.
        foreach (['queue.sqlite', 'queue.sqlite-wal', 'queue.sqlite-shm'] as $file) {
            if (is_file($directory . '/' . $file)) { unlink($directory . '/' . $file); }
        }
        return $result;
    }
}
