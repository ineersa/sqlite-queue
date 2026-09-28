<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Owns the capture schedule and writes a report after every repetition. */
final readonly class Benchmark
{
    public function __construct(private Cancellation $cancellation)
    {
    }

    /** @param callable(string): void $progress */
    public function run(string $workloadName, bool $smoke, callable $progress): array
    {
        $this->checkPlatform();
        $workloads = $this->workloads($workloadName, $smoke);
        $directory = $this->createDirectory();
        $metadata = $this->metadata($directory, $smoke, count($workloads));
        $runs = [];

        $progress('Artifacts: ' . $directory);
        foreach ($workloads as $workload) {
            for ($index = $smoke ? 1 : 0; $index <= ($smoke ? 1 : Config::MEASURED_REPETITIONS); ++$index) {
                if ($this->cancellation->isRequested()) {
                    break 2;
                }

                $name = $index === 0 ? 'warmup' : 'run-' . $index;
                $runner = new Runner(
                    $directory . '/' . $workload['name'] . '-' . $name,
                    $workload,
                    $this->cancellation,
                );
                $run = $runner->run($name, $index === 0, $metadata['clock_probe']);
                $runs[] = $run;
                Report::write($directory, Report::build($runs, $metadata));
                $progress($workload['name'] . '/' . $name . ': ' . $run['status']);
            }
        }

        $metadata['coordinator_rusage'] = getrusage();
        $metadata['coordinator_php_peak_bytes'] = memory_get_peak_usage(true);
        $report = Report::build($runs, $metadata);
        Report::write($directory, $report);

        return $report;
    }

    private function checkPlatform(): void
    {
        if (PHP_OS_FAMILY !== 'Linux' || !ProcessTree::available()) {
            throw new \RuntimeException('Benchmark process accounting requires Linux /proc.');
        }

        foreach (['pdo_sqlite', 'posix', 'pcntl'] as $extension) {
            if (!extension_loaded($extension)) {
                throw new \RuntimeException('Benchmark requires ext-' . $extension . '.');
            }
        }
    }

    private function workloads(string $name, bool $smoke): array
    {
        $workloads = $name === 'all' ? Config::workloads() : [$name => Config::workload($name)];
        if (!$smoke) {
            return $workloads;
        }

        foreach ($workloads as &$workload) {
            foreach ($workload['publishers'] as &$publisher) {
                $publisher['count'] = Config::SMOKE_PUBLISHER_COUNT;
            }
            unset($publisher);

            if (isset($workload['prefill'])) {
                $workload['prefill']['per_queue'] = Config::SMOKE_PREFILL_PER_QUEUE;
            }
            $workload['timeout_s'] = 20;
        }

        return $workloads;
    }

    private function createDirectory(): string
    {
        umask(0077);
        $directory = Config::varDir() . '/' . gmdate('Ymd-His') . '-' . bin2hex(random_bytes(4));
        if (!mkdir($directory, 0700, true)) {
            throw new \RuntimeException('Cannot create capture directory: ' . $directory);
        }

        return $directory;
    }

    private function metadata(string $directory, bool $smoke, int $workloadCount): array
    {
        $metadata = Machine::describe($directory, Config::rootDir());
        $kernelClock = $metadata['kernel_clock'];
        ProcessTree::setTicksPerSecond($kernelClock['ticks_per_second']);
        ProcessTree::setPageSizeBytes($kernelClock['page_size_bytes']);

        $metadata['clock_probe'] = ClockProbe::run();
        $metadata['scheduled_runs'] = $workloadCount * ($smoke ? 1 : Config::WARMUP_REPETITIONS + Config::MEASURED_REPETITIONS);
        $metadata['profile'] = $smoke ? 'smoke_not_performance' : 'baseline-v1';
        $metadata['runner'] = 'symfony-console';
        $metadata['settings'] = [
            'warmup_repetitions' => $smoke ? 0 : Config::WARMUP_REPETITIONS,
            'measured_repetitions' => $smoke ? 1 : Config::MEASURED_REPETITIONS,
            'payload_bytes' => [Config::SMALL_PAYLOAD_BYTES, Config::LARGE_PAYLOAD_BYTES],
            'poll_sleep_us' => Config::POLL_SLEEP_US,
            'busy_timeout_ms' => Config::BUSY_TIMEOUT_MS,
            'redeliver_timeout_s' => Config::REDELIVER_TIMEOUT_S,
            'coordinator_progress_interval_us' => Config::PROGRESS_INTERVAL_US,
            'percentiles' => Config::PERCENTILE_METHOD,
            'tail_min_samples' => Config::MIN_TAIL_SAMPLES,
            'clock_ticks' => $kernelClock['ticks_per_second'],
            'page_size' => $kernelClock['page_size_bytes'],
        ];
        $metadata['benchmark_source_hashes'] = $this->sourceHashes();
        $metadata['lock_sha256'] = hash_file('sha256', Config::rootDir() . '/composer.lock');

        return $metadata;
    }

    private function sourceHashes(): array
    {
        $root = Config::rootDir();
        $hashes = ['bin/benchmark' => hash_file('sha256', $root . '/bin/benchmark')];
        $files = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($root . '/bench/src', \FilesystemIterator::SKIP_DOTS));
        foreach ($files as $file) {
            if ($file->isFile() && $file->getExtension() === 'php') {
                $hashes[substr($file->getPathname(), strlen($root) + 1)] = hash_file('sha256', $file->getPathname());
            }
        }
        ksort($hashes);

        return $hashes;
    }
}
