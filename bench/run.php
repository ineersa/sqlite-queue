<?php

declare(strict_types=1);

require __DIR__ . '/bootstrap.php';

use Ineersa\SqliteQueue\Bench\{ClockProbe, Config, Machine, ProcessTree, Report, Runner};

$options = getopt('', ['smoke', 'workload:']);
$smoke = isset($options['smoke']);
$name = $options['workload'] ?? 'all';
if (PHP_OS_FAMILY !== 'Linux' || !ProcessTree::available() || !extension_loaded('pdo_sqlite') || !extension_loaded('posix') || !extension_loaded('pcntl')) {
    fwrite(STDERR, "Benchmark requires Linux /proc, ext-pdo_sqlite, ext-posix, and ext-pcntl.\n");
    exit(2);
}
$workloads = $name === 'all' ? Config::workloads() : [$name => Config::workload($name)];
pcntl_async_signals(true);
foreach ([SIGINT, SIGTERM] as $signal) {
    pcntl_signal($signal, static function (): void { $GLOBALS['benchmark_interrupted'] = true; });
}
$directory = Config::varDir() . '/' . gmdate('Ymd-His') . '-' . bin2hex(random_bytes(4));
umask(0077);
mkdir($directory, 0700, true);
$kernelClock = Machine::clock();
$ticks = $kernelClock['ticks_per_second'];
$pageSize = $kernelClock['page_size_bytes'];
if ($ticks <= 0 || $pageSize <= 0) { throw new RuntimeException('Cannot determine kernel accounting units'); }
ProcessTree::setTicksPerSecond($ticks);
ProcessTree::setPageSizeBytes($pageSize);
$metadata = Machine::describe($directory, Config::rootDir());
$metadata['clock_probe'] = ClockProbe::run();
$metadata['scheduled_runs'] = count($workloads) * ($smoke ? 1 : Config::WARMUP_REPETITIONS + Config::MEASURED_REPETITIONS);
$metadata['profile'] = $smoke ? 'smoke_not_performance' : 'baseline-v1';
$metadata['settings'] = ['warmup_repetitions' => $smoke ? 0 : Config::WARMUP_REPETITIONS, 'measured_repetitions' => $smoke ? 1 : Config::MEASURED_REPETITIONS,
    'payload_bytes' => [Config::SMALL_PAYLOAD_BYTES, Config::LARGE_PAYLOAD_BYTES], 'poll_sleep_us' => Config::POLL_SLEEP_US,
    'busy_timeout_ms' => Config::BUSY_TIMEOUT_MS, 'redeliver_timeout_s' => Config::REDELIVER_TIMEOUT_S,
    'coordinator_progress_interval_us' => Config::PROGRESS_INTERVAL_US,
    'percentiles' => Config::PERCENTILE_METHOD, 'tail_min_samples' => Config::MIN_TAIL_SAMPLES, 'clock_ticks' => $ticks, 'page_size' => $pageSize];
$hashes = [];
$files = new RecursiveIteratorIterator(new RecursiveDirectoryIterator(__DIR__, FilesystemIterator::SKIP_DOTS));
foreach ($files as $file) {
    if ($file->isFile() && $file->getExtension() === 'php') { $hashes[substr($file->getPathname(), strlen(__DIR__) + 1)] = hash_file('sha256', $file->getPathname()); }
}
ksort($hashes);
$metadata['benchmark_source_hashes'] = $hashes;
$metadata['lock_sha256'] = hash_file('sha256', Config::rootDir() . '/composer.lock');
$runs = [];
fwrite(STDOUT, "Artifacts: $directory\n");
foreach ($workloads as $workload) {
    if ($smoke) {
        foreach ($workload['publishers'] as &$publisher) { $publisher['count'] = Config::SMOKE_PUBLISHER_COUNT; }
        unset($publisher);
        if (isset($workload['prefill'])) { $workload['prefill']['per_queue'] = Config::SMOKE_PREFILL_PER_QUEUE; }
        $workload['timeout_s'] = 20;
    }
    for ($i = $smoke ? 1 : 0; $i <= ($smoke ? 1 : Config::MEASURED_REPETITIONS); ++$i) {
        $repetition = $i === 0 ? 'warmup' : 'run-' . $i;
        if (!empty($GLOBALS['benchmark_interrupted'])) { break 2; }
        $run = Runner::repetition($directory . '/' . $workload['name'] . '-' . $repetition, $workload, $repetition, $i === 0, $metadata['clock_probe']);
        $runs[] = $run;
        Report::write($directory, Report::build($runs, $metadata));
        fwrite(STDOUT, $workload['name'] . '/' . $repetition . ': ' . $run['status'] . "\n");
    }
}
$metadata['coordinator_rusage'] = getrusage();
$metadata['coordinator_php_peak_bytes'] = memory_get_peak_usage(true);
$report = Report::build($runs, $metadata);
Report::write($directory, $report);
exit($report['baseline_status'] === 'complete' ? 0 : 1);
