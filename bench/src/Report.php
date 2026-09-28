<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class Report
{
    public static function build(array $runs, array $metadata): array
    {
        $variation = [];
        foreach ($runs as $run) {
            if ($run['warmup']) {
                continue;
            }
            $name = $run['workload']['name'];
            foreach (['send_ms', 'publish_to_handler_ms', 'ack_ms', 'full_cycle_ms', 'delayed_lateness_ms'] as $metric) {
                if (isset($run['metrics'][$metric])) {
                    $variation[$name][$metric][] = ['repetition' => $run['repetition'], 'status' => $run['status']] + $run['metrics'][$metric];
                }
            }
        }
        return ['schema_version' => Config::SCHEMA_VERSION, 'comparison' => ['status' => 'incomplete_baseline_only', 'candidate' => null],
            'baseline_status' => $runs !== [] && count($runs) === ($metadata['scheduled_runs'] ?? count($runs)) && array_all($runs, static fn (array $r): bool => $r['status'] === 'complete') ? 'complete' : 'incomplete',
            'unexecuted_runs' => max(0, ($metadata['scheduled_runs'] ?? count($runs)) - count($runs)),
            'metadata' => $metadata, 'runs' => $runs, 'per_run_variation' => $variation];
    }

    public static function write(string $directory, array $report): void
    {
        file_put_contents($directory . '/summary.json', json_encode($report, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR));
        $text = "# SQLite baseline\n\nBaseline only. The candidate has not been measured. A/B comparison is incomplete.\n\n";
        $text .= "Baseline status: " . $report['baseline_status'] . ". All scheduled runs, including warmup and failures, are retained.\n\n";
        $text .= "| Workload/run | Status | Metric, ms | n | p50 | p95 | p99 | max | Tail evidence |\n| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |\n";
        foreach ($report['runs'] as $run) {
            foreach ($run['metrics'] as $metric => $values) {
                if (!isset($values['count'])) {
                    continue;
                }
                $numbers = array_map(static fn ($v): string => $v === null ? 'n/a' : sprintf('%.3f', $v), [$values['p50'], $values['p95'], $values['p99'], $values['max']]);
                $text .= '| ' . $run['workload']['name'] . '/' . $run['repetition'] . ' | ' . $run['status'] . ' | ' . $metric . ' | ' . $values['count'] . ' | ' . implode(' | ', $numbers) . ' | ' . $values['tail'] . " |\n";
            }
            if ($run['incomplete_reasons']) {
                $text .= "\nFailure: " . implode('; ', $run['incomplete_reasons']) . "\n\n";
            }
        }
        $text .= "\n## Accounting by repetition\n\n| Workload/run | Confirmed sends | ACKs | Unfinished | Failures | Handled/s | Sampled peak child processes | Sampled peak child RSS, KiB |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n";
        foreach ($report['runs'] as $run) {
            $i = $run['integrity'];
            $text .= sprintf(
                "| %s/%s | %d | %d | %d | %d | %s | %s | %s |\n",
                $run['workload']['name'],
                $run['repetition'],
                $i['sends_confirmed'],
                $i['acks'],
                $i['unfinished'],
                $i['failures'],
                isset($run['throughput']['handled_per_second']) ? sprintf('%.2f', $run['throughput']['handled_per_second']) : 'n/a',
                $run['resources']['peak_processes'] ?? 'n/a',
                $run['resources']['peak_tree_rss_kb'] ?? 'n/a'
            );
        }
        $text .= "\nRaw samples, per-payload metrics, throughput, resource usage, polling, effective durability, configuration, and integrity counts are in summary.json and each repetition directory. No samples are trimmed or pooled across repetitions.\n";
        file_put_contents($directory . '/report.md', $text);
    }
}
