<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Conservative range separation, not a significance test or pooled percentile. */
final class Comparison
{
    /**
     * @param list<array<string, mixed>> $runs
     *
     * @return array<string, mixed>
     */
    public static function build(array $runs, int $scheduled): array
    {
        $result = ['status' => ComparisonStatus::Inconclusive->value, 'candidate' => Backend::Broker->value, 'reason' => 'Missing, failed, or tail-insufficient repetitions.', 'ranges' => []];
        if (\count($runs) !== $scheduled || array_any($runs, static fn (array $run): bool => 'complete' !== $run['status'])) {
            return $result;
        }
        $ranges = [];
        $pairs = [];
        foreach ($runs as $run) {
            if (true === $run['warmup'] || 'concurrent' !== $run['workload']['name']) {
                continue;
            }
            $backend = Backend::from($run['workload']['backend']);
            $pairs[$run['repetition']][$backend->value] = true;
            foreach (['small', 'large'] as $size) {
                foreach (['publish_to_handler_ms', 'full_cycle_ms'] as $metric) {
                    $values = $run['metrics']['by_payload'][$size][$metric] ?? [];
                    if (($values['count'] ?? 0) < Config::MIN_TAIL_SAMPLES) {
                        return $result;
                    }
                    foreach (['p95', 'p99'] as $percentile) {
                        $ranges[$size.'/'.$metric.'/'.$percentile][$backend->value][] = $values[$percentile];
                    }
                }
            }
        }
        if (Config::MEASURED_REPETITIONS !== \count($pairs) || array_any($pairs, static fn (array $pair): bool => 2 !== \count($pair))) {
            return $result;
        }
        $win = true;
        $regression = true;
        foreach ($ranges as $metric => $backends) {
            $doctrine = $backends[Backend::Doctrine->value];
            $broker = $backends[Backend::Broker->value];
            $result['ranges'][$metric] = ['doctrine' => [min($doctrine), max($doctrine)], 'broker' => [min($broker), max($broker)]];
            $win = $win && max($broker) < min($doctrine);
            $regression = $regression && min($broker) > max($doctrine);
        }
        $result['status'] = ($win ? ComparisonStatus::Win : ($regression ? ComparisonStatus::Regression : ComparisonStatus::Neutral))->value;
        $result['reason'] = 'All concurrent payload-specific p95/p99 pickup and full-cycle ranges must separate; overlapping or mixed ranges are neutral. Resource tradeoffs require review.';

        return $result;
    }
}
