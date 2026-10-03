<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;

final class Calibration
{
    public const PROPOSED_PERTURBATION_BUDGET = 0.05;

    /** @param array<string, array<string, mixed>> $runs
     * @return array<string, mixed>
     */
    public static function summarize(array $runs, RunOptionsDTO $options): array
    {
        $comparisons = [];
        $exceeded = false;
        foreach ($runs as $run) {
            if (TelemetryLevel::Detailed->value !== $run['telemetry'] || true !== $run['resource_snapshots']) {
                continue;
            }
            $prefix = $run['backend'].'-'.$run['repetition'].'-';
            $pairs = [
                'combined' => [$prefix.'essential-no-resources', $prefix.'detailed-resources'],
                'recorder-only' => [$prefix.'essential-no-resources', $prefix.'detailed-no-resources'],
                'resource-boundaries-only' => [$prefix.'detailed-no-resources', $prefix.'detailed-resources'],
            ];
            foreach ($pairs as $component => [$reference, $observed]) {
                $base = $runs[$reference] ?? [];
                $instrumented = $runs[$observed] ?? [];
                $comparison = ['backend' => $run['backend'], 'repetition' => $run['repetition'], 'component' => $component, 'reference_run' => $reference, 'instrumented_run' => $observed, 'status' => 'unavailable', 'throughput_perturbation_fraction' => null, 'latency_coverage' => []];
                if (!self::healthy($base) || !self::healthy($instrumented)) {
                    $comparison['reason'] = 'Missing, failed or partial paired runs.';
                    $comparisons[] = $comparison;
                    continue;
                }
                $rate = $base['window_unique_completions_per_second'];
                $instrumentedRate = $instrumented['window_unique_completions_per_second'] ?? null;
                if (!is_numeric($rate) || $rate <= 0 || !is_numeric($instrumentedRate)) {
                    $comparison['reason'] = 'A paired rate is unavailable, or the reference rate is zero.';
                    $comparisons[] = $comparison;
                    continue;
                }
                $fraction = ($rate - $instrumentedRate) / $rate;
                $comparison['status'] = 'observed';
                $comparison['throughput_perturbation_fraction'] = $fraction;
                $comparison['observed_budget_exceeded'] = abs($fraction) > self::PROPOSED_PERTURBATION_BUDGET;
                $exceeded = $exceeded || $comparison['observed_budget_exceeded'];
                foreach (['send_ms', 'delivery_ms', 'handler_ms', 'ack_ms', 'full_cycle_ms'] as $metric) {
                    $comparison['latency_coverage'][$metric] = ['reference' => $base['latencies'][$metric], 'instrumented' => $instrumented['latencies'][$metric]];
                }
                $comparisons[] = $comparison;
            }
        }
        $sufficient = !$options->smoke && $options->durationSeconds >= RunOptionsDTO::DEFAULT_DURATION_SECONDS && $options->repetitions >= RunOptionsDTO::DEFAULT_REPETITIONS;
        $partial = false;
        foreach ($comparisons as $comparison) {
            if ('observed' !== $comparison['status']) {
                $partial = true;
            }
        }

        return ['comparisons' => $comparisons, 'proposed_budget_fraction' => self::PROPOSED_PERTURBATION_BUDGET, 'observed_budget_exceeded' => $exceeded, 'budget_status' => ($partial ? CalibrationBudgetStatus::PartialPairs : (!$sufficient ? CalibrationBudgetStatus::InsufficientEvidence : ($exceeded ? CalibrationBudgetStatus::Exceeded : CalibrationBudgetStatus::Within)))->value, 'scope' => 'essential versus detailed recorder and phase-boundary resource snapshots only; integrity remains exact in both', 'latency_limitations' => 'per-run counts retained; empty receive latency absent in essential; no uncertainty estimate or pooled tails', 'unavailable_components' => ['serializer', 'payload-validation', 'control-channel', 'expected-ID journal', 'periodic-resource-sampler', 'whole-system-overhead']];
    }

    /** @param array<string, mixed> $run */
    private static function healthy(array $run): bool
    {
        return ExecutionStatus::Complete->value === ($run['execution_status'] ?? '') && IntegrityStatus::Pass->value === ($run['integrity_status'] ?? '') && AccountingStatus::Complete->value === ($run['accounting_status'] ?? '');
    }
}
