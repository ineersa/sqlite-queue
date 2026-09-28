<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * Turns raw correlated samples into per-repetition metrics and integrity accounting.
 *
 * Nothing is dropped: failed sends, deliveries without an ACK, unfinished work, corrupt
 * sample lines, and processes that exited non-zero all appear in the result. A metric with
 * too few samples for a tail claim is labeled inconclusive instead of quietly reported.
 */
final class Stats
{
    /**
     * @param array<string, mixed> $workload
     * @param list<array{role: string, argument: string, path: string, exit_code: int|null, timed_out: bool, killed: bool, stderr: string}> $sources
     * @param array<string, mixed> $facts
     * @return array<string, mixed>
     */
    public static function analyze(array $workload, string $repetition, array $sources, array $facts): array
    {
        $sends = [];
        $deliveries = [];
        $failures = [];
        $durability = [];
        $feet = [];
        $corrupt = 0;
        $records = 0;
        $processes = [];
        $clockChecks = [];
        $duplicateSends = 0;
        $pollTotals = ['polls' => 0, 'empty_polls' => 0, 'fetched' => 0, 'poll_ms' => 0.0];

        foreach ($sources as $source) {
            $file = SampleStore::read($source['path']);
            $corrupt += $file['corrupt'];
            $records += \count($file['records']);

            $counts = [];

            foreach ($file['records'] as $record) {
                $kind = (string) ($record['kind'] ?? '');
                $counts[$kind] = ($counts[$kind] ?? 0) + 1;

                switch ($kind) {
                    case 'header':
                        if (\is_array($record['durability'] ?? null)) {
                            $durability[] = ($record['durability']) + [
                                'role' => $source['role'],
                                'argument' => $source['argument'],
                                'proc' => $record['proc'] ?? null,
                            ];
                        }
                        break;

                    case 'send':
                        if (isset($sends[(string) $record['msg']])) { ++$duplicateSends; }
                        $sends[(string) $record['msg']] = $record;
                        if (true !== ($record['ok'] ?? false)) {
                            $failures[] = ['kind' => 'send', 'msg' => $record['msg'], 'error' => $record['error'] ?? 'unknown'];
                        }
                        break;

                    case 'deliver':
                        $deliveries[(string) $record['msg']][] = $record;
                        if ('ack' !== ($record['outcome'] ?? '')) {
                            $failures[] = [
                                'kind' => 'deliver',
                                'msg' => $record['msg'],
                                'outcome' => $record['outcome'] ?? 'unknown',
                                'error' => $record['error'] ?? null,
                            ];
                        }
                        break;

                    case 'error':
                        $failures[] = [
                            'kind' => 'child_error',
                            'where' => $record['where'] ?? 'unknown',
                            'class' => $record['class'] ?? '',
                            'message' => $record['message'] ?? '',
                        ];
                        break;

                    case 'foot':
                        $feet[] = $record;
                        $totals = \is_array($record['poll_totals'] ?? null) ? $record['poll_totals'] : [];
                        foreach (['polls', 'empty_polls', 'fetched', 'poll_ms'] as $key) {
                            $pollTotals[$key] += (float) ($totals[$key] ?? 0);
                        }
                        break;

                    case 'clock_check':
                        $clockChecks[] = $record;
                        break;

                    case 'budget':
                        $failures[] = ['kind' => 'sample_budget_exceeded', 'limit' => $record['limit'] ?? null];
                        break;
                }
            }

            $processes[] = [
                'role' => $source['role'],
                'argument' => $source['argument'],
                'records' => \count($file['records']),
                'kinds' => $counts,
                'sample_file_missing' => $file['missing'],
                'exit_code' => $source['exit_code'],
                'timed_out' => $source['timed_out'],
                'killed' => $source['killed'],
                'stderr_tail' => $source['stderr'],
            ];
        }

        $metrics = self::metrics($sends, $deliveries, $workload, $facts);
        $integrity = self::integrity($workload, $sends, $deliveries, $failures, $facts);
        $durabilityCheck = self::durability($durability);
        if (isset($facts['setup_durability']) && !Baseline::isDurabilityEquivalent($facts['setup_durability'])) {
            $durabilityCheck['verified'] = false;
            $durabilityCheck['violations'][] = 'coordinator setup connection durability mismatch';
        }

        $incomplete = [];
        foreach ($processes as $process) {
            if (true === $process['timed_out']) {
                $incomplete[] = \sprintf('%s:%s timed out', $process['role'], $process['argument']);
            }
            if (null !== $process['exit_code'] && 0 !== $process['exit_code']) {
                $incomplete[] = \sprintf('%s:%s exited with code %d', $process['role'], $process['argument'], $process['exit_code']);
            }
            if (true === $process['sample_file_missing']) {
                $incomplete[] = \sprintf('%s:%s wrote no sample file', $process['role'], $process['argument']);
            }
            if (($process['kinds']['foot'] ?? 0) !== 1 || ($process['kinds']['header'] ?? 0) < 1 || ($process['kinds']['clock_check'] ?? 0) !== 1) {
                $incomplete[] = 'missing lifecycle or clock evidence';
            }
            if ($process['exit_code'] === null) { $incomplete[] = 'missing process exit status'; }
        }
        if ($duplicateSends) { $incomplete[] = 'duplicate send correlation IDs'; }
        if ($failures) { $incomplete[] = 'recorded operation failures'; }
        if (!empty($facts['survivors'])) { $incomplete[] = 'owned processes survived teardown'; }
        foreach ($facts['coordinator_errors'] ?? [] as $error) { $incomplete[] = $error; }
        $clocksValid = ($facts['clock_probe']['verified'] ?? false) === true && count($clockChecks) === count($sources) && $sources !== []
            && array_all($clockChecks, static fn (array $c): bool => ($c['valid'] ?? false) === true
                && $c['before_ready_ns'] <= $c['parent_go_ns'] && $c['parent_go_ns'] <= $c['after_go_ns']);
        if (!$clocksValid) {
            $incomplete[] = 'cross-process clock validation failed';
            foreach (['publish_to_handler_ms', 'full_cycle_ms', 'confirmation_to_handler_ms'] as $key) { $metrics[$key] = self::percentiles([]); }
            foreach (['small', 'large'] as $key) { $metrics['by_payload'][$key]['full_cycle_ms'] = self::percentiles([]); }
        }

        if (0 !== $corrupt) {
            $incomplete[] = \sprintf('%d corrupt sample lines', $corrupt);
        }

        if (!$durabilityCheck['verified']) {
            $incomplete[] = 'effective durability was not WAL plus FULL on every connection';
        }

        $incomplete = [...$incomplete, ...$integrity['incomplete_reasons']];

        return [
            'repetition' => $repetition,
            'warmup' => (bool) ($facts['warmup'] ?? false),
            'status' => [] === $incomplete ? 'complete' : 'incomplete',
            'incomplete_reasons' => \array_values(\array_unique($incomplete)),
            'processes' => $processes,
            'records' => $records,
            'corrupt_lines' => $corrupt,
            'clock_comparable' => $clocksValid,
            'child_final_resources' => array_map(static fn (array $foot): array => ['pid' => $foot['proc'] ?? null, 'rusage' => $foot['rusage'] ?? null, 'php_peak_bytes' => $foot['php_peak_bytes'] ?? null], $feet),
            'metrics' => $metrics,
            'throughput' => $clocksValid ? self::throughput($sends, $deliveries) : null,
            'integrity' => $integrity,
            'durability' => $durabilityCheck + ['observations' => $durability],
            'polls' => $pollTotals,
            'resources' => $facts['resources'] ?? null,
            'idle_window' => $facts['idle_window'] ?? null,
            'inventory' => $facts['inventory'] ?? [],
            'checkpoint' => $facts['checkpoint'] ?? [],
            'failures' => $failures,
            'windows' => $facts['windows'] ?? [],
        ];
    }

    /**
     * @param array<string, array<string, mixed>> $sends
     * @param array<string, list<array<string, mixed>>> $deliveries
     * @param array<string, mixed> $workload
     * @param array<string, mixed> $facts
     * @return array<string, array<string, mixed>>
     */
    private static function metrics(array $sends, array $deliveries, array $workload, array $facts): array
    {
        $sendMs = [];
        $publishToHandlerMs = [];
        $ackInvocationMs = [];
        $deliverMs = [];
        $cycleMs = [];
        $latenessMs = [];
        $scheduleLagMs = [];
        $confirmationToHandlerMs = [];
        $requestedLatenessMs = [];
        $quantizationMs = [];
        $probes = [];
        $byPayload = ['small' => ['send_ms' => [], 'cycle_ms' => []], 'large' => ['send_ms' => [], 'cycle_ms' => []]];
        $delivered = [];

        foreach ($sends as $msg => $send) {
            $confirmed = true === ($send['ok'] ?? false);
            // Probe messages evidence a transport property; they are never part of a measured
            // distribution, so their deadlines stay out of the delayed-lateness samples.
            $measured = false !== ($send['measured'] ?? true);
            if ($confirmed && $measured) {
                $sendMs[] = (float) $send['duration_ms'];
                $size = (int) ($send['size'] ?? 0);
                $bucket = $size <= Config::SMALL_PAYLOAD_BYTES ? 'small' : 'large';
                $byPayload[$bucket]['send_ms'][] = (float) $send['duration_ms'];
            }

            if ($measured && isset($send['schedule_lag_ms'])) {
                $scheduleLagMs[] = (float) $send['schedule_lag_ms'];
            }

            foreach ($deliveries[$msg] ?? [] as $delivery) {
                if (($send['delay_ms'] ?? 0) > 0 && isset($delivery['stored_deadline_wall'], $send['requested_deadline_wall'])) {
                    $quantization = ($delivery['stored_deadline_wall'] - $send['requested_deadline_wall']) * 1000;
                    if ($measured) {
                        $quantizationMs[] = $quantization;
                        $requestedLatenessMs[] = ($delivery['handler_wall'] - $send['requested_deadline_wall']) * 1000;
                    } else {
                        $probes[] = ['msg' => $msg, 'delay_ms' => $send['delay_ms'], 'requested_deadline_wall' => $send['requested_deadline_wall'], 'stored_deadline_wall' => $delivery['stored_deadline_wall'], 'quantization_ms' => $quantization, 'handler_wall' => $delivery['handler_wall']];
                    }
                }
                if (!$measured) { continue; }
                $delivered[$msg] = true;

                if (($delivery['outcome'] ?? '') === 'ack' && \is_numeric($delivery['ack_duration_ms'] ?? null)) {
                    $ackInvocationMs[] = (float) $delivery['ack_duration_ms'];
                }
                if (\is_numeric($delivery['handling_ms'] ?? null)) {
                    $deliverMs[] = (float) $delivery['handling_ms'];
                }
                if ($confirmed && \is_numeric($delivery['t_handler_ns'] ?? null)) {
                    $publishToHandlerMs[] = ((int) $delivery['t_handler_ns'] - (int) $send['t_invoke_ns']) / 1e6;
                    $confirmationToHandlerMs[] = ((int) $delivery['t_handler_ns'] - (int) $send['t_confirm_ns']) / 1e6;
                }
                if ($confirmed && ($delivery['outcome'] ?? '') === 'ack' && \is_numeric($delivery['t_ack_confirm_ns'] ?? null)) {
                    $cycleMs[] = ((int) $delivery['t_ack_confirm_ns'] - (int) $send['t_invoke_ns']) / 1e6;
                    $size = (int) ($send['size'] ?? 0);
                    $byPayload[$size <= Config::SMALL_PAYLOAD_BYTES ? 'small' : 'large']['cycle_ms'][] =
                        ((int) $delivery['t_ack_confirm_ns'] - (int) $send['t_invoke_ns']) / 1e6;
                }
                if ($measured && \is_numeric($delivery['lateness_ms'] ?? null)) {
                    $latenessMs[] = (float) $delivery['lateness_ms'];
                }
            }
        }

        $metrics = [
            'send_ms' => self::percentiles($sendMs),
            'publish_to_handler_ms' => self::percentiles($publishToHandlerMs),
            'ack_ms' => self::percentiles($ackInvocationMs),
            'delivery_handling_ms' => self::percentiles($deliverMs),
            'full_cycle_ms' => self::percentiles($cycleMs),
            'schedule_lag_ms' => self::percentiles($scheduleLagMs),
            'confirmation_to_handler_ms' => self::percentiles($confirmationToHandlerMs),
        ];

        if (isset($workload['prefill']['delay_ms'])) {
            $metrics['delayed_lateness_ms'] = self::percentiles($latenessMs);
            $metrics['requested_deadline_lateness_ms'] = self::percentiles($requestedLatenessMs);
            $metrics['deadline_quantization_ms'] = self::percentiles($quantizationMs);
            $metrics['subsecond_probes'] = $probes;
        }

        $metrics['by_payload'] = [
            'small' => [
                'send_ms' => self::percentiles($byPayload['small']['send_ms']),
                'full_cycle_ms' => self::percentiles($byPayload['small']['cycle_ms']),
            ],
            'large' => [
                'send_ms' => self::percentiles($byPayload['large']['send_ms']),
                'full_cycle_ms' => self::percentiles($byPayload['large']['cycle_ms']),
            ],
        ];

        if (isset($facts['idle_window'])) {
            $metrics['idle_cpu_percent'] = $facts['idle_window'];
        }

        return $metrics;
    }

    /**
     * @param array<string, mixed> $workload
     * @param array<string, array<string, mixed>> $sends
     * @param array<string, list<array<string, mixed>>> $deliveries
     * @param list<array<string, mixed>> $failures
     * @param array<string, mixed> $facts
     * @return array<string, mixed>
     */
    private static function integrity(array $workload, array $sends, array $deliveries, array $failures, array $facts): array
    {
        $expectedSends = Config::expectedSends($workload);
        $expectedMeasuredSends = Config::expectedMeasuredSends($workload);

        $sent = \count($sends);
        $sentOk = \count(\array_filter($sends, static fn (array $send): bool => true === ($send['ok'] ?? false)));
        $deliveredMessages = \array_keys($deliveries);
        $deliveryCount = 0;
        $ackCount = 0;
        $rejectCount = 0;
        $payloadMismatch = 0;
        $duplicateDeliveries = 0;
        $negativeLateness = 0;
        $earlyDeliveries = 0;
        $ackedMessages = [];

        foreach ($deliveries as $msg => $records) {
            $deliveryCount += \count($records);
            if (\count($records) > 1) {
                ++$duplicateDeliveries;
            }
            foreach ($records as $record) {
                $outcome = (string) ($record['outcome'] ?? '');
                if ('ack' === $outcome) {
                    ++$ackCount;
                    if (isset($sends[$msg])) { $ackedMessages[$msg] = true; }
                } elseif ('reject' === $outcome) {
                    ++$rejectCount;
                }
                if (true !== ($record['payload_ok'] ?? false)) {
                    ++$payloadMismatch;
                }
                if (\is_numeric($record['lateness_ms'] ?? null) && (float) $record['lateness_ms'] < 0) {
                    ++$negativeLateness;
                }
                if (true === ($record['delivered_before_deadline'] ?? false)) {
                    ++$earlyDeliveries;
                }
            }
        }

        $unknownDeliveries = \count(\array_diff($deliveredMessages, \array_keys($sends)));
        $missingDeliveries = \count(\array_diff(\array_keys($sends), $deliveredMessages));
        $pendingRows = \array_sum(\is_array($facts['inventory'] ?? null) ? $facts['inventory'] : []);

        $incomplete = [];
        if ($sent !== $expectedSends) {
            $incomplete[] = \sprintf('observed %d sends, workload defines %d', $sent, $expectedSends);
        }
        if ($sentOk !== $expectedSends) {
            $incomplete[] = \sprintf('%d of %d sends were not confirmed', $expectedSends - $sentOk, $expectedSends);
        }
        if ($ackCount !== $expectedSends) {
            $incomplete[] = \sprintf('observed %d ACKs, workload expects %d', $ackCount, $expectedSends);
        }
        if ($missingDeliveries > 0) {
            $incomplete[] = \sprintf('%d messages were never delivered', $missingDeliveries);
        }
        if ($unknownDeliveries > 0) {
            $incomplete[] = \sprintf('%d deliveries had no matching send', $unknownDeliveries);
        }
        if ($duplicateDeliveries > 0) {
            $incomplete[] = \sprintf('%d messages were delivered more than once', $duplicateDeliveries);
        }
        if ($payloadMismatch > 0) {
            $incomplete[] = \sprintf('%d deliveries failed payload verification', $payloadMismatch);
        }
        if ($pendingRows > 0) { $incomplete[] = 'persisted work remains'; }
        if ($earlyDeliveries > 0) { $incomplete[] = 'delivery before stored eligibility'; }

        return [
            'expected_sends' => $expectedSends,
            'expected_measured_sends' => $expectedMeasuredSends,
            'sends' => $sent,
            'sends_confirmed' => $sentOk,
            'messages_delivered' => \count($deliveredMessages),
            'deliveries' => $deliveryCount,
            'acks' => $ackCount,
            'rejects' => $rejectCount,
            'duplicate_deliveries' => $duplicateDeliveries,
            'payload_mismatches' => $payloadMismatch,
            'missing_deliveries' => $missingDeliveries,
            'unknown_deliveries' => $unknownDeliveries,
            'failures' => \count($failures),
            'unfinished' => \max(0, $expectedSends - count($ackedMessages)),
            'pending_rows_after_run' => $pendingRows,
            'deliveries_before_deadline' => $earlyDeliveries,
            'negative_lateness_samples' => $negativeLateness,
            'incomplete_reasons' => $incomplete,
        ];
    }

    /**
     * @param list<array<string, mixed>> $connections
     * @return array{verified: bool, connections: int, violations: list<string>}
     */
    private static function durability(array $connections): array
    {
        $violations = [];

        foreach ($connections as $connection) {
            if (!Baseline::isDurabilityEquivalent([
                'journal_mode' => (string) ($connection['journal_mode'] ?? ''),
                'synchronous' => (int) ($connection['synchronous'] ?? -1),
                'file_backed' => (bool) ($connection['file_backed'] ?? false),
            ])) {
                $violations[] = \sprintf(
                    '%s:%s reported journal_mode=%s synchronous=%s file_backed=%s',
                    $connection['role'] ?? '?',
                    $connection['argument'] ?? '?',
                    \var_export($connection['journal_mode'] ?? null, true),
                    \var_export($connection['synchronous'] ?? null, true),
                    \var_export($connection['file_backed'] ?? null, true),
                );
            }
        }

        return [
            'verified' => [] === $violations && [] !== $connections,
            'connections' => \count($connections),
            'violations' => $violations,
        ];
    }

    /**
     * @param array<string, array<string, mixed>> $sends
     * @param array<string, list<array<string, mixed>>> $deliveries
     * @return array<string, float|null>
     */
    private static function throughput(array $sends, array $deliveries): array
    {
        $sendInvokes = [];
        $sendConfirms = [];
        foreach ($sends as $send) {
            if (true !== ($send['ok'] ?? false)) {
                continue;
            }
            $sendInvokes[] = (int) $send['t_invoke_ns'];
            $sendConfirms[] = (int) $send['t_confirm_ns'];
        }

        $handledWindows = [];
        foreach ($deliveries as $records) {
            foreach ($records as $record) {
                if ('ack' !== ($record['outcome'] ?? '')) {
                    continue;
                }
                $handledWindows[] = [(int) $record['t_handler_ns'], (int) $record['t_ack_confirm_ns']];
            }
        }

        return [
            'publish_seconds' => [] === $sendInvokes ? null : (\max($sendConfirms) - \min($sendInvokes)) / 1e9,
            'publish_per_second' => [] === $sendInvokes ? null : self::rate(\count($sendInvokes), (\max($sendConfirms) - \min($sendInvokes)) / 1e9),
            'handling_seconds' => [] === $handledWindows ? null : (\max(\array_map('max', $handledWindows)) - \min(\array_map('min', $handledWindows))) / 1e9,
            'handled_per_second' => [] === $handledWindows ? null : self::rate(
                \count($handledWindows),
                (\max(\array_map('max', $handledWindows)) - \min(\array_map('min', $handledWindows))) / 1e9,
            ),
        ];
    }

    private static function rate(int $count, float $seconds): ?float
    {
        return $seconds > 0 ? $count / $seconds : null;
    }

    /**
     * Nearest-rank percentiles over the observed samples. No interpolation, no smoothing.
     *
     * @param list<int|float> $values
     * @return array{count: int, min: float|null, p50: float|null, p95: float|null, p99: float|null, max: float|null, mean: float|null, tail: string}
     */
    public static function percentiles(array $values): array
    {
        $values = \array_values(\array_map('floatval', $values));
        \sort($values);

        $count = \count($values);

        if (0 === $count) {
            return [
                'count' => 0,
                'min' => null,
                'p50' => null,
                'p95' => null,
                'p99' => null,
                'max' => null,
                'mean' => null,
                'tail' => 'inconclusive',
            ];
        }

        return [
            'count' => $count,
            'min' => $values[0],
            'p50' => self::percentile($values, 50.0),
            'p95' => self::percentile($values, 95.0),
            'p99' => self::percentile($values, 99.0),
            'max' => $values[$count - 1],
            'mean' => \array_sum($values) / $count,
            'tail' => $count >= Config::MIN_TAIL_SAMPLES ? 'ok' : 'inconclusive',
        ];
    }

    /**
     * @param list<float> $sorted
     */
    public static function percentile(array $sorted, float $percentile): ?float
    {
        $count = \count($sorted);
        if (0 === $count) {
            return null;
        }

        $rank = (int) \ceil($percentile / 100 * $count);

        return $sorted[\max(0, \min($count - 1, $rank - 1))];
    }
}
