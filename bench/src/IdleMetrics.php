<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Half-open idle window. Attempts are counted at public receive return, not WAIT entry. */
final class IdleMetrics
{
    /** @param iterable<array<string, mixed>> $events
     * @return array<string, mixed>
     */
    public static function summarize(iterable $events, int $start, int $end): array
    {
        if ($end <= $start) {
            throw new \InvalidArgumentException('Idle window must have positive duration.');
        }
        $counts = ['receive_attempts' => 0, 'receive_empty' => 0, 'receive_errors' => 0, 'publication_attempts' => 0, 'unexpected_work_records' => 0];
        foreach ($events as $event) {
            $operation = $event['operation'] ?? '';
            $timestamp = Operation::Send->value === $operation ? ($event['started_ns'] ?? null) : ($event['ended_ns'] ?? null);
            if (!\is_int($timestamp) || $timestamp < $start || $timestamp >= $end) {
                continue;
            }
            if (Operation::Send->value === $operation) {
                ++$counts['publication_attempts'];
            }
            if (Operation::Receive->value === $operation && Role::Consumer->value === ($event['role'] ?? '')) {
                ++$counts['receive_attempts'];
                if (Outcome::Empty->value === ($event['outcome'] ?? '')) {
                    ++$counts['receive_empty'];
                }
                if (Outcome::Error->value === ($event['outcome'] ?? '')) {
                    ++$counts['receive_errors'];
                }
            }
            if (\in_array($operation, [Operation::Delivery->value, Operation::Handler->value], true)) {
                ++$counts['unexpected_work_records'];
            }
        }

        return $counts + ['start_ns' => $start, 'end_ns' => $end, 'window_seconds' => ($end - $start) / 1e9, 'receive_counter_coverage' => 'detailed receive-return boundaries in the declared window', 'counter_scope' => 'consumer public receive returns in [start,end); publisher attempts at invocation', 'wait_registrations' => null, 'wait_wakes' => null, 'wait_timeouts' => null, 'wait_counters_coverage' => 'unavailable; native bundle WAIT is not replaced or internally instrumented', 'integrity_status' => 0 === $counts['publication_attempts'] && 0 === $counts['unexpected_work_records'] && 0 === $counts['receive_errors'] ? IntegrityStatus::Pass->value : IntegrityStatus::Fail->value];
    }
}
