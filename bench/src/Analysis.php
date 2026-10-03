<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Disk-backed offline correlation. PHP memory is independent of event/message count. */
final class Analysis
{
    private const TRANSACTION_BATCH_EVENTS = 1000;
    private const MIN_TAIL_SAMPLES = 1000;

    /** @param iterable<array<string, mixed>> $events
     * @param iterable<string> $ids actual expected correlations, streamed for measured cohorts
     *
     * @return array<string, mixed>
     */
    public static function build(string $path, iterable $events, iterable $ids, int $start, int $end, Phase $phase): array
    {
        $db = new \SQLite3($path);
        try {
            $db->enableExceptions(true);
            $db->exec('PRAGMA cache_size=-4096');
            $db->exec('PRAGMA temp_store=FILE');
            $db->exec('CREATE TABLE expected (id TEXT PRIMARY KEY)');
            $db->exec('CREATE TABLE observations (id TEXT PRIMARY KEY, send_call INTEGER, send_return INTEGER, scheduled INTEGER, payload_bytes INTEGER, delivery INTEGER, receive_call INTEGER, handler_enter INTEGER, handler_exit INTEGER, ack_call INTEGER, ack_return INTEGER, requested_deadline INTEGER, requested_wall INTEGER, anchor_mono INTEGER, anchor_wall INTEGER, delay_ms INTEGER, deliveries INTEGER NOT NULL DEFAULT 0, acks INTEGER NOT NULL DEFAULT 0, sends INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0)');
            $insert = $db->prepare('INSERT INTO expected VALUES (?)');
            $db->exec('BEGIN');
            foreach ($ids as $id) {
                $insert->bindValue(1, $id, \SQLITE3_TEXT);
                $insert->execute();
            }
            $counters = ['receive_attempts' => 0, 'receive_empty' => 0, 'receive_errors' => 0, 'handler_starts' => 0, 'public_errors' => 0, 'observer_errors' => 0, 'unknown_outcome_operations' => 0];
            $operations = 0;
            foreach ($events as $event) {
                $eventPhase = $event['phase'] ?? '';
                if ($eventPhase !== $phase->value && !(\in_array($phase, [Phase::Measure, Phase::Pickup], true) && Phase::Drain->value === $eventPhase)) {
                    continue;
                }
                $operation = $event['operation'] ?? '';
                $outcome = $event['outcome'] ?? '';
                if ('receive' === $operation) {
                    ++$counters['receive_attempts'];
                    if ('empty' === $outcome) {
                        ++$counters['receive_empty'];
                    }
                    if ('error' === $outcome) {
                        ++$counters['receive_errors'];
                    }
                }
                if ('handler' === $operation && 'enter' === $outcome) {
                    ++$counters['handler_starts'];
                }
                if ('error' === $outcome) {
                    ++$counters['control' === $operation ? 'observer_errors' : 'public_errors'];
                }
                if (true === ($event['unknown_commit'] ?? false)) {
                    ++$counters['unknown_outcome_operations'];
                }
                $id = $event['correlation'] ?? '';
                if (!\is_string($id) || '' === $id) {
                    continue;
                }
                $columns = match ($operation) {
                    'send' => ['send_call' => $event['started_ns'], 'send_return' => 'success' === $outcome ? $event['ended_ns'] : null, 'scheduled' => $event['scheduled_ns'] ?? null],
                    'delivery' => ['delivery' => $event['ended_ns'], 'receive_call' => $event['started_ns']],
                    'handler' => 'enter' === $outcome ? ['handler_enter' => $event['started_ns']] : ('success' === $outcome ? ['handler_exit' => $event['ended_ns']] : []),
                    'ack' => ['ack_call' => $event['started_ns'], 'ack_return' => 'success' === $outcome ? $event['ended_ns'] : null],
                    default => [],
                };
                if ([] === $columns && 'error' !== $outcome) {
                    continue;
                }
                $columns['payload_bytes'] = $event['payload_bytes'] ?? null;
                if (Operation::Send->value === $operation && \is_array($event['eligibility'] ?? null)) {
                    foreach (['requested_monotonic_ns' => 'requested_deadline', 'requested_wall_ms' => 'requested_wall', 'anchor_monotonic_ns' => 'anchor_mono', 'anchor_wall_ms' => 'anchor_wall', 'delay_ms' => 'delay_ms'] as $field => $column) {
                        $columns[$column] = $event['eligibility'][$field] ?? null;
                    }
                }
                $stmt = $db->prepare('INSERT OR IGNORE INTO observations(id) VALUES (?)');
                $stmt->bindValue(1, $id, \SQLITE3_TEXT);
                $stmt->execute();
                if ('send' === $operation || 'error' === $outcome || true === ($event['unknown_commit'] ?? false)) {
                    $column = 'send' === $operation && 'success' === $outcome ? 'sends' : 'failures';
                    $stmt = $db->prepare('UPDATE observations SET '.$column.' = '.$column.' + 1 WHERE id = ?');
                    $stmt->bindValue(1, $id, \SQLITE3_TEXT);
                    $stmt->execute();
                }
                foreach ($columns as $column => $value) {
                    if (!\is_int($value)) {
                        continue;
                    }
                    $stmt = $db->prepare('UPDATE observations SET '.$column.' = CASE WHEN '.$column.' IS NULL THEN ? ELSE MIN('.$column.', ?) END WHERE id = ?');
                    $stmt->bindValue(1, $value, \SQLITE3_INTEGER);
                    $stmt->bindValue(2, $value, \SQLITE3_INTEGER);
                    $stmt->bindValue(3, $id, \SQLITE3_TEXT);
                    $stmt->execute();
                }
                if ('delivery' === $operation || ('ack' === $operation && 'success' === $outcome)) {
                    $column = 'delivery' === $operation ? 'deliveries' : 'acks';
                    $stmt = $db->prepare('UPDATE observations SET '.$column.' = '.$column.' + 1 WHERE id = ?');
                    $stmt->bindValue(1, $id, \SQLITE3_TEXT);
                    $stmt->execute();
                }
                if (0 === ++$operations % self::TRANSACTION_BATCH_EVENTS) {
                    $db->exec('COMMIT; BEGIN');
                }
            }
            $db->exec('COMMIT');
            $valid = ' FROM observations o JOIN expected e ON e.id=o.id WHERE o.send_return IS NOT NULL AND o.delivery IS NOT NULL AND o.handler_exit IS NOT NULL AND o.ack_return IS NOT NULL';
            $completed = (int) $db->querySingle('SELECT COUNT(*)'.$valid);
            $expected = (int) $db->querySingle('SELECT COUNT(*) FROM expected');
            $duplicates = (int) $db->querySingle('SELECT COUNT(*) FROM observations WHERE deliveries > 1 OR acks > 1');
            $unexpected = (int) $db->querySingle('SELECT COUNT(*) FROM observations o LEFT JOIN expected e ON e.id=o.id WHERE e.id IS NULL');
            $window = (int) $db->querySingle('SELECT COUNT(*)'.$valid.' AND ack_return >= '.$start.' AND ack_return < '.$end);
            $last = $db->querySingle('SELECT MAX(ack_return)'.$valid);
            $windowSeconds = ($end - $start) / 1e9;
            $attempts = (int) $db->querySingle('SELECT COUNT(*) FROM observations WHERE send_call >= '.$start.' AND send_call < '.$end);
            $confirmed = (int) $db->querySingle('SELECT COUNT(*) FROM observations WHERE send_return >= '.$start.' AND send_return < '.$end);
            $backlog = ['confirmed_minus_completed_at_end' => $confirmed - $window, 'trend_messages_per_second' => ($confirmed - $window) / $windowSeconds, 'uncertain' => $counters['unknown_outcome_operations'] > 0, 'coverage' => 'derived public confirmations minus unique valid completions, no inventory polling'];
            $strata = ['all' => '', (string) Config::SMALL_PAYLOAD_BYTES => ' AND payload_bytes='.Config::SMALL_PAYLOAD_BYTES, (string) Config::LARGE_PAYLOAD_BYTES => ' AND payload_bytes='.Config::LARGE_PAYLOAD_BYTES];
            $distributions = [];
            foreach ($strata as $stratum => $filter) {
                $latencies = [];
                $source = ' FROM observations WHERE id IN (SELECT id FROM expected)'.$filter;
                $stratumExpected = '' === $filter ? $expected : (int) $db->querySingle('SELECT COUNT(*)'.$source);
                foreach (['send_ms' => 'send_return-send_call', 'receive_ms' => 'delivery-receive_call', 'delivery_ms' => 'delivery-send_call', 'handler_ms' => 'handler_exit-handler_enter', 'ack_ms' => 'ack_return-ack_call', 'full_cycle_ms' => 'ack_return-send_call', 'schedule_lag_ms' => 'send_call-scheduled', 'confirmation_gap_ms' => 'delivery-send_return', 'requested_delivery_lateness_ms' => 'delivery-requested_deadline', 'requested_handler_lateness_ms' => 'handler_enter-requested_deadline'] as $name => $expression) {
                    $count = (int) $db->querySingle('SELECT COUNT('.$expression.')'.$source);
                    $stats = ['count' => $count, 'expected_count' => $stratumExpected, 'missing_boundary_count' => $stratumExpected - $count, 'coverage' => 'available-boundaries-only', 'p50' => null, 'p95' => null, 'p99' => null, 'maximum' => null, 'mean' => null, 'tail' => $count >= self::MIN_TAIL_SAMPLES ? 'observed' : 'insufficient-samples'];
                    if ($count > 0) {
                        foreach (['p50' => 0.5, 'p95' => 0.95, 'p99' => 0.99] as $percentile => $fraction) {
                            $offset = (int) ceil($count * $fraction) - 1;
                            $stats[$percentile] = (float) $db->querySingle('SELECT ('.$expression.') / 1000000.0'.$source.' AND ('.$expression.') IS NOT NULL ORDER BY ('.$expression.') LIMIT 1 OFFSET '.$offset);
                        }
                        $stats['maximum'] = (float) $db->querySingle('SELECT MAX('.$expression.')/1000000.0'.$source);
                        $stats['mean'] = (float) $db->querySingle('SELECT AVG('.$expression.')/1000000.0'.$source);
                    }
                    $latencies[$name] = $stats;
                }

                $distributions[$stratum] = $latencies;
            }

            return $counters + ['window_publish_attempts' => $attempts, 'window_confirmed_publications' => $confirmed, 'window_attempts_per_second' => $attempts / $windowSeconds, 'window_confirmations_per_second' => $confirmed / $windowSeconds, 'derived_backlog' => $backlog, 'expected' => $expected, 'unique_completions' => $completed, 'window_unique_completions' => $window, 'duplicates' => $duplicates, 'unexpected' => $unexpected, 'unfinished' => $expected - $completed, 'window_seconds' => ($end - $start) / 1e9, 'window_unique_completions_per_second' => $window / (($end - $start) / 1e9), 'cohort_seconds' => $expected === $completed && \is_int($last) ? ($last - $start) / 1e9 : null, 'latencies' => $distributions['all'], 'payload_latencies' => $distributions, 'integrity_status' => $expected === $completed && 0 === $duplicates && 0 === $unexpected && 0 === $counters['public_errors'] && 0 === $counters['observer_errors'] && 0 === $counters['unknown_outcome_operations'] ? IntegrityStatus::Pass->value : IntegrityStatus::Fail->value];
        } finally {
            $db->close();
        }
    }
}
