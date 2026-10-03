<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Offline disk-backed execution-to-result correlation, never used on the measured path. */
final class WorkflowAnalysis
{
    public const RESULT_SUFFIX = ':result';
    private const MIN_TAIL_SAMPLES = 1000;

    /** @param array<string, mixed> $messages
     * @return array<string, mixed>
     */
    public static function apply(string $path, array $messages, int $start, int $end): array
    {
        $db = new \SQLite3($path);
        try {
            $db->enableExceptions(true);
            $db->exec('PRAGMA cache_size=-4096');
            $db->exec('PRAGMA temp_store=FILE');
            $suffix = \strlen(self::RESULT_SUFFIX);
            $roots = ' FROM expected e WHERE substr(e.id, -'.$suffix.") != ':result'";
            $expected = (int) $db->querySingle('SELECT COUNT(*)'.$roots);
            $join = " FROM expected e JOIN observations root ON root.id=e.id JOIN observations result ON result.id=e.id || ':result' WHERE substr(e.id, -".$suffix.") != ':result'";
            $clean = $join;
            foreach (['root', 'result'] as $actor) {
                $clean .= ' AND '.$actor.'.sends=1 AND '.$actor.'.deliveries=1 AND '.$actor.'.acks=1 AND '.$actor.'.failures=0 AND '.$actor.'.send_return IS NOT NULL AND '.$actor.'.handler_enter IS NOT NULL AND '.$actor.'.handler_exit IS NOT NULL AND '.$actor.'.ack_return IS NOT NULL';
            }
            $clean .= ' AND root.send_call IS NOT NULL AND root.payload_bytes=result.payload_bytes';
            $completed = (int) $db->querySingle('SELECT COUNT(*)'.$clean);
            // A result can be ACKed before the execution worker settles its root message.
            $completion = 'MAX(root.ack_return, result.ack_return)';
            $window = (int) $db->querySingle('SELECT COUNT(*)'.$clean.' AND '.$completion.' >= '.$start.' AND '.$completion.' < '.$end);
            $last = $db->querySingle('SELECT MAX('.$completion.')'.$clean);
            $rootSource = ' FROM expected e JOIN observations root ON root.id=e.id WHERE substr(e.id, -'.$suffix.") != ':result'";
            $attempts = (int) $db->querySingle('SELECT COUNT(*)'.$rootSource.' AND root.send_call >= '.$start.' AND root.send_call < '.$end);
            $confirmed = (int) $db->querySingle('SELECT COUNT(*)'.$rootSource.' AND root.send_return >= '.$start.' AND root.send_return < '.$end);
            $windowSeconds = ($end - $start) / 1e9;
            $latencies = [];
            foreach (['all' => '', (string) Config::SMALL_PAYLOAD_BYTES => ' AND root.payload_bytes='.Config::SMALL_PAYLOAD_BYTES, (string) Config::LARGE_PAYLOAD_BYTES => ' AND root.payload_bytes='.Config::LARGE_PAYLOAD_BYTES] as $stratum => $filter) {
                $source = $clean.$filter;
                $count = (int) $db->querySingle('SELECT COUNT(*)'.$source);
                $stats = ['count' => $count, 'p50' => null, 'p95' => null, 'p99' => null, 'maximum' => null, 'mean' => null, 'tail' => $count >= self::MIN_TAIL_SAMPLES ? 'observed' : 'insufficient-samples'];
                if ($count > 0) {
                    $expression = '(result.ack_return-root.send_call)/1000000.0';
                    foreach (['p50' => 0.5, 'p95' => 0.95, 'p99' => 0.99] as $name => $fraction) {
                        $offset = (int) ceil($count * $fraction) - 1;
                        $stats[$name] = (float) $db->querySingle('SELECT '.$expression.$source.' ORDER BY '.$expression.' LIMIT 1 OFFSET '.$offset);
                    }
                    $stats['maximum'] = (float) $db->querySingle('SELECT MAX('.$expression.')'.$source);
                    $stats['mean'] = (float) $db->querySingle('SELECT AVG('.$expression.')'.$source);
                }
                $latencies[$stratum] = $stats;
            }
            $messages['message_accounting'] = ['expected' => $messages['expected'], 'unique_completions' => $messages['unique_completions'], 'window_unique_completions' => $messages['window_unique_completions'], 'window_unique_completions_per_second' => $messages['window_unique_completions_per_second'], 'unfinished' => $messages['unfinished'], 'cohort_seconds' => $messages['cohort_seconds']];
            $messages['total_message_ack_completions'] = $messages['unique_completions'];
            $messages['expected'] = $expected;
            $messages['unique_completions'] = $completed;
            $messages['unfinished'] = $expected - $completed;
            $messages['window_unique_completions'] = $window;
            $messages['window_unique_completions_per_second'] = $window / (($end - $start) / 1e9);
            $messages['cohort_seconds'] = $expected === $completed && \is_int($last) ? ($last - $start) / 1e9 : null;
            $messages['workflow_latency_ms'] = $latencies['all'];
            $messages['workflow_payload_latency_ms'] = $latencies;
            $messages['workflow_window_attempts_per_second'] = $attempts / $windowSeconds;
            $messages['workflow_window_confirmations_per_second'] = $confirmed / $windowSeconds;
            $messages['workflow_derived_backlog'] = ['confirmed_roots_minus_completed' => $confirmed - $window, 'trend_workflows_per_second' => ($confirmed - $window) / $windowSeconds, 'uncertain' => $messages['unknown_outcome_operations'] > 0, 'coverage' => 'public root confirmations minus fully settled clean workflows; no inventory polling'];
            $messages['workflow_window_boundary'] = 'later of root ACK and result ACK; end-to-end latency is root send call to result ACK';
            $messages['completion_interpretation'] = 'unique clean root-to-result-ACK workflows; both sends, handlers, deliveries and ACKs required once; public message counters include intermediate messages';
            if ($expected !== $completed) {
                $messages['integrity_status'] = IntegrityStatus::Fail->value;
            }

            return $messages;
        } finally {
            $db->close();
        }
    }
}
