<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\FailureFinalization;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\RetentionAnalysis;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

final class RetentionAndFailureTest extends TestCase
{
    public function testStreamingMatchedPointsExcludeInventoryIdentityAndTopologyChanges(): void
    {
        $base = ['role' => 'consumer', 'pid' => 123, 'start_ticks' => 456, 'coverage' => 'observed', 'topology' => 'one publisher, one consumer', 'equivalent_empty_point' => true, 'inventory' => ['remaining' => 0, 'ready' => 0, 'inflight' => 0], 'php_used_bytes' => null];
        $rows = (static function () use ($base): \Generator {
            for ($cycle = 0; $cycle <= 20; ++$cycle) {
                yield $base + ['retention_cycle' => $cycle, 'historical_completions' => $cycle * 2, 'pss_bytes' => 1000 + $cycle];
            }
            yield array_replace($base, ['retention_cycle' => 21, 'historical_completions' => 42, 'pss_bytes' => 99999, 'inventory' => ['remaining' => 1, 'ready' => 0, 'inflight' => 1]]);
            yield array_replace($base, ['retention_cycle' => 22, 'historical_completions' => 44, 'pid' => 999, 'pss_bytes' => 99999]);
            yield array_replace($base, ['retention_cycle' => 23, 'historical_completions' => 46, 'topology' => 'extra client', 'pss_bytes' => 99999]);
        })();
        $summary = RetentionAnalysis::summarize($rows);
        $role = $summary['roles']['consumer'];
        $this->assertSame(21, $role['matched_points']);
        $this->assertSame(3, $role['excluded_points']);
        $this->assertSame(1, $role['identity_changes']);
        $this->assertSame(20, $role['metrics']['pss_bytes']['delta_bytes']);
        $this->assertSame(0.5, $role['metrics']['pss_bytes']['endpoint_bytes_per_historical_completion']);
        $this->assertSame('unavailable', $role['metrics']['php_used_bytes']['coverage']);
        $this->assertNull($role['metrics']['php_used_bytes']['first']);
        $this->assertFalse($summary['forced_gc']);
    }

    public function testOfflineFailureAnalysisRetainsHandlerEntryAndUnknownAckOutcome(): void
    {
        $directory = $this->fixture(false);
        try {
            $summary = FailureFinalization::analyze($directory, ['publisher', 'consumer'], Phase::Measure, 100, 200, false);
            $this->assertSame(1, $summary['handler_starts']);
            $this->assertSame(1, $summary['public_errors']);
            $this->assertSame(1, $summary['unknown_outcome_operations']);
            $this->assertSame(1, $summary['latencies']['handler_ms']['count']);
            $this->assertSame(0, $summary['unique_completions']);
            $this->assertSame('fail', $summary['integrity_status']);
            $this->assertSame('complete', $summary['accounting_status']);
            $this->assertTrue($summary['failure_analysis']['original_failure_preserved']);
            $this->assertArrayNotHasKey('failure', $summary);
        } finally {
            (new Filesystem())->remove($directory);
        }
    }

    public function testMissingFooterAndInterruptedTailCannotProduceCleanPass(): void
    {
        $directory = $this->fixture(true);
        try {
            unlink($directory.'/consumer.operations.jsonl.counters.json');
            file_put_contents($directory.'/consumer.operations.jsonl', '{"operation":"ack"', \FILE_APPEND);
            $summary = FailureFinalization::analyze($directory, ['publisher', 'consumer'], Phase::Measure, 100, 200, false);
            $this->assertSame('partial', $summary['accounting_status']);
            $this->assertSame('unknown', $summary['integrity_status']);
            $this->assertSame(1, $summary['handler_starts']);
            $this->assertSame(1, $summary['unique_completions']);
            $this->assertSame(1, $summary['failure_analysis']['actor_issues']['consumer']['interrupted_or_oversized_records']);
            $this->assertArrayHasKey('footer_error', $summary['failure_analysis']['actor_issues']['consumer']);
        } finally {
            (new Filesystem())->remove($directory);
        }
    }

    public function testTerminalWriteLossCannotPreserveCompleteAccounting(): void
    {
        $directory = $this->fixture(true);
        try {
            $path = $directory.'/consumer.operations.jsonl.counters.json';
            $footer = json_decode(file_get_contents($path), true, flags: \JSON_THROW_ON_ERROR);
            $footer['lost_records'] = 3;
            $footer['write_failures'] = 1;
            file_put_contents($path, json_encode($footer, \JSON_THROW_ON_ERROR));
            $terminal = FailureFinalization::terminalFooters($directory, ['publisher', 'consumer']);
            $this->assertSame(3, $terminal['issues']['consumer']['lost_records']);
            $this->assertSame(1, $terminal['issues']['consumer']['write_failures']);
            $summary = FailureFinalization::analyze($directory, ['publisher', 'consumer'], Phase::Measure, 100, 200, false);
            $this->assertSame('partial', $summary['accounting_status']);
            $this->assertSame('unknown', $summary['integrity_status']);
            $this->assertTrue($summary['failure_analysis']['original_failure_preserved']);
        } finally {
            (new Filesystem())->remove($directory);
        }
    }

    public function testEarlierPhaseFooterCannotMaskAnInterruptedActor(): void
    {
        $directory = $this->fixture(true);
        try {
            $path = $directory.'/consumer.operations.jsonl.counters.json';
            $footer = json_decode(file_get_contents($path), true, flags: \JSON_THROW_ON_ERROR);
            $footer['finalized'] = false;
            file_put_contents($path, json_encode($footer, \JSON_THROW_ON_ERROR));
            $summary = FailureFinalization::analyze($directory, ['publisher', 'consumer'], Phase::Measure, 100, 200, false);
            $this->assertSame('partial', $summary['accounting_status']);
            $this->assertSame('unknown', $summary['integrity_status']);
            $this->assertStringContainsString('not terminal', $summary['failure_analysis']['actor_issues']['consumer']['footer_error']);
        } finally {
            (new Filesystem())->remove($directory);
        }
    }

    public function testMissingActorTelemetryPreservesAvailableSendEvidence(): void
    {
        $directory = $this->fixture(true);
        try {
            unlink($directory.'/consumer.operations.jsonl');
            $summary = FailureFinalization::analyze($directory, ['publisher', 'consumer'], Phase::Measure, 100, 200, false);
            $this->assertSame('partial', $summary['accounting_status']);
            $this->assertSame(1, $summary['window_publish_attempts']);
            $this->assertSame(1, $summary['unfinished']);
            $this->assertTrue($summary['failure_analysis']['actor_issues']['consumer']['telemetry_missing']);
        } finally {
            (new Filesystem())->remove($directory);
        }
    }

    private function fixture(bool $ackSuccess): string
    {
        $directory = sys_get_temp_dir().'/sq-finalization-'.bin2hex(random_bytes(6));
        (new Filesystem())->mkdir($directory);
        file_put_contents($directory.'/expected-ids.txt', "message\n");
        $base = ['phase' => 'measure', 'correlation' => 'message', 'started_ns' => 110, 'ended_ns' => 120, 'payload_bytes' => 256];
        file_put_contents($directory.'/publisher.operations.jsonl', json_encode($base + ['operation' => 'send', 'outcome' => 'success'], \JSON_THROW_ON_ERROR)."\n");
        $consumer = [];
        foreach ([['delivery', 'success'], ['handler', 'enter'], ['handler', 'success'], ['ack', $ackSuccess ? 'success' : 'error']] as [$operation, $outcome]) {
            $consumer[] = json_encode($base + ['operation' => $operation, 'outcome' => $outcome, 'unknown_commit' => 'ack' === $operation && !$ackSuccess], \JSON_THROW_ON_ERROR);
        }
        file_put_contents($directory.'/consumer.operations.jsonl', implode("\n", $consumer)."\n");
        foreach (['publisher', 'consumer'] as $role) {
            file_put_contents($directory.'/'.$role.'.operations.jsonl.counters.json', json_encode(['finalized' => true, 'lost_records' => 0, 'write_failures' => 0, 'buffered_bytes' => 0, 'empty_receives_by_phase' => [], 'empty_duration_bin_upper_ns' => \Ineersa\SqliteQueue\Bench\Recorder::EMPTY_DURATION_BINS_NS], \JSON_THROW_ON_ERROR));
        }

        return $directory;
    }
}
