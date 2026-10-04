<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Analysis;
use Ineersa\SqliteQueue\Bench\Arrivals;
use Ineersa\SqliteQueue\Bench\Clock;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\Resources;
use Ineersa\SqliteQueue\Bench\Role;
use PHPUnit\Framework\TestCase;

final class InterruptedHelpersTest extends TestCase
{
    public function testArrivalsRetainTheirOriginalSchedule(): void
    {
        $arrivals = new Arrivals(100, 1000000100, 4);
        $this->assertSame(4, $arrivals->count());
        $this->assertSame(100, $arrivals->at(0));
        $this->assertSame(750000100, $arrivals->at(3));
        $this->expectException(\InvalidArgumentException::class);
        $arrivals->at(-1);
    }

    public function testResourcesRejectReusedPidAndKeepMissingMemoryUnknown(): void
    {
        $start = 10;
        $rows = [];
        $read = static function (string $path) use (&$start): string|false {
            if (!str_ends_with($path, '/stat')) {
                return false;
            }
            $fields = array_fill(0, 22, '0');
            $fields[0] = 'S';
            $fields[11] = '200';
            $fields[12] = '100';
            $fields[19] = (string) $start;
            $fields[21] = '3';

            return '123 (name with spaces) '.implode(' ', $fields);
        };
        $resources = new Resources(new Clock(static fn (): int => 123), $read, static function (string $line) use (&$rows): bool {
            $rows[] = json_decode($line, true, flags: \JSON_THROW_ON_ERROR);

            return true;
        }, 100, 4096);
        $resources->register(Role::Consumer, 123);
        $resources->capture('baseline');
        $this->assertSame(2, $rows[0]['user_seconds']);
        $this->assertSame(12288, $rows[0]['rss_bytes']);
        $this->assertNull($rows[0]['pss_bytes']);
        $start = 11;
        $resources->capture('drain');
        $this->assertSame('pid-reused', $rows[1]['coverage']);
        $this->assertNull($rows[1]['rss_bytes']);
    }

    public function testAnalysisCountsUniqueCompletionsAndHalfOpenWindow(): void
    {
        $path = tempnam(sys_get_temp_dir(), 'sq-analysis-');
        $this->assertNotFalse($path);
        $events = [];
        foreach ([0 => 199, 1 => 200] as $index => $ack) {
            foreach ([['send', 'success', 115], ['delivery', 'success', 110], ['handler', 'enter', 111], ['handler', 'success', 120], ['ack', 'success', $ack]] as [$operation, $outcome, $ended]) {
                $events[] = ['phase' => 'ack' === $operation && 1 === $index ? 'drain' : 'measure', 'operation' => $operation, 'outcome' => $outcome, 'correlation' => 'actual:'.$index, 'payload_bytes' => 256, 'started_ns' => 100, 'ended_ns' => $ended];
            }
        }
        $events[] = $events[4];
        try {
            $result = Analysis::build($path, $events, ['actual:0', 'actual:1'], 100, 200, Phase::Measure);
            $this->assertSame(2, $result['unique_completions']);
            $this->assertSame(1, $result['window_unique_completions']);
            $this->assertSame(1, $result['duplicates']);
            $this->assertSame('fail', $result['integrity_status']);
            $this->assertLessThan(0, $result['latencies']['confirmation_gap_ms']['p50']);
            $this->assertSame(2, $result['payload_latencies'][256]['send_ms']['count']);
        } finally {
            unlink($path);
        }
    }

    public function testResourceDeltasAreAssignedToThePrecedingPhase(): void
    {
        $base = ['role' => 'consumer', 'pid' => 7, 'start_ticks' => 9, 'coverage' => 'observed', 'user_seconds' => 1.0, 'system_seconds' => 2.0, 'io' => null, 'pss_bytes' => null];
        $rows = [$base + ['phase' => 'measure'], array_replace($base, ['phase' => 'drain', 'user_seconds' => 4.0, 'system_seconds' => 3.0]), array_replace($base, ['phase' => 'shutdown', 'coverage' => 'unavailable'])];
        $result = Resources::summarize($rows);
        $delta = $result['phase_roles']['measure']['consumer']['until_next_boundary'];
        $this->assertSame(3.0, $delta['user_seconds']);
        $this->assertSame(1.0, $delta['system_seconds']);
        $this->assertNull($delta['io']);
        $this->assertNull($result['phase_roles']['measure']['consumer']['pss_bytes']);
        $this->assertArrayNotHasKey('until_next_boundary', $result['phase_roles']['drain']['consumer']);
    }

    public function testDrainCompletionIsOutsideWindowButCompletesTheActualCohort(): void
    {
        $events = [];
        foreach (['send', 'delivery', 'handler', 'ack'] as $operation) {
            $events[] = ['phase' => 'ack' === $operation ? 'drain' : 'measure', 'operation' => $operation, 'outcome' => 'success', 'correlation' => 'runner-id', 'started_ns' => 100, 'ended_ns' => 'ack' === $operation ? 250 : 150, 'payload_bytes' => 16384];
        }
        $path = tempnam(sys_get_temp_dir(), 'sq-analysis-');
        try {
            $result = Analysis::build($path, $events, ['runner-id'], 100, 200, Phase::Measure);
            $this->assertSame('pass', $result['integrity_status']);
            $this->assertSame(1, $result['unique_completions']);
            $this->assertSame(0, $result['window_unique_completions']);
            $this->assertNotNull($result['cohort_seconds']);
        } finally {
            unlink($path);
        }
        foreach (['missing', 'failed', 'unexpected'] as $fault) {
            $invalid = $events;
            if ('missing' === $fault) {
                array_pop($invalid);
            } elseif ('failed' === $fault) {
                $invalid[3]['outcome'] = 'error';
            } else {
                $invalid[3]['correlation'] = 'different-id';
            }
            $path = tempnam(sys_get_temp_dir(), 'sq-analysis-');
            try {
                $result = Analysis::build($path, $invalid, ['runner-id'], 100, 200, Phase::Measure);
                $this->assertSame('fail', $result['integrity_status'], $fault);
            } finally {
                unlink($path);
            }
        }
    }
}
