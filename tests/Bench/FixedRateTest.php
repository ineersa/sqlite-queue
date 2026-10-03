<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Analysis;
use Ineersa\SqliteQueue\Bench\Arrivals;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\Scenario;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;

final class FixedRateTest extends TestCase
{
    public function testFloatingCeilBoundaryMatchesActualScheduledTimestamps(): void
    {
        foreach ([[35000000, 200.0, 7], [35000000, 2000.0, 70], [1000000000, 2.5, 3], [200000000, 20.0, 4], [1, 2000000000.0, 2]] as [$window, $rate, $expected]) {
            $arrivals = new Arrivals(100, 100 + $window, $rate);
            $this->assertSame($expected, $arrivals->count());
            for ($index = 0; $index < $arrivals->count(); ++$index) {
                $this->assertLessThan(100 + $window, $arrivals->at($index));
            }
            $this->assertGreaterThanOrEqual(100 + $window, $arrivals->at($arrivals->count()));
            $this->assertSame($expected, $arrivals->admit(100 + $window - 1, 0, $expected)['next']);
        }
    }

    public function testUnrepresentableCountIsRejectedBeforeIntegerConversion(): void
    {
        $arrivals = new Arrivals(0, \PHP_INT_MAX, \PHP_FLOAT_MAX);
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('count exceeds');
        $arrivals->count();
    }

    public function testUnrepresentableFutureOffsetDoesNotOmitTheFirstArrival(): void
    {
        $arrivals = new Arrivals(0, 1, 1e-300);
        $this->assertSame(1, $arrivals->count());
        $this->assertSame(0, $arrivals->at(0));
        $this->expectException(\InvalidArgumentException::class);
        $arrivals->at(1);
    }

    public function testAbsoluteScheduleAndOverflowNeverShiftLaterArrivals(): void
    {
        $arrivals = new Arrivals(1000000000, 2000000000, 10);
        $this->assertSame(10, $arrivals->count());
        $this->assertSame(1300000000, $arrivals->at(3));
        $batch = $arrivals->admit(1350000000, 0, 2);
        $this->assertSame(['indices' => [0, 1], 'next' => 4, 'overflow' => 2], $batch);
        $this->assertSame(1400000000, $arrivals->at($batch['next']));
        $this->assertSame(['indices' => [], 'next' => 4, 'overflow' => 0], $arrivals->admit(1390000000, 4, 2));
        $this->assertSame(['indices' => [], 'next' => 10, 'overflow' => 6], $arrivals->admit(2000000000, 4, 0));
    }

    public function testFractionalRateUsesExclusiveWindowAndUnstartedRemainCountable(): void
    {
        $arrivals = new Arrivals(0, 1000000000, 2.5);
        $this->assertSame(3, $arrivals->count());
        $this->assertSame(800000000, $arrivals->at(2));
        $this->assertSame(['indices' => [0], 'next' => 1, 'overflow' => 0], $arrivals->admit(0, 0, 1));
        $this->assertSame(2, $arrivals->count() - 1);
    }

    public function testOptionsFreezeRateCapacityAndTopologyBeforeRun(): void
    {
        $command = new RunCommand();
        $options = RunCommand::options(new ArrayInput(['--workload' => 'fixed-rate', '--rate' => '7.5', '--capacity' => '3', '--pilot' => true], $command->getDefinition()));
        $this->assertSame(7.5, $options->rate);
        $this->assertSame(3, $options->capacity);
        $this->assertSame(60.0, $options->fixedRateSeconds());
        $this->assertCount(10, $options->schedule());
        $this->assertSame('one synchronous publisher in coordinator, one native consumer', $options->configuration()['topology']);
        $smoke = new RunOptionsDTO(Scenario::FixedRate, true, false, 60, 5);
        $this->assertSame(0.2, $smoke->fixedRateSeconds());
    }

    public function testDiskBackedAnalysisKeepsWindowDenominatorsAndSchedulingLag(): void
    {
        $path = sys_get_temp_dir().'/sq-fixed-analysis-'.bin2hex(random_bytes(6));
        $events = [];
        foreach (['a' => 1500000000, 'b' => 2000000000] as $id => $ack) {
            foreach (['send', 'delivery', 'handler', 'ack'] as $operation) {
                $events[] = ['phase' => 'measure', 'operation' => $operation, 'outcome' => 'success', 'correlation' => $id, 'started_ns' => 1100000000, 'ended_ns' => 'ack' === $operation ? $ack : 1200000000, 'scheduled_ns' => 1000000000, 'payload_bytes' => 256];
            }
        }
        try {
            $summary = Analysis::build($path, $events, ['a', 'b'], 1000000000, 2000000000, Phase::Measure);
            $this->assertSame(2, $summary['unique_completions']);
            $this->assertSame(1, $summary['window_unique_completions']);
            $this->assertSame(2.0, $summary['window_attempts_per_second']);
            $this->assertSame(2.0, $summary['window_confirmations_per_second']);
            $this->assertSame(100.0, $summary['latencies']['schedule_lag_ms']['mean']);
            $this->assertSame(1.0, $summary['cohort_seconds']);
            $this->assertSame(1, $summary['derived_backlog']['confirmed_minus_completed_at_end']);
            $this->assertFalse($summary['derived_backlog']['uncertain']);
        } finally {
            unlink($path);
        }
    }
}
