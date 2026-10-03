<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Calibration;
use Ineersa\SqliteQueue\Bench\CohortJournal;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Ineersa\SqliteQueue\Bench\Manifest;
use Ineersa\SqliteQueue\Bench\Scenario;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;

final class RoundtripConfigurationTest extends TestCase
{
    public function testCalibrationScheduleIsFrozenAndAlternatesBothOrders(): void
    {
        $options = new RunOptionsDTO(Scenario::Calibration, false, true, 0.1, 2);
        $schedule = $options->schedule();
        $this->assertCount(16, $schedule);
        $this->assertSame('doctrine', $schedule[0]['backend']);
        $this->assertSame('broker', $schedule[1]['backend']);
        $this->assertSame('broker', $schedule[8]['backend']);
        $this->assertSame('broker-2-essential-resources', $schedule[8]['id']);
        $this->assertCount(16, array_unique(array_column($schedule, 'id')));
        $this->assertSame($schedule, $options->schedule());
        Manifest::assertSourceMode($options, '?? unrelated-user-file');
        $this->assertSame('pilot', $options->configuration()['mode']);
    }

    #[DataProvider('invalidOptions')]
    public function testEachInvalidOptionFailsBeforeAcquisition(array $arguments): void
    {
        $command = new RunCommand();
        $this->expectException(\InvalidArgumentException::class);
        RunCommand::options(new ArrayInput($arguments, $command->getDefinition()));
    }

    public static function invalidOptions(): iterable
    {
        yield 'nonnumeric duration' => [['--duration' => 'invalid']];
        yield 'zero duration' => [['--duration' => '0']];
        yield 'unrepresentable duration' => [['--duration' => '1e-12']];
        yield 'negative duration' => [['--duration' => '-1']];
        yield 'duration limit' => [['--duration' => '3601']];
        yield 'nonfinite duration' => [['--duration' => '1e999']];
        yield 'fractional repetition' => [['--repetitions' => '1.5']];
        yield 'zero repetitions' => [['--repetitions' => '0']];
        yield 'repetition limit' => [['--repetitions' => '21']];
        yield 'unimplemented workload' => [['--workload' => 'all']];
        yield 'nonnumeric rate' => [['--rate' => 'invalid']];
        yield 'zero rate' => [['--rate' => '0']];
        yield 'rate limit' => [['--rate' => '100001']];
        yield 'nonfinite rate' => [['--rate' => '1e999']];
        yield 'fractional capacity' => [['--capacity' => '1.5']];
        yield 'zero capacity' => [['--capacity' => '0']];
        yield 'capacity limit' => [['--capacity' => '10001']];
        yield 'nonnumeric handler work' => [['--handler-ms' => 'invalid']];
        yield 'fractional handler work' => [['--handler-ms' => '1.5']];
        yield 'negative handler work' => [['--handler-ms' => '-1']];
        yield 'handler work limit' => [['--handler-ms' => '10001']];
        yield 'nonnumeric delay' => [['--delay' => 'invalid']];
        yield 'fractional delay' => [['--delay' => '1.5']];
        yield 'zero delay' => [['--delay' => '0']];
        yield 'negative delay' => [['--delay' => '-1']];
        yield 'delay limit' => [['--delay' => '30001']];
    }

    public function testStreamingJournalPreservesEveryActualCorrelation(): void
    {
        $path = sys_get_temp_dir().'/sq-journal-'.bin2hex(random_bytes(6));
        $journal = CohortJournal::create($path);
        try {
            for ($index = 0; $index < 10000; ++$index) {
                $journal->append('actual:'.$index);
            }
            $count = 0;
            $matched = true;
            foreach ($journal->ids() as $id) {
                $matched = $matched && 'actual:'.$count === $id;
                ++$count;
            }
            $this->assertTrue($matched);
            $this->assertSame(10000, $count);
        } finally {
            $journal->close();
            unlink($path);
        }
    }

    public function testCalibrationFlagsBudgetAndRetainsFailedPairWithoutRetuning(): void
    {
        $options = new RunOptionsDTO(Scenario::Calibration, false, false, 60, 5);
        $runs = [];
        foreach ($options->schedule() as $entry) {
            $runs[$entry['id']] = $entry + ['execution_status' => 'complete', 'integrity_status' => 'pass', 'accounting_status' => 'complete', 'window_unique_completions_per_second' => $entry['resource_snapshots'] ? 80 : 100, 'latencies' => array_fill_keys(['send_ms', 'delivery_ms', 'handler_ms', 'ack_ms', 'full_cycle_ms'], ['count' => 100, 'p50' => 1])];
        }
        $result = Calibration::summarize($runs, $options);
        $this->assertTrue($result['observed_budget_exceeded']);
        $this->assertSame('exceeds-proposed-budget', $result['budget_status']);
        $this->assertCount(30, $result['comparisons']);
        $runs['doctrine-1-essential-no-resources']['execution_status'] = 'failed';
        $result = Calibration::summarize($runs, $options);
        $this->assertSame('partial-pairs', $result['budget_status']);
        $this->assertSame('unavailable', $result['comparisons'][0]['status']);
        $this->assertCount(40, $runs);
    }
}
