<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

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
    public function testScheduleHasOnlyTwoBackendsPerFrozenPairAndAlternatesOrder(): void
    {
        $options = new RunOptionsDTO(Scenario::Roundtrip, false, true, 0.1, 2);
        $schedule = $options->schedule();
        $this->assertCount(4, $schedule);
        $this->assertSame(['doctrine', 'broker', 'broker', 'doctrine'], array_column($schedule, 'backend'));
        $this->assertSame(['doctrine-1', 'broker-1', 'broker-2', 'doctrine-2'], array_column($schedule, 'id'));
        $this->assertSame(['id', 'backend', 'repetition', 'scenario'], array_keys($schedule[0]));
        $this->assertSame($schedule, $options->schedule());
        Manifest::assertSourceMode($options, '?? unrelated-user-file');
        $this->assertSame('pilot', $options->configuration()['mode']);
    }

    public function testDefaultCommandIsSixtySecondsAndOnePairedRepetition(): void
    {
        $command = new RunCommand();
        $options = RunCommand::options(new ArrayInput([], $command->getDefinition()));
        $this->assertSame(60.0, $options->durationSeconds);
        $this->assertSame(1, $options->repetitions);
        $this->assertCount(2, $options->schedule());
        $this->assertSame(['doctrine-1', 'broker-1'], array_column($options->schedule(), 'id'));
        $this->assertArrayNotHasKey('telemetry_profiles', $options->configuration());
    }

    public function testCastorDefaultsMatchTheSinglePairCommand(): void
    {
        require_once \dirname(__DIR__, 2).'/.castor/tasks.php';
        $function = new \ReflectionFunction('SqliteQueueTasks\\bench');
        $defaults = [];
        foreach ($function->getParameters() as $parameter) {
            $defaults[$parameter->getName()] = $parameter->getDefaultValue();
        }
        $this->assertSame(60.0, $defaults['duration']);
        $this->assertSame(1, $defaults['repetitions']);
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
        yield 'removed calibration' => [['--workload' => 'calibration']];
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
}
