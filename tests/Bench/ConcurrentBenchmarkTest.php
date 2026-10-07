<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Analysis;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Bench\ConcurrentCohort;
use Ineersa\SqliteQueue\Bench\Payload;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;

final class ConcurrentBenchmarkTest extends TestCase
{
    public function testFrozenFiniteCohortAndAlternatingSchedule(): void
    {
        $command = new RunCommand();
        $options = RunCommand::options(new ArrayInput(['--workload' => 'concurrent'], $command->getDefinition()));
        $this->assertSame(3000, $options->configuration()['measured_messages']);
        $this->assertSame(3, $options->configuration()['publishers']);
        $this->assertSame(2, $options->configuration()['consumers']);
        $this->assertSame(['doctrine', 'broker'], array_column($options->schedule(), 'backend'));
        $ids = iterator_to_array(ConcurrentCohort::ids(Phase::Measure, ConcurrentCohort::MESSAGES_PER_PUBLISHER));
        $this->assertCount(3000, array_unique($ids));
        $sizes = array_count_values(array_map(static fn (string $id): int => \strlen(Payload::generate($id)), $ids));
        $this->assertSame([256 => 1500, 16384 => 1500], $sizes);
        $this->assertNotSame(Payload::generate('measure:publisher-0:0'), Payload::generate('measure:publisher-1:0'));
        $database = new IsolatedDatabase();
        try {
            $analysis = Analysis::build($database->path(), [], ConcurrentCohort::ids(Phase::Measure, 1000), 1, 100, Phase::Measure);
            $this->assertSame(3000, $analysis['expected']);
            $this->assertSame(3000, $analysis['unfinished']);
            $this->assertSame('fail', $analysis['integrity_status']);
            $this->assertNull($analysis['cohort_seconds']);
        } finally {
            $database->remove();
        }
    }

    public static function invalidIdentities(): iterable
    {
        yield ['publisher-3', Phase::Measure, 0];
        yield ['publisher-0', Phase::Drain, 0];
        yield ['publisher-0', Phase::Measure, -1];
        yield ['publisher-0', Phase::Measure, 1000];
    }

    #[DataProvider('invalidIdentities')]
    public function testInvalidCorrelationRulesAreRejected(string $actor, Phase $phase, int $index): void
    {
        $this->expectException(\InvalidArgumentException::class);
        ConcurrentCohort::identity($actor, $phase, $index);
    }
}
