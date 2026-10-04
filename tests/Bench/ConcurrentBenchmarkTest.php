<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Analysis;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Bench\ConcurrentCohort;
use Ineersa\SqliteQueue\Bench\Payload;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\Recorder;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Process\Process;

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

    public static function modes(): iterable
    {
        yield ['normal'];
        yield ['full'];
    }

    #[DataProvider('modes')]
    public function testNativeSmokeWarmsEveryActorAndKeepsOwningIdentities(string $mode): void
    {
        $root = \dirname(__DIR__, 2);
        $process = new Process([\PHP_BINARY, $root.'/bin/benchmark', 'run', '--smoke', '--workload=concurrent', '--synchronous='.$mode], $root, timeout: 120);
        $process->mustRun();
        $output = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
        $summary = json_decode(file_get_contents($output['capture'].'/summary.json'), true, flags: \JSON_THROW_ON_ERROR);
        foreach (['doctrine', 'broker'] as $backend) {
            $result = $summary['results'][$backend];
            $this->assertSame('complete', $result['execution_status'], $result['failure']);
            $this->assertSame('complete', $result['accounting_status']);
            $this->assertSame('pass', $result['integrity_status']);
            $this->assertSame('pass', $result['warmup_integrity_status']);
            $this->assertSame(12, $result['unique_completions']);
            $this->assertSame(0, $result['audit_remaining']);
            $this->assertSame($mode, $result['owning_connection_durability']['effective']);
            $this->assertTrue($result['cleanup']['complete']);
            $this->assertSame([], $result['terminal_telemetry']['issues']);
            $this->assertLessThanOrEqual($result['last_ack_return_ns'], $result['first_receive_return_ns']);
            $this->assertGreaterThanOrEqual($result['measurement_started_ns'], $result['first_receive_return_ns']);
            $this->assertEquals(($result['last_ack_return_ns'] - $result['measurement_started_ns']) / 1e9, $result['cohort_seconds']);
            $registry = $result['resources']['coverage_details']['registry'];
            foreach (['publisher-0', 'publisher-1', 'publisher-2', 'consumer-0', 'consumer-1'] as $actor) {
                $events = Recorder::read($output['capture'].'/'.$backend.'/'.$actor.'.operations.jsonl');
                $phases = [];
                foreach ($events as $event) {
                    $phases[$event['phase']] = true;
                    $this->assertSame($registry[$actor]['pid'], $event['pid']);
                }
                $this->assertArrayHasKey('warmup', $phases);
                if (str_starts_with($actor, 'publisher-')) {
                    $this->assertArrayHasKey('measure', $phases);
                }
                $this->assertTrue($result['telemetry_footers'][$actor]['finalized']);
            }
            if ('broker' === $backend) {
                $this->assertArrayHasKey('persistence', $result['cleanup']['roles']);
                $this->assertNotSame($registry['broker']['pid'], $registry['persistence']['pid']);
            } else {
                $this->assertCount(5, $result['owning_connection_durability']['connections']);
            }
        }
    }
}
