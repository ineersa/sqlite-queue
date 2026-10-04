<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Bench\Baseline;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Process\Process;

final class ModeAndDefaultsTest extends TestCase
{
    public function testRequestedRetentionDefaultsAndExplicitSmokePolicy(): void
    {
        $command = new RunCommand();
        foreach ([false, true] as $smoke) {
            $options = RunCommand::options(new ArrayInput(['--workload' => 'retention', '--smoke' => $smoke], $command->getDefinition()));
            $this->assertSame($smoke ? 2 : 20, $options->effectiveCycles());
            $this->assertSame($smoke ? 2 : 100, $options->effectiveCycleMessages());
        }
    }

    public static function modes(): iterable
    {
        yield ['normal', 1];
        yield ['full', 2];
    }

    #[DataProvider('modes')]
    public function testOwningDoctrineReadbackMatchesSelectedMode(string $mode, int $expected): void
    {
        $database = new IsolatedDatabase();
        $selected = SqliteSynchronousMode::from($mode);
        $connection = Baseline::connect($database->path(), $selected);
        try {
            $effective = Baseline::durability($connection);
            $this->assertSame($expected, $effective['synchronous']);
            $this->assertSame('wal', $effective['journal_mode']);
            $this->assertSame('immediate', $effective['transaction_mode']);
            $this->assertTrue(Baseline::isDurabilityEquivalent($effective, $selected));
            $connection->executeStatement('PRAGMA synchronous='.(1 === $expected ? 'FULL' : 'NORMAL'));
            $this->assertFalse(Baseline::isDurabilityEquivalent(Baseline::durability($connection), $selected));
        } finally {
            $connection->close();
            $database->remove();
        }
    }

    public static function invalidModes(): iterable
    {
        foreach (['off', 'extra', 'automatic', 'invalid', 'NORMAL', ''] as $mode) {
            yield [$mode];
        }
    }

    #[DataProvider('invalidModes')]
    public function testInvalidModesAreRejectedBeforeExecution(string $mode): void
    {
        $command = new RunCommand();
        $this->expectException(\InvalidArgumentException::class);
        RunCommand::options(new ArrayInput(['--synchronous' => $mode], $command->getDefinition()));
    }

    #[DataProvider('modes')]
    public function testPairedRoundtripSmokeRecordsOwningMode(string $mode, int $expected): void
    {
        $root = \dirname(__DIR__, 2);
        $process = new Process([\PHP_BINARY, $root.'/bin/benchmark', 'run', '--smoke', '--synchronous='.$mode], $root, timeout: 60);
        $process->mustRun();
        $output = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
        $summary = json_decode(file_get_contents($output['capture'].'/summary.json'), true, flags: \JSON_THROW_ON_ERROR);
        $manifest = json_decode(file_get_contents($output['capture'].'/manifest.json'), true, flags: \JSON_THROW_ON_ERROR);
        $this->assertSame($mode, $summary['configuration']['synchronous_desired']);
        $this->assertSame($mode, $manifest['settings']['synchronous_desired']);
        foreach (['doctrine', 'broker'] as $backend) {
            $result = $summary['results'][$backend];
            $this->assertSame('complete', $result['execution_status']);
            $this->assertSame('pass', $result['integrity_status']);
            $this->assertSame($mode, $result['synchronous_desired']);
            $this->assertSame($mode, $result['owning_connection_durability']['effective']);
            $this->assertSame($mode, $manifest['owning_connection_durability'][$backend]['effective']);
            if ('doctrine' === $backend) {
                $connections = $result['owning_connection_durability']['connections'];
                $this->assertCount(2, $connections);
                foreach ($connections as $connection) {
                    $this->assertSame($expected, $connection['effective']['synchronous']);
                }
            }
        }
    }
}
