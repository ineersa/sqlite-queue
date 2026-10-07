<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Baseline;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;

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
}
