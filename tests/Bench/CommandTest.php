<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Command\ClockProbeCommand;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Bench\Command\WorkerCommand;
use Ineersa\SqliteQueue\Bench\Process;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Tester\ApplicationTester;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Process\Process as SymfonyProcess;

final class CommandTest extends TestCase
{
    public function testConsoleOwnsHelpAndOptions(): void
    {
        $application = new Application('SQLite queue benchmark');
        $application->addCommand(new RunCommand());
        $application->addCommand(new WorkerCommand());
        $application->addCommand(new ClockProbeCommand());
        $application->setAutoExit(false);
        $tester = new ApplicationTester($application);

        self::assertSame(0, $tester->run(['command' => 'run', '--help' => true]));
        self::assertStringContainsString('--smoke', $tester->getDisplay());
        self::assertStringContainsString('--workload', $tester->getDisplay());
        self::assertStringContainsString('roundtrip', $tester->getDisplay());
        self::assertTrue($application->find('worker')->isHidden());
        self::assertTrue($application->find('clock-probe')->isHidden());
    }

    public function testWorkerRejectsInvalidRoleBeforeOpeningAFile(): void
    {
        $tester = new CommandTester(new WorkerCommand());
        $this->expectException(\InvalidArgumentException::class);
        $tester->execute(['config' => '/does/not/exist', 'role' => 'unknown', 'index' => '0']);
    }

    public function testClockProbeIsATestableConsoleCommand(): void
    {
        $tester = new CommandTester(new ClockProbeCommand());
        $tester->setInputs(['probe', 'probe']);

        self::assertSame(0, $tester->execute([]));
        $timestamps = explode("\n", trim($tester->getDisplay()));
        self::assertCount(2, $timestamps);
        self::assertTrue(ctype_digit($timestamps[0]));
        self::assertGreaterThanOrEqual((int) $timestamps[0], (int) $timestamps[1]);
    }

    public function testSymfonyProcessDoesNotInheritApplicationEnvironment(): void
    {
        $name = 'SQLITE_QUEUE_TEST_INHERITED_DSN';
        $previous = getenv($name);
        putenv($name . '=unrelated-service');

        try {
            $process = new SymfonyProcess(
                [PHP_BINARY, '-r', 'echo getenv("' . $name . '") === false ? "isolated" : "leaked";'],
                env: Process::environment(),
                timeout: 5,
            );
            $process->mustRun();
            self::assertSame('isolated', $process->getOutput());
        } finally {
            putenv($previous === false ? $name : $name . '=' . $previous);
        }
    }
}
