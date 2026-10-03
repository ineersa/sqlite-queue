<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Command\ClockProbeCommand;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Bench\Manifest;
use Ineersa\SqliteQueue\Bench\Process;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Tester\ApplicationTester;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Process\Process as SymfonyProcess;

final class CommandTest extends TestCase
{
    public function testConsoleOwnsHelpAndOptions(): void
    {
        $application = new Application('SQLite queue benchmark');
        $application->addCommand(new RunCommand());
        $application->addCommand(new ClockProbeCommand());
        $application->setAutoExit(false);
        $tester = new ApplicationTester($application);

        $this->assertSame(0, $tester->run(['command' => 'run', '--help' => true]));
        $this->assertStringContainsString('--smoke', $tester->getDisplay());
        $this->assertStringContainsString('--workload', $tester->getDisplay());
        $this->assertStringContainsString('roundtrip', $tester->getDisplay());
        $this->assertTrue($application->find('clock-probe')->isHidden());
    }

    public function testUnsupportedScenarioDoesNotStartAnyProcesses(): void
    {
        $tester = new CommandTester(new RunCommand());
        $this->expectException(\InvalidArgumentException::class);
        $tester->execute(['--smoke' => true, '--workload' => 'concurrent']);
    }

    public function testFormalDefaultsAndDirtyUnrelatedFilesAreExplicit(): void
    {
        $command = new RunCommand();
        $options = RunCommand::options(new ArrayInput([], $command->getDefinition()));
        $this->assertSame(60.0, $options->durationSeconds);
        $this->assertSame(5, $options->repetitions);
        $this->assertCount(10, $options->schedule());
        $this->expectException(\RuntimeException::class);
        Manifest::assertSourceMode($options, '?? unrelated-user-file');
    }

    public function testClockProbeIsATestableConsoleCommand(): void
    {
        $tester = new CommandTester(new ClockProbeCommand());
        $tester->setInputs(['probe', 'probe']);

        $this->assertSame(0, $tester->execute([]));
        $timestamps = explode("\n", trim($tester->getDisplay()));
        $this->assertCount(2, $timestamps);
        $this->assertTrue(ctype_digit($timestamps[0]));
        $this->assertGreaterThanOrEqual((int) $timestamps[0], (int) $timestamps[1]);
    }

    public function testSymfonyProcessDoesNotInheritApplicationEnvironment(): void
    {
        $name = 'SQLITE_QUEUE_TEST_INHERITED_DSN';
        $previous = getenv($name);
        putenv($name.'=unrelated-service');

        try {
            $process = new SymfonyProcess(
                [\PHP_BINARY, '-r', 'echo getenv("'.$name.'") === false ? "isolated" : "leaked";'],
                env: Process::environment(),
                timeout: 5,
            );
            $process->mustRun();
            $this->assertSame('isolated', $process->getOutput());
        } finally {
            putenv(false === $previous ? $name : $name.'='.$previous);
        }
    }
}
