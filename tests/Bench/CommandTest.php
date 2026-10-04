<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Command\RunCommand;
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
        $application->setAutoExit(false);
        $tester = new ApplicationTester($application);

        $this->assertSame(0, $tester->run(['command' => 'run', '--help' => true]));
        $this->assertStringContainsString('--smoke', $tester->getDisplay());
        $this->assertStringContainsString('--workload', $tester->getDisplay());
        $this->assertStringContainsString('roundtrip', $tester->getDisplay());
        $this->assertSame(['workload', 'smoke', 'duration', 'synchronous'], array_keys((new RunCommand())->getDefinition()->getOptions()));
    }

    public function testRemovedWorkloadsAreRejectedBeforeAcquisition(): void
    {
        $command = new RunCommand();
        foreach (['fixed-rate', 'delayed'] as $workload) {
            try {
                RunCommand::options(new \Symfony\Component\Console\Input\ArrayInput(['--workload' => $workload], $command->getDefinition()));
                $this->fail('Removed workload accepted.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('Use roundtrip', $error->getMessage());
            }
        }
    }

    public function testDefaultSettingsAreFixedAndOnePairIsDeclared(): void
    {
        $command = new RunCommand();
        $options = RunCommand::options(new \Symfony\Component\Console\Input\ArrayInput([], $command->getDefinition()));
        $this->assertSame(60.0, $options->durationSeconds);
        $this->assertSame('normal', $options->synchronous->value);
        $this->assertCount(2, $options->schedule());
        $this->assertSame(['doctrine', 'broker'], array_column($options->schedule(), 'backend'));
    }

    public function testUnsupportedScenarioDoesNotStartAnyProcesses(): void
    {
        $tester = new CommandTester(new RunCommand());
        $this->expectException(\InvalidArgumentException::class);
        $tester->execute(['--smoke' => true, '--workload' => 'unknown-workload']);
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
