<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Sqlite;

use Amp\DeferredFuture;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\RecordingContextFactory;
use Ineersa\SqliteQueue\Tests\Support\ShortUnixEndpoint;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

use function Amp\async;

final class SqliteQueueStorageTest extends TestCase
{
    public function testOpenReportsConfiguredSynchronousModeAndClosesCleanly(): void
    {
        $database = new IsolatedDatabase();
        try {
            $storage = SqliteQueueStorage::open($database->path(), SqliteSynchronousMode::Normal);
            try {
                $this->assertSame(SqliteSynchronousMode::Normal, $storage->synchronousMode());
                $configuration = $storage->configuration();
                $this->assertSame('wal', strtolower((string) $configuration['journal_mode']));
                $this->assertSame(SqliteSynchronousMode::Normal->value, $configuration['synchronous']);
            } finally {
                $storage->close();
            }
        } finally {
            $database->remove();
        }
    }

    public function testOpenUsesInjectedConnectorAndSpawnsWorkerProcess(): void
    {
        $database = new IsolatedDatabase();
        $factory = new RecordingContextFactory(new ProcessContextFactory());
        try {
            $storage = SqliteQueueStorage::open(
                $database->path(),
                connector: new SqliteConnector($factory),
            );
            try {
                $this->assertCount(1, $factory->contexts);
                $process = $this->process($factory->contexts[0]);
                $this->assertTrue($process->isRunning());
                $this->assertSame(1, $storage->insert('jobs', 'payload', '', 0));
                $this->assertTrue($process->isRunning());
            } finally {
                $storage->close();
            }
            $this->assertFalse($process->isRunning());
        } finally {
            $database->remove();
        }
    }

    public function testBrokerFactoryUsesInjectedConnectorForWorkerBackedStorage(): void
    {
        $database = new IsolatedDatabase();
        $endpointResource = new ShortUnixEndpoint('factory.sock');
        $factory = new RecordingContextFactory(new ProcessContextFactory());
        $endpoint = $endpointResource->path();
        $broker = (new BrokerFactory(
            $database->path(),
            $endpoint,
            connector: new SqliteConnector($factory),
        ))->create();
        $run = async(static fn (): int => $broker->run());
        try {
            $this->assertCount(1, $factory->contexts);
            $process = $this->process($factory->contexts[0]);
            $this->assertTrue($process->isRunning());

            $client = Client::connect($endpoint);
            try {
                $client->send('jobs', 'factory-payload');
                $delivery = $client->receive('jobs');
                $this->assertNotNull($delivery);
                $this->assertSame('factory-payload', $delivery->body);
                $client->acknowledge($delivery->receipt);
            } finally {
                $client->close();
            }
            $this->assertTrue($process->isRunning());
        } finally {
            $broker->stop();
            $this->assertSame(0, $run->await(new TimeoutCancellation(15)));
            $endpointResource->remove();
            $database->remove();
        }
        $this->assertFalse($process->isRunning());
        $this->assertFileDoesNotExist($endpoint);
    }

    public function testBrokerCommandUsesInjectedConnectorForWorkerBackedStorage(): void
    {
        $database = new IsolatedDatabase();
        $endpointResource = new ShortUnixEndpoint('command.sock');
        $factory = new RecordingContextFactory(new ProcessContextFactory());
        $endpoint = $endpointResource->path();
        $command = new BrokerCommand(connector: new SqliteConnector($factory));
        $ready = new DeferredFuture();
        $output = new class($ready) extends BufferedOutput {
            public function __construct(private readonly DeferredFuture $ready)
            {
                parent::__construct();
            }

            protected function doWrite(string $message, bool $newline): void
            {
                parent::doWrite($message, $newline);
                if (str_contains($message, '"event":"ready"')) {
                    $this->ready->complete();
                }
            }
        };
        $run = async(static fn (): int => $command->run(new ArrayInput([
            '--database' => $database->path(),
            '--endpoint' => $endpoint,
        ], $command->getDefinition()), $output));
        try {
            $ready->getFuture()->await(new TimeoutCancellation(15));
            $this->assertCount(1, $factory->contexts);
            $process = $this->process($factory->contexts[0]);
            $this->assertTrue($process->isRunning());

            $client = Client::connect($endpoint);
            try {
                $client->send('jobs', 'command-payload');
                $delivery = $client->receive('jobs');
                $this->assertNotNull($delivery);
                $this->assertSame('command-payload', $delivery->body);
                $client->acknowledge($delivery->receipt);
            } finally {
                $client->close();
            }
            $this->assertTrue($process->isRunning());
        } finally {
            if (!$run->isComplete()) {
                posix_kill(getmypid(), \SIGTERM);
            }
            $this->assertSame(0, $run->await(new TimeoutCancellation(15)));
            $endpointResource->remove();
            $database->remove();
        }
        $this->assertFalse($process->isRunning());
        $this->assertFileDoesNotExist($endpoint);
        $this->assertStringContainsString('"event":"ready"', $output->fetch());
    }

    private function process(ProcessContext $context): Process
    {
        $process = (new \ReflectionProperty(ProcessContext::class, 'process'))->getValue($context);
        $this->assertInstanceOf(Process::class, $process);

        return $process;
    }
}
