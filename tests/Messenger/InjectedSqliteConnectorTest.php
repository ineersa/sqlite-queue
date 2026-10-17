<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\DeferredFuture;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\NativeKernel;
use Ineersa\SqliteQueue\Tests\Support\ClockFile;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\RecordingContextFactory;
use Ineersa\SqliteQueue\Tests\Support\ShortUnixEndpoint;
use PHPUnit\Framework\Attributes\RequiresOperatingSystem;
use PHPUnit\Framework\TestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Filesystem\Filesystem;

use function Amp\async;

#[RequiresOperatingSystem('Linux')]
final class InjectedSqliteConnectorTest extends TestCase
{
    public function testBundleDefaultConnectorKeepsDriverProcessIsolation(): void
    {
        $fixture = new IsolatedDatabase();
        $project = $fixture->directory().'/app';
        $filesystem = new Filesystem();
        $filesystem->mirror(__DIR__.'/Fixtures/NativeApp/config', $project.'/config');
        $kernel = new NativeKernel($project);
        try {
            $kernel->boot();
            $command = $kernel->getContainer()->get(BrokerCommand::class);
            $this->assertInstanceOf(BrokerCommand::class, $command);
            $connector = (new \ReflectionProperty(BrokerCommand::class, 'connector'))->getValue($command);
            $this->assertInstanceOf(SqliteConnector::class, $connector);
            $factory = (new \ReflectionProperty(SqliteConnector::class, 'contextFactory'))->getValue($connector);
            $this->assertInstanceOf(ProcessContextFactory::class, $factory);
        } finally {
            $kernel->shutdown();
            $filesystem->remove($project);
            $fixture->remove();
        }
    }

    public function testHostCanReplaceConnectorServiceAndReachWorkerBackedBroker(): void
    {
        $fixture = new IsolatedDatabase();
        $endpointResource = new ShortUnixEndpoint('bundle.sock');
        $project = $fixture->directory().'/app';
        $filesystem = new Filesystem();
        $filesystem->mirror(__DIR__.'/Fixtures/NativeApp/config', $project.'/config');
        ClockFile::configure($fixture->path(), 1_700_000_000_000);
        $filesystem->appendToFile($project.'/config/services.yaml', <<<'YAML'

    Amp\Parallel\Context\ProcessContextFactory: ~

    Ineersa\SqliteQueue\Tests\Support\RecordingContextFactory:
        public: true
        arguments:
            - '@Amp\Parallel\Context\ProcessContextFactory'

    Fabpot\Amp\Sqlite\SqliteConnector:
        public: true
        arguments:
            - '@Ineersa\SqliteQueue\Tests\Support\RecordingContextFactory'
YAML);
        $kernel = new NativeKernel($project);
        $kernel->boot();
        $recording = $kernel->getContainer()->get(RecordingContextFactory::class);
        $this->assertInstanceOf(RecordingContextFactory::class, $recording);
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);
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
        $endpoint = $endpointResource->path();
        $run = async(static fn (): int => $application->run(new ArrayInput([
            'command' => 'sqlite-queue:broker',
            '--database' => $fixture->path(),
            '--endpoint' => $endpoint,
        ]), $output));
        try {
            $ready->getFuture()->await(new TimeoutCancellation(15));
            $this->assertCount(1, $recording->contexts);
            $process = $this->process($recording->contexts[0]);
            $this->assertTrue($process->isRunning());

            $client = Client::connect($endpoint);
            try {
                $client->send('jobs', 'bundle-payload');
                $delivery = $client->receive('jobs');
                $this->assertNotNull($delivery);
                $this->assertSame('bundle-payload', $delivery->body);
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
            $kernel->shutdown();
            $endpointResource->remove();
            $filesystem->remove($project);
            $fixture->remove();
        }
        $this->assertFalse($process->isRunning());
        $this->assertFileDoesNotExist($endpoint);
    }

    private function process(ProcessContext $context): Process
    {
        $process = (new \ReflectionProperty(ProcessContext::class, 'process'))->getValue($context);
        $this->assertInstanceOf(Process::class, $process);

        return $process;
    }
}
