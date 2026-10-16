<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Exception\TransportException;
use Ineersa\SqliteQueue\Messenger\BrokerConnection;
use Ineersa\SqliteQueue\Messenger\Transport;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeProbeMessage;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\NativeKernel;
use Ineersa\SqliteQueue\Tests\Support\ClockFile;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ShortUnixEndpoint;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use PHPUnit\Framework\TestCase;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

use function Amp\async;

/**
 * Bundle-configured payload and connection budgets through the Messenger adapter.
 *
 * Broker-level multi-megabyte body/header and connection proofs live in BrokerTest.
 * This case proves the raised bundle values reach the live broker command and the
 * Messenger Transport round-trips a multi-megabyte opaque body under that budget.
 */
final class ConfigurableLimitsIntegrationTest extends TestCase
{
    private const int SAFETY_SECONDS = 15;

    public function testMessengerRoundTripsRaisedPayloadAndHonorsConfiguredConnectionBudget(): void
    {
        $fixture = new IsolatedDatabase();
        $endpointResource = new ShortUnixEndpoint();
        $project = $fixture->directory().'/app';
        $filesystem = new Filesystem();
        $filesystem->mirror(__DIR__.'/Fixtures/NativeApp/config', $project.'/config');
        // PhpSerializer expands opaque bodies; 6 MiB leaves room for a multi-megabyte
        // NativeProbeMessage while staying above the published 1_040_000 default.
        $filesystem->dumpFile($project.'/config/packages/sqlite_queue.yaml', "sqlite_queue:\n    max_payload: 6000000\n    max_connections: 80\n");
        ClockFile::configure($fixture->path(), 1_700_000_000_000);
        $kernel = new NativeKernel($project);
        $kernel->boot();
        $application = new Application($kernel);
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);
        $command = $application->find('sqlite-queue:broker');
        $this->assertSame('6000000', $command->getDefinition()->getOption('max-payload')->getDefault());
        $this->assertSame('80', $command->getDefinition()->getOption('max-connections')->getDefault());

        $ready = new DeferredFuture();
        $output = new class($ready) extends BufferedOutput {
            public function __construct(private readonly DeferredFuture $ready)
            {
                parent::__construct();
            }

            protected function doWrite(string $message, bool $newline): void
            {
                parent::doWrite($message, $newline);
                if (!$this->ready->isComplete() && str_contains($message, '"event":"ready"')) {
                    $this->ready->complete($message);
                }
            }
        };
        $run = async(static fn (): int => $application->run(new ArrayInput([
            'command' => 'sqlite-queue:broker',
            '--database' => $fixture->path(),
            '--endpoint' => $endpointResource->path(),
        ]), $output));
        try {
            $readyMessage = $ready->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
            $this->assertStringContainsString('"max_payload":6000000', $readyMessage);
            $this->assertStringContainsString('"max_connections":80', $readyMessage);

            $connect = static fn (): Client => Client::connect($endpointResource->path());
            $transport = new Transport(
                new BrokerConnection($connect),
                new QueueName('jobs'),
                new PhpSerializer(),
                new BrokerConnection($connect),
                $endpointResource->path(),
            );
            try {
                $serializer = new PhpSerializer();
                $body = str_repeat("\0\xff", 1_200_000);
                $envelope = new Envelope(new NativeProbeMessage($body));
                $encoded = $serializer->encode($envelope);
                $this->assertIsString($encoded['body']);
                $headers = \array_key_exists('headers', $encoded) && \is_array($encoded['headers'])
                    ? json_encode($encoded['headers'], \JSON_THROW_ON_ERROR)
                    : '{}';
                $this->assertGreaterThan(Limits::MAX_PAYLOAD, \strlen($encoded['body']) + \strlen($headers));
                $this->assertLessThanOrEqual(6_000_000, \strlen($encoded['body']) + \strlen($headers));
                $transport->send($envelope);
                $envelopes = iterator_to_array($transport->get());
                $this->assertCount(1, $envelopes);
                $received = $envelopes[0]->getMessage();
                $this->assertInstanceOf(NativeProbeMessage::class, $received);
                $this->assertSame($body, $received->body);
                $transport->ack($envelopes[0]);
            } finally {
                $transport->close();
            }

            $admitted = [];
            try {
                for ($i = 0; $i < 80; ++$i) {
                    $admitted[] = Client::connect($endpointResource->path());
                }
                $this->assertCount(80, $admitted);
                try {
                    Client::connect($endpointResource->path());
                    $this->fail('Configured connection capacity must reject the 81st client.');
                } catch (TransportException) {
                }
            } finally {
                foreach ($admitted as $client) {
                    $client->close();
                }
            }
        } finally {
            if (!$run->isComplete()) {
                // Same-process Application::run listens for SIGTERM through Revolt and cancels the broker.
                posix_kill(getmypid(), \SIGTERM);
            }
            $this->assertSame(0, $run->await(new TimeoutCancellation(self::SAFETY_SECONDS)));
            $kernel->shutdown();
            $filesystem->remove($project);
            $endpointResource->remove();
            $fixture->remove();
        }
    }
}
