<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Exception\TransportException as ClientTransportException;
use Ineersa\SqliteQueue\Messenger\Transport;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;

use function Amp\async;

final class TransportFactoryTest extends TestCase
{
    private ?IsolatedDatabase $database = null;
    private ?Broker $broker = null;
    /** @var Future<int>|null */
    private ?Future $brokerFuture = null;
    private string $endpoint = '';

    protected function setUp(): void
    {
        parent::setUp();
        $this->database = new IsolatedDatabase();
    }

    protected function tearDown(): void
    {
        try {
            $this->broker?->stop();
            if (null !== $this->brokerFuture) {
                $this->assertSame(0, $this->brokerFuture->await(new TimeoutCancellation(10)));
            }
        } finally {
            $this->broker = null;
            $this->brokerFuture = null;
            $this->database?->remove();
        }

        parent::tearDown();
    }

    public function testSupportsOnlySqliteQueueScheme(): void
    {
        $factory = new TransportFactory();
        $this->assertTrue($factory->supports('sqlite-queue://jobs?endpoint=/tmp/broker.sock', []));
        $this->assertFalse($factory->supports('doctrine://default', []));
        $this->assertFalse($factory->supports('sqlite://jobs', []));
    }

    public function testParseUrlPreservesHostCase(): void
    {
        $parts = parse_url('sqlite-queue://Jobs?endpoint=/var/missing-sqlite-queue.sock');
        $this->assertIsArray($parts);
        $this->assertSame('Jobs', $parts['host']);
    }

    #[DataProvider('invalidConfigurations')]
    public function testInvalidConfigurationFailsBeforeIo(string $dsn, array $options, string $needle): void
    {
        $factory = new TransportFactory();
        try {
            $factory->createTransport($dsn, $options, new PhpSerializer());
            $this->fail('Invalid configuration must fail before connecting.');
        } catch (\InvalidArgumentException $error) {
            $this->assertStringContainsString($needle, $error->getMessage());
        }
    }

    /**
     * @return iterable<string, array{0: string, 1: array<string, mixed>, 2: string}>
     */
    public static function invalidConfigurations(): iterable
    {
        yield 'missing endpoint' => ['sqlite-queue://jobs', [], 'endpoint'];
        yield 'empty endpoint' => ['sqlite-queue://jobs?endpoint=', [], 'non-empty endpoint'];
        yield 'relative endpoint' => ['sqlite-queue://jobs?endpoint=broker.sock', [], 'absolute'];
        yield 'unknown option' => ['sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue.sock&foo=1', [], 'Unknown'];
        yield 'unknown explicit option' => ['sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue.sock', ['batch' => 1], 'Unknown'];
        yield 'userinfo' => ['sqlite-queue://user@jobs?endpoint=/var/missing-sqlite-queue.sock', [], 'credentials'];
        yield 'password' => ['sqlite-queue://:secret@jobs?endpoint=/var/missing-sqlite-queue.sock', [], 'credentials'];
        yield 'port' => ['sqlite-queue://jobs:1234?endpoint=/var/missing-sqlite-queue.sock', [], 'port'];
        yield 'fragment' => ['sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue.sock#x', [], 'fragment'];
        yield 'path' => ['sqlite-queue://jobs/extra?endpoint=/var/missing-sqlite-queue.sock', [], 'path'];
        yield 'invalid queue' => ['sqlite-queue://-bad?endpoint=/var/missing-sqlite-queue.sock', [], 'Queue names'];
        yield 'nonpositive timeout' => ['sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue.sock&timeout=0', [], 'positive'];
        yield 'non numeric timeout' => ['sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue.sock&timeout=INF', [], 'number'];
        yield 'non finite timeout' => ['sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue.sock', ['timeout' => \INF], 'finite'];
        yield 'options override still validates' => [
            'sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue.sock',
            ['endpoint' => 'relative.sock'],
            'absolute',
        ];
    }

    public function testConnectFailureIsSymfonyTransportException(): void
    {
        $factory = new TransportFactory();
        $transport = $factory->createTransport(
            'sqlite-queue://jobs?endpoint=/var/missing-sqlite-queue-'.bin2hex(random_bytes(4)).'.sock',
            [],
            new PhpSerializer(),
        );
        try {
            $transport->get();
            $this->fail('Missing endpoint must fail connect.');
        } catch (TransportException $error) {
            $this->assertInstanceOf(ClientTransportException::class, $error->getPrevious());
        }
    }

    public function testExplicitOptionsOverrideQueryAndConnectOnFirstOperation(): void
    {
        $this->runAsync(function (): void {
            $this->startBroker();
            $wrong = $this->database?->path('missing.sock') ?? '';
            $factory = new TransportFactory();
            $transport = $factory->createTransport(
                'sqlite-queue://Jobs?endpoint='.rawurlencode($wrong).'&timeout=3',
                [
                    'endpoint' => $this->endpoint,
                    'timeout' => 5,
                    'transport_name' => 'async',
                ],
                new PhpSerializer(),
            );
            $this->assertInstanceOf(Transport::class, $transport);

            $sent = $transport->send(new Envelope(new TransportProbeMessage('factory')));
            $this->assertInstanceOf(TransportMessageIdStamp::class, $sent->last(TransportMessageIdStamp::class));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $this->assertSame('Jobs', (new \ReflectionProperty(Transport::class, 'queue'))->getValue($transport)->value);
            $transport->ack($received[0]);
            $transport->close();
        });
    }

    private function startBroker(): void
    {
        $database = $this->database ?? throw new \LogicException('Missing test database.');
        $this->endpoint = $database->path('queue.sock');
        $this->broker = (new BrokerFactory($database->path(), $this->endpoint))->create();
        $ready = new DeferredFuture();
        $this->brokerFuture = async(fn (): int => $this->broker->run(static function (array $event) use ($ready): void {
            $ready->complete($event);
        }));
        $event = $ready->getFuture()->await(new TimeoutCancellation(15));
        $this->assertSame('ready', $event['event']);
    }

    private function runAsync(\Closure $operation): void
    {
        async($operation)->await();
    }
}
