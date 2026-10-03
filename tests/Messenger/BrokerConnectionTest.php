<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Exception\TransportException;
use Ineersa\SqliteQueue\Messenger\BrokerConnection;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\TestCase;

use function Amp\async;
use function Amp\Socket\listen;

final class BrokerConnectionTest extends TestCase
{
    private const int SAFETY_SECONDS = 5;

    public function testFailedInitialAcquisitionIsNotRetried(): void
    {
        $attempts = 0;
        $owner = new BrokerConnection(static function (Cancellation $cancellation) use (&$attempts): Client {
            ++$attempts;
            throw new TransportException('initial connection failed');
        });
        $this->assertSame(0, $attempts);
        for ($operation = 0; $operation < 2; ++$operation) {
            try {
                $owner->client();
                $this->fail('A failed connection owner must stay unusable.');
            } catch (TransportException $error) {
                $this->assertSame(0 === $operation ? 'initial connection failed' : 'The broker connection owner is closed.', $error->getMessage());
            }
        }
        $this->assertSame(1, $attempts);
    }

    public function testCloseBeforeFirstOperationDoesNotConnect(): void
    {
        $owner = new BrokerConnection(static function (Cancellation $cancellation): Client {
            throw new \LogicException('Closing must not acquire resources.');
        });
        $owner->close();
        $owner->close();
        $this->expectException(TransportException::class);
        $owner->client();
    }

    public function testCloseDuringGatedAcquisitionClosesAnyLateClient(): void
    {
        async(function (): void {
            $fixture = new IsolatedDatabase();
            $endpoint = $fixture->path('acquisition.sock');
            $server = listen('unix://'.$endpoint);
            $entered = new DeferredFuture();
            $release = new DeferredFuture();
            $peer = async(static function () use ($server, $entered, $release): bool {
                $socket = $server->accept();
                if (null === $socket) {
                    throw new \LogicException('Missing acquisition probe peer.');
                }
                try {
                    Frame::read($socket, new TimeoutCancellation(self::SAFETY_SECONDS));
                    $entered->complete();
                    $release->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
                    $socket->write((new Frame([
                        'v' => Frame::VERSION,
                        'id' => 0,
                        'ok' => true,
                        'result' => ['max_payload' => Frame::MAX_PAYLOAD],
                    ]))->encode());

                    return null === Frame::read($socket, new TimeoutCancellation(self::SAFETY_SECONDS));
                } finally {
                    $socket->close();
                }
            });
            // Deliberately ignore cancellation to exercise a connector returning a late resource.
            $owner = new BrokerConnection(static fn (Cancellation $cancellation): Client => Client::connect($endpoint));
            $connecting = async(static fn (): Client => $owner->client());
            try {
                $entered->getFuture()->await(new TimeoutCancellation(self::SAFETY_SECONDS));
                $owner->close();
                $release->complete();
                try {
                    $connecting->await(new TimeoutCancellation(self::SAFETY_SECONDS));
                    $this->fail('Closing during acquisition must prevent resurrection.');
                } catch (CancelledException) {
                    $this->addToAssertionCount(1);
                }
                $this->assertTrue($peer->await(new TimeoutCancellation(self::SAFETY_SECONDS)), 'The late client must close its peer socket.');
                try {
                    $owner->client();
                    $this->fail('A closed owner must not reconnect.');
                } catch (TransportException $error) {
                    $this->assertSame('The broker connection owner is closed.', $error->getMessage());
                }
            } finally {
                $owner->close();
                $server->close();
                @unlink($endpoint);
                $fixture->remove();
            }
        })->await(new TimeoutCancellation(2 * self::SAFETY_SECONDS));
    }
}
