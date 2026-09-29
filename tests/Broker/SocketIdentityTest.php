<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Ineersa\SqliteQueue\Broker\SocketIdentity;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\TestCase;

final class SocketIdentityTest extends TestCase
{
    private ?IsolatedDatabase $fixture = null;
    /** @var list<resource> */
    private array $listeners = [];
    /** @var list<string> Socket paths whose files survive fclose() and need explicit unlinking. */
    private array $sockets = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->fixture = new IsolatedDatabase();
    }

    protected function tearDown(): void
    {
        try {
            foreach ($this->listeners as $listener) {
                fclose($listener);
            }
            foreach ($this->sockets as $socket) {
                @unlink($socket);
            }
        } finally {
            $this->listeners = [];
            $this->sockets = [];
            $this->fixture?->remove();
            $this->fixture = null;
            parent::tearDown();
        }
    }

    public function testBoundSocketMatchesUntilItIsReplaced(): void
    {
        $endpoint = $this->path('queue.sock');
        $listener = stream_socket_server('unix://'.$endpoint, $code, $message);
        $this->assertIsResource($listener, (string) $message);
        $this->listeners[] = $listener;
        $this->sockets[] = $endpoint;

        $identity = SocketIdentity::fromEndpoint($endpoint);
        $this->assertTrue($identity->matches($endpoint));

        // A swapped-in regular file must never verify, even if it inherited the inode.
        fclose($listener);
        $this->listeners = [];
        unlink($endpoint);
        file_put_contents($endpoint, 'impostor');

        $this->assertFalse($identity->matches($endpoint));
        $this->assertFileExists($endpoint);
    }

    public function testRegularFileCannotBecomeAnIdentity(): void
    {
        $endpoint = $this->path('queue.sock');
        file_put_contents($endpoint, 'not a socket');

        try {
            SocketIdentity::fromEndpoint($endpoint);
            $this->fail('A regular file must not verify as a bound socket.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Cannot verify bound socket ownership', $error->getMessage());
        }
    }

    public function testMissingPathNeitherVerifiesNorMatches(): void
    {
        $endpoint = $this->path('queue.sock');

        try {
            SocketIdentity::fromEndpoint($endpoint);
            $this->fail('A missing path must not verify as a bound socket.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Cannot verify bound socket ownership', $error->getMessage());
        }

        $other = $this->path('other.sock');
        $listener = stream_socket_server('unix://'.$other, $code, $message);
        $this->assertIsResource($listener, (string) $message);
        $this->listeners[] = $listener;
        $this->sockets[] = $other;

        $this->assertFalse(SocketIdentity::fromEndpoint($other)->matches($endpoint));
    }

    private function path(string $name): string
    {
        return ($this->fixture ?? throw new \LogicException('Missing test fixture.'))->path($name);
    }
}
