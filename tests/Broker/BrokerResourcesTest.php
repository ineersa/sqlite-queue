<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Broker\BrokerLifetimeLocks;
use Ineersa\SqliteQueue\Broker\SocketIdentity;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\TestCase;

final class BrokerResourcesTest extends TestCase
{
    private ?IsolatedDatabase $fixture = null;
    /** @var list<resource> */
    private array $listeners = [];
    /** @var list<BrokerLifetimeLocks> */
    private array $locks = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->fixture = new IsolatedDatabase();
        chmod($this->fixture->directory(), 0o700);
    }

    protected function tearDown(): void
    {
        try {
            foreach ($this->locks as $locks) {
                $locks->close();
            }
            foreach ($this->listeners as $listener) {
                fclose($listener);
            }
        } finally {
            $this->listeners = [];
            $this->locks = [];
            $directory = $this->fixture?->directory();
            foreach (false === $directory ? [] : $this->entries($directory) as $entry) {
                if ('.' !== $entry && '..' !== $entry && !is_dir($directory.'/'.$entry)) {
                    @unlink($directory.'/'.$entry);
                }
            }
            $this->fixture?->remove();
            parent::tearDown();
        }
    }

    public function testSecondBrokerLifetimeLocksCannotTakeDatabaseOrEndpointLocks(): void
    {
        [$database, $endpoint] = $this->paths();
        $first = $this->own($database, $endpoint);
        $this->assertFileDoesNotExist($database);
        $this->assertPrivateLockDirectory();

        foreach ([1, 2] as $attempt) {
            try {
                new BrokerLifetimeLocks($database, $endpoint);
                $this->fail('A second locks must not take the locks.');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('Broker lifetime locks unavailable', $error->getMessage());
            }
        }

        $this->assertFileDoesNotExist($database);
        $this->assertSame($database, $first->database);
        $this->assertSame($endpoint, $first->endpoint);

        $first->close();
        $this->own($database, $endpoint);
    }

    public function testSameDatabaseWithDifferentEndpointIsRefused(): void
    {
        [$database, $endpoint] = $this->paths();
        $other = $this->fixture->path('queue-other.sock');
        $first = $this->own($database, $endpoint);

        try {
            new BrokerLifetimeLocks($database, $other);
            $this->fail('A second broker must not take the same database under another endpoint.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Broker lifetime locks unavailable', $error->getMessage());
        }

        $this->assertFileDoesNotExist($other);
        $this->assertPrivateLockDirectory();

        $first->close();
        $this->own($database, $other);
    }

    public function testSameEndpointWithDifferentDatabaseIsRefused(): void
    {
        [$database, $endpoint] = $this->paths();
        $other = $this->fixture->path('queue-other.sqlite');
        $first = $this->own($database, $endpoint);

        try {
            new BrokerLifetimeLocks($other, $endpoint);
            $this->fail('A second broker must not take the same endpoint with another database.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Broker lifetime locks unavailable', $error->getMessage());
        }

        $this->assertFileDoesNotExist($other);
        $this->assertPrivateLockDirectory();

        $first->close();
        $this->own($other, $endpoint);
    }

    public function testBoundSocketIsMadePrivateAndRemovedOnClose(): void
    {
        [$database, $endpoint] = $this->paths();
        $broker = (new BrokerFactory($database, $endpoint))->create();
        $this->assertSame('socket', $this->type($endpoint));
        $this->assertSame(0o600, $this->permissions($endpoint));
        $this->assertSame(0o600, $this->permissions($database));
        $this->assertSame(0, $broker->run(static function () use ($broker): void {
            $broker->stop();
        }));
        $this->assertFileDoesNotExist($endpoint);
        $this->assertFileExists($database);
        $this->assertPrivateLockDirectory();
        $this->own($database, $endpoint);
    }

    public function testPreExistingLiveSocketIsRefusedAndLeftIntact(): void
    {
        [$database, $endpoint] = $this->paths();
        $listener = stream_socket_server('unix://'.$endpoint, $code, $message);
        $this->assertIsResource($listener, (string) $message);
        $this->listeners[] = $listener;

        try {
            (new BrokerFactory($database, $endpoint))->create();
            $this->fail('An existing endpoint must not be silently removed.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Endpoint already exists', $error->getMessage());
        }

        $this->assertSame('socket', $this->type($endpoint));
        $peer = stream_socket_client('unix://'.$endpoint, $code, $message);
        $this->assertIsResource($peer, (string) $message);
        fclose($peer);
        $this->assertFileExists($database);
    }

    public function testRegularFileEndpointIsRefusedAndLeftIntact(): void
    {
        [$database, $endpoint] = $this->paths();
        file_put_contents($endpoint, 'not a socket');

        try {
            (new BrokerFactory($database, $endpoint))->create();
            $this->fail('A regular file endpoint must not be unlinked.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Endpoint already exists', $error->getMessage());
        }

        $this->assertFileExists($endpoint);
        $this->assertSame('not a socket', file_get_contents($endpoint));
    }

    public function testSymlinkEndpointIsRefusedAndLeftIntact(): void
    {
        [$database, $endpoint] = $this->paths();
        $target = $this->fixture->path('elsewhere.sqlite');
        $this->assertTrue(symlink($target, $endpoint));

        try {
            (new BrokerFactory($database, $endpoint))->create();
            $this->fail('A symlink endpoint must not be unlinked.');
        } catch (\RuntimeException $error) {
            $this->assertStringContainsString('Endpoint already exists', $error->getMessage());
        }

        $this->assertTrue(is_link($endpoint));
        $this->assertSame($target, readlink($endpoint));
        unlink($endpoint);
    }

    public function testNonPrivateDirectoryIsRejectedWithoutCreatingLocks(): void
    {
        $public = $this->fixture->directory().'/public';
        $this->assertTrue(mkdir($public, 0o700));
        chmod($public, 0o755);

        try {
            new BrokerLifetimeLocks($public.'/queue.sqlite', $public.'/queue.sock');
            $this->fail('A group-accessible directory must be rejected.');
        } catch (\InvalidArgumentException $error) {
            $this->assertStringContainsString('private', $error->getMessage());
        }

        $this->assertSame(['.', '..'], $this->entries($public));
        chmod($public, 0o700);
        rmdir($public);
    }

    public function testRelativePathsAreRejected(): void
    {
        foreach ([['relative.sqlite', $this->fixture->path('queue.sock')], [$this->fixture->path('queue.sqlite'), 'relative.sock']] as [$database, $endpoint]) {
            try {
                new BrokerLifetimeLocks($database, $endpoint);
                $this->fail('Relative paths must be rejected.');
            } catch (\InvalidArgumentException $error) {
                $this->assertStringContainsString('absolute', $error->getMessage());
            }
        }

        $this->assertSame(['.', '..'], $this->entries($this->fixture->directory()));
    }

    public function testMissingSocketFailsWithoutLeakingFilesystemWarnings(): void
    {
        [$database, $endpoint] = $this->paths();
        $this->own($database, $endpoint);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot verify bound socket ownership.');
        SocketIdentity::fromEndpoint($endpoint);
    }

    private function own(string $database, string $endpoint): BrokerLifetimeLocks
    {
        $locks = new BrokerLifetimeLocks($database, $endpoint);
        $this->locks[] = $locks;

        return $locks;
    }

    /** @return array{string, string} */
    private function paths(): array
    {
        $name = bin2hex(random_bytes(4));

        return [$this->fixture->path('queue-'.$name.'.sqlite'), $this->fixture->path('queue-'.$name.'.sock')];
    }

    /** @return list<string> */
    private function entries(string $directory): array
    {
        $entries = scandir($directory);

        return array_values(false === $entries ? [] : $entries);
    }

    private function type(string $path): string
    {
        clearstatcache(true, $path);

        return filetype($path) ?: '';
    }

    private function permissions(string $path): int
    {
        clearstatcache(true, $path);

        return fileperms($path) & 0o777;
    }

    /**
     * Symfony owns lock sidecar names and modes, so this asserts the permission boundary
     * instead: the directory holding them stays private to the effective user.
     */
    private function assertPrivateLockDirectory(): void
    {
        $this->assertSame(0o700, $this->permissions($this->fixture->directory()));
    }
}
