<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\Socket\ServerSocket;
use Ineersa\SqliteQueue\Broker\Ownership;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\TestCase;

use function Amp\Socket\listen;

final class OwnershipTest extends TestCase
{
    private ?IsolatedDatabase $fixture = null;
    /** @var list<resource> */
    private array $listeners = [];
    /** @var list<Ownership> */
    private array $ownerships = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->fixture = new IsolatedDatabase();
        chmod($this->fixture->directory(), 0o700);
    }

    protected function tearDown(): void
    {
        try {
            foreach ($this->ownerships as $ownership) {
                $ownership->close();
            }
            foreach ($this->listeners as $listener) {
                fclose($listener);
            }
        } finally {
            $this->listeners = [];
            $this->ownerships = [];
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

    public function testSecondOwnershipCannotTakeDatabaseOrEndpointLocks(): void
    {
        [$database, $endpoint] = $this->paths();
        $first = $this->own($database, $endpoint);
        $this->assertSame(0o600, $this->permissions($database));
        $this->assertFileExists($endpoint.'.lock');
        $this->assertSame(0o600, $this->permissions($endpoint.'.lock'));

        foreach ([1, 2] as $attempt) {
            try {
                new Ownership($database, $endpoint);
                $this->fail('A second ownership must not take the locks.');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('Ownership unavailable', $error->getMessage());
            }
        }

        $this->assertFileExists($database);
        $this->assertSame(0o600, $this->permissions($database));
        $this->assertSame($database, $first->database);
        $this->assertSame($endpoint, $first->endpoint);

        $first->close();
        $this->own($database, $endpoint);
    }

    public function testBoundSocketIsMadePrivateAndRemovedOnClose(): void
    {
        [$database, $endpoint] = $this->paths();
        file_put_contents($database, 'engine state');
        chmod($database, 0o600);
        $ownership = $this->own($database, $endpoint);
        $server = $this->server($endpoint);
        $ownership->recordSocket();
        $this->assertSame('socket', $this->type($endpoint));
        $this->assertSame(0o600, $this->permissions($endpoint));

        $server->close();
        $ownership->close();
        $this->assertFileDoesNotExist($endpoint);
        $this->assertFileExists($database);
        $this->assertSame('engine state', file_get_contents($database));
        $this->assertFileExists($endpoint.'.lock');
        $this->assertSame(0o600, $this->permissions($endpoint.'.lock'));
        $this->own($database, $endpoint);
    }

    public function testPreExistingLiveSocketIsRefusedAndLeftIntact(): void
    {
        [$database, $endpoint] = $this->paths();
        $listener = stream_socket_server('unix://'.$endpoint, $code, $message);
        $this->assertIsResource($listener, (string) $message);
        $this->listeners[] = $listener;

        try {
            new Ownership($database, $endpoint);
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
            new Ownership($database, $endpoint);
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
            new Ownership($database, $endpoint);
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
            new Ownership($public.'/queue.sqlite', $public.'/queue.sock');
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
                new Ownership($database, $endpoint);
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
        $ownership = $this->own($database, $endpoint);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Cannot verify bound socket ownership.');
        $ownership->recordSocket();
    }

    private function own(string $database, string $endpoint): Ownership
    {
        $ownership = new Ownership($database, $endpoint);
        $this->ownerships[] = $ownership;

        return $ownership;
    }

    /** @return array{string, string} */
    private function paths(): array
    {
        $name = bin2hex(random_bytes(4));

        return [$this->fixture->path('queue-'.$name.'.sqlite'), $this->fixture->path('queue-'.$name.'.sock')];
    }

    private function server(string $endpoint): ServerSocket
    {
        return listen('unix://'.$endpoint);
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
}
