<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Amp\ByteStream\BufferedReader;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use Ineersa\SqliteQueue\Tests\Support\ProcessTree;
use PHPUnit\Framework\Attributes\RequiresOperatingSystem;
use PHPUnit\Framework\TestCase;

#[RequiresOperatingSystem('Linux')]
final class BrokerProcessTest extends TestCase
{
    private ?IsolatedDatabase $fixture = null;
    private string $socket = '';
    /** @var list<Process> */
    private array $processes = [];
    /** @var list<Client> */
    private array $clients = [];
    /** @var list<int> */
    private array $tracked = [];

    protected function setUp(): void
    {
        if (!ProcessTree::available()) {
            $this->markTestSkipped('The /proc filesystem is unavailable.');
        }
        $this->fixture = new IsolatedDatabase();
        $this->socket = $this->fixture->path('queue.sock');
    }

    protected function tearDown(): void
    {
        foreach ($this->clients as $client) {
            $client->close();
        }
        foreach (array_reverse($this->tracked) as $pid) {
            if (isset(ProcessTree::snapshot()[$pid]) && \function_exists('posix_kill')) {
                @posix_kill($pid, \SIGKILL);
            }
        }
        foreach ($this->processes as $process) {
            if ($process->isRunning()) {
                $process->kill();
            }
        }
        if (is_file($this->socket) || is_link($this->socket)) {
            @unlink($this->socket);
        }
        $this->fixture?->remove();
    }

    public function testGracefulStopReleasesOwnedTreeAndDatabaseSurvives(): void
    {
        $database = $this->fixture->path();
        $first = $this->startBroker($database, $this->socket);
        $pid = $first->getPid();
        $owned = $this->trackOwned($pid);
        $this->assertCount(1, $owned['workers'], 'The broker must own exactly one persistence worker.');

        $client = $this->client();
        $body = random_bytes(64);
        $headers = random_bytes(8);
        $sent = $client->send('jobs', $body, $headers);
        $this->assertGreaterThan(0, $sent);
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame($sent, $delivery->id);
        $this->assertSame($body, $delivery->body);
        $this->assertSame($headers, $delivery->headers);
        $client->acknowledge($delivery->receipt);

        $rejected = $client->send('jobs', 'discard me');
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame($rejected, $delivery->id);
        $client->reject($delivery->receipt);
        $this->assertNull($client->receive('jobs'));

        $persisted = $client->send('jobs', 'survives restart');
        $client->close();
        array_pop($this->clients);

        $this->stopBroker($first);
        $this->assertTrackedGone();
        $this->assertFileDoesNotExist($this->socket, 'Completed shutdown must release the endpoint.');
        $this->assertFileExists($database, 'Shutdown must preserve the database.');

        $second = $this->startBroker($database, $this->socket);
        $this->trackOwned($second->getPid());
        $client = $this->client();
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery, 'A restarted broker must serve persisted messages.');
        $this->assertSame($persisted, $delivery->id);
        $this->assertSame('survives restart', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $client->close();
        array_pop($this->clients);
        $this->stopBroker($second);
        $this->assertTrackedGone();
        $this->assertFileExists($database);
    }

    public function testExclusiveOwnershipRejectsConflictingBrokers(): void
    {
        $database = $this->fixture->path();
        $broker = $this->startBroker($database, $this->socket);
        $this->trackOwned($broker->getPid());
        $client = $this->client();
        $this->assertGreaterThan(0, $client->send('jobs', 'before conflict'));

        $sameDatabase = $this->conflictingBroker($database, $this->fixture->path('other.sock'));
        $this->assertNotSame(0, $sameDatabase->join(new TimeoutCancellation(10)), 'A second broker must not write the same database.');
        $otherDatabase = $this->conflictingBroker($this->fixture->path('other.sqlite'), $this->socket);
        $this->assertNotSame(0, $otherDatabase->join(new TimeoutCancellation(10)), 'A second broker must not take the same endpoint.');

        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame('before conflict', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $this->assertGreaterThan(0, $client->send('jobs', 'after conflict'));
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery);
        $this->assertSame('after conflict', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $client->close();
        array_pop($this->clients);

        $this->stopBroker($broker);
        $this->assertTrackedGone();
        $this->assertFileDoesNotExist($this->socket);
    }

    public function testIdlePersistenceDeathFailsBrokerAndPreservesConfirmedData(): void
    {
        $database = $this->fixture->path();
        $broker = $this->startBroker($database, $this->socket);
        $owned = $this->trackOwned($broker->getPid());
        $this->assertCount(1, $owned['workers'], 'The broker must own exactly one persistence worker.');
        $worker = $owned['workers'][0];

        $client = $this->client();
        $confirmed = $client->send('jobs', 'confirmed message');
        $this->assertGreaterThan(0, $confirmed);
        $client->close();
        array_pop($this->clients);

        $this->assertNotSame(0, posix_geteuid());
        $this->assertSame(posix_geteuid(), fileowner('/proc/'.$worker), 'The killed process must be this user\'s persistence worker.');
        $this->assertTrue(posix_kill($worker, \SIGKILL), 'The test must kill only the persistence worker.');

        $this->assertNotSame(0, $broker->join(new TimeoutCancellation(10)), 'An idle persistence death must fail the broker without another client request.');
        $this->assertTrackedGone();
        $this->assertFileDoesNotExist($this->socket, 'A failed broker must release the endpoint.');
        $this->assertFileExists($database, 'A failed broker must preserve confirmed data.');

        $restarted = $this->startBroker($database, $this->socket);
        $this->trackOwned($restarted->getPid());
        $client = $this->client();
        $delivery = $client->receive('jobs');
        $this->assertNotNull($delivery, 'A confirmation must survive a persistence failure.');
        $this->assertSame($confirmed, $delivery->id);
        $this->assertSame('confirmed message', $delivery->body);
        $client->acknowledge($delivery->receipt);
        $client->close();
        array_pop($this->clients);
        $this->stopBroker($restarted);
        $this->assertTrackedGone();
        $this->assertFileExists($database);
    }

    public function testInvalidStorageFailsStartupAndReleasesOwnership(): void
    {
        $database = $this->fixture->path();
        $invalid = str_repeat('not a sqlite database', 8);
        file_put_contents($database, $invalid);
        chmod($database, 0o600);

        $before = ProcessTree::ownedBy(getmypid());
        $failed = $this->conflictingBroker($database, $this->socket);
        $this->tracked[] = $failed->getPid();

        $stdout = '';
        while (null !== ($chunk = $failed->getStdout()->read(new TimeoutCancellation(10)))) {
            $stdout .= $chunk;
        }
        $this->assertNotSame(0, $failed->join(new TimeoutCancellation(10)), 'Unreadable storage must fail startup.');
        $this->assertStringNotContainsString('ready', $stdout, 'Readiness must not precede storage initialization.');
        $this->assertSame($invalid, file_get_contents($database), 'Failed startup must not modify the rejected database file.');
        $after = ProcessTree::ownedBy(getmypid());
        $this->assertSame([], array_values(array_diff($after['workers'], $before['workers'])), 'Failed startup left an owned persistence worker.');
        $this->assertSame([], array_values(array_diff($after['launchers'], $before['launchers'])), 'Failed startup left an owned worker launcher.');
        $this->assertTrackedGone();

        unlink($database);
        file_put_contents($database, '');
        chmod($database, 0o600);
        $restarted = $this->startBroker($database, $this->socket);
        $this->trackOwned($restarted->getPid());
        $this->assertFileExists($this->socket);
        $this->stopBroker($restarted);
        $this->assertTrackedGone();
        $this->assertFileExists($database);
    }

    private function startBroker(string $database, string $socket): Process
    {
        $process = $this->spawn($database, $socket);
        $line = (new BufferedReader($process->getStdout()))->readUntil("\n", new TimeoutCancellation(10), 65536);
        $ready = json_decode((string) $line, true, 16, \JSON_THROW_ON_ERROR);
        $this->assertIsArray($ready);
        $this->assertSame('ready', $ready['event'] ?? null, 'Readiness must be reported as a positive event.');
        $this->assertSame($process->getPid(), $ready['pid'] ?? null);
        $this->assertSame($socket, $ready['endpoint'] ?? null);
        $this->processes[] = $process;

        return $process;
    }

    private function conflictingBroker(string $database, string $socket): Process
    {
        $process = $this->spawn($database, $socket);
        $this->processes[] = $process;

        return $process;
    }

    private function spawn(string $database, string $socket): Process
    {
        return Process::start([
            \PHP_BINARY,
            \dirname(__DIR__, 2).'/bin/sqlite-queue',
            'broker',
            '--database='.$database,
            '--endpoint='.$socket,
        ], null, ['PATH' => '/usr/bin:/bin', 'LANG' => 'C']);
    }

    private function client(): Client
    {
        $client = Client::connect($this->socket, 10);
        $this->clients[] = $client;

        return $client;
    }

    /** @return array{launchers: list<int>, workers: list<int>} */
    private function trackOwned(int $pid): array
    {
        $owned = ProcessTree::ownedBy($pid);
        $this->tracked = [...$this->tracked, ...$owned['launchers'], ...$owned['workers'], $pid];

        return $owned;
    }

    private function stopBroker(Process $broker): void
    {
        $broker->signal(\SIGTERM);
        $this->assertSame(0, $broker->join(new TimeoutCancellation(10)), 'A signal must stop the broker cleanly.');
    }

    private function assertTrackedGone(): void
    {
        $snapshot = ProcessTree::snapshot();
        foreach ($this->tracked as $pid) {
            $this->assertArrayNotHasKey($pid, $snapshot, 'A broker-owned process survived shutdown.');
        }
    }
}
