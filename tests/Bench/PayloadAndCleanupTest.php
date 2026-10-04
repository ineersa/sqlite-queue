<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Clock;
use Ineersa\SqliteQueue\Bench\Handler;
use Ineersa\SqliteQueue\Bench\Payload;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\ProbeMessage;
use Ineersa\SqliteQueue\Bench\Recorder;
use Ineersa\SqliteQueue\Bench\Resources;
use Ineersa\SqliteQueue\Bench\Role;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\MessageBusInterface;

final class PayloadAndCleanupTest extends TestCase
{
    #[DataProvider('identities')]
    public function testExactPayloadAndIndependentSize(string $id): void
    {
        $payload = Payload::generate($id);
        Payload::verify($id, $payload);
        $this->assertSame($payload, Payload::generate($id));
        foreach (['a' === $payload[0] ? 'b'.substr($payload, 1) : 'a'.substr($payload, 1), Payload::generate('measure:1')] as $bad) {
            $bytes = '';
            $recorder = new Recorder(static function (string $data) use (&$bytes): bool {
                $bytes .= $data;

                return true;
            }, 65536, 'run', 'consumer');
            try {
                (new Handler($recorder, $this->createStub(MessageBusInterface::class), Clock::system()))(new ProbeMessage($id, Phase::Measure, $bad, false));
                $this->fail('Corrupted payload accepted.');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('Payload', $error->getMessage());
            }
            $recorder->flush();
            $events = array_map(static fn (string $line): array => json_decode($line, true, flags: \JSON_THROW_ON_ERROR), explode("\n", trim($bytes)));
            $this->assertSame('error', $events[1]['outcome']);
        }
    }

    public static function identities(): iterable
    {
        foreach (['warmup:0', 'measure:0', 'warmup:0:result', 'measure:0:result', 'cycle:1:0'] as $id) {
            yield [$id];
        }
    }

    public function testCleanupDoesNotSignalReusedPidAndFailsClosedForSurvivor(): void
    {
        $start = 10;
        $read = static function (string $path) use (&$start): string {
            $fields = array_fill(0, 22, '0');
            $fields[0] = 'S';
            $fields[19] = (string) $start;

            return '123 (worker) '.implode(' ', $fields);
        };
        $resources = new Resources(Clock::system(), $read, static fn (string $bytes): bool => true, 100, 4096);
        $resources->register(Role::Persistence, 123);
        $signals = [];
        $kill = static function (int $pid) use (&$signals): bool {
            $signals[] = $pid;

            return false;
        };
        $this->assertFalse($resources->cleanup($kill, 0)['complete']);
        $this->assertSame([123], $signals);
        $start = 11;
        $signals = [];
        $this->assertTrue($resources->cleanup($kill, 0)['complete']);
        $this->assertSame([], $signals);
    }

    public function testStoppedPersistenceSurvivesBrokerDeathUntilOwnedCleanup(): void
    {
        $fixture = new \Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase();
        $process = new \Symfony\Component\Process\Process([\PHP_BINARY, \dirname(__DIR__, 2).'/bin/sqlite-queue', 'broker', '--database='.$fixture->path(), '--endpoint='.$fixture->path('broker.sock'), '--synchronous=full'], timeout: 10);
        $worker = 0;
        try {
            $process->start();
            $this->assertTrue($process->waitUntil(static fn (string $type, string $data): bool => str_contains($data, '"event":"ready"')));
            $ready = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
            $worker = $ready['persistence_pid'];
            $resources = new Resources(Clock::system(), static fn (string $path): string|false => @file_get_contents($path), static fn (string $bytes): bool => true, 100, 4096);
            $resources->register(Role::Broker, $ready['pid']);
            $resources->register(Role::Persistence, $worker);
            $this->assertTrue(posix_kill($worker, \SIGSTOP));
            $deadline = hrtime(true) + 10_000_000_000;
            do {
                $stat = file_get_contents('/proc/'.$worker.'/stat');
                $stopped = 1 === preg_match('/\) T /', $stat);
                if (hrtime(true) >= $deadline) {
                    $this->fail('Persistence worker did not enter stopped state.');
                }
            } while (!$stopped);
            $process->signal(\SIGKILL);
            $process->wait();
            $this->assertStringContainsString(') T ', file_get_contents('/proc/'.$worker.'/stat'));
            $signals = [];
            $result = $resources->cleanup(static function (int $pid) use (&$signals): bool {
                $signals[] = $pid;

                return posix_kill($pid, \SIGKILL);
            }, 10);
            $this->assertTrue($result['complete']);
            $this->assertContains($worker, $signals);
            $this->assertArrayHasKey(Role::Persistence->value, $result['roles']);
        } finally {
            $process->stop(0);
            if (isset($resources)) {
                $resources->cleanup(static fn (int $pid): bool => posix_kill($pid, \SIGKILL), 10);
            }
            @unlink($fixture->path('broker.sock'));
            $fixture->remove();
        }
    }
}
