<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Analysis;
use Ineersa\SqliteQueue\Bench\Clock;
use Ineersa\SqliteQueue\Bench\Control;
use Ineersa\SqliteQueue\Bench\Handler;
use Ineersa\SqliteQueue\Bench\ObservedTransport;
use Ineersa\SqliteQueue\Bench\Operation;
use Ineersa\SqliteQueue\Bench\Outcome;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\ProbeMessage;
use Ineersa\SqliteQueue\Bench\Recorder;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class TelemetryTest extends TestCase
{
    public function testReceiveIterationThrowIsRecorded(): void
    {
        $bytes = '';
        $recorder = new Recorder(static function (string $data) use (&$bytes): bool {
            $bytes .= $data;

            return true;
        }, 4096, 'run', 'consumer');
        $transport = $this->createStub(TransportInterface::class);
        $transport->method('get')->willReturnCallback(static function (): \Generator {
            throw new \RuntimeException('receive boundary');
            yield;
        });
        $pair = stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        try {
            $observed = new ObservedTransport($transport, $recorder, new Control($pair[0]), Clock::system());
            try {
                iterator_to_array($observed->get());
                $this->fail('Expected receive failure.');
            } catch (\RuntimeException $error) {
                $this->assertSame('receive boundary', $error->getMessage());
            }
            $recorder->flush();
            $event = json_decode(trim($bytes), true, flags: \JSON_THROW_ON_ERROR);
            $this->assertSame('receive', $event['operation']);
            $this->assertSame('error', $event['outcome']);
            $this->assertGreaterThanOrEqual($event['started_ns'], $event['ended_ns']);
            $this->assertNotEmpty($event['operation_id']);
        } finally {
            fclose($pair[0]);
            fclose($pair[1]);
        }
    }

    public function testAckFailureDoesNotEraseDeliveryOrHandlerEntry(): void
    {
        $bytes = '';
        $recorder = new Recorder(static function (string $data) use (&$bytes): bool {
            $bytes .= $data;

            return true;
        }, 8192, 'run', 'consumer');
        $envelope = new Envelope(new ProbeMessage('measure:0', Phase::Measure, \Ineersa\SqliteQueue\Bench\Payload::generate('measure:0'), false));
        $inner = $this->createStub(TransportInterface::class);
        $inner->method('get')->willReturn([$envelope]);
        $inner->method('ack')->willThrowException(new \RuntimeException('ACK reply lost'));
        $pair = stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        try {
            $observed = new ObservedTransport($inner, $recorder, new Control($pair[0]), Clock::system());
            iterator_to_array($observed->get());
            (new Handler($recorder, $this->createStub(MessageBusInterface::class), Clock::system()))($envelope->getMessage());
            try {
                $observed->ack($envelope);
                $this->fail('ACK must throw.');
            } catch (\RuntimeException) {
            }
            $recorder->flush();
            $events = array_map(static fn (string $line): array => json_decode($line, true, flags: \JSON_THROW_ON_ERROR), explode("\n", trim($bytes)));
            $this->assertSame(['delivery', 'receive', 'handler', 'handler', 'ack'], array_column($events, 'operation'));
            $this->assertSame('enter', $events[2]['outcome']);
            $this->assertSame('success', $events[3]['outcome']);
            $this->assertSame('error', $events[4]['outcome']);
            $this->assertTrue($events[4]['unknown_commit']);
        } finally {
            fclose($pair[0]);
            fclose($pair[1]);
        }
    }

    public function testUniqueAccountingPreservesWindowAndCohortMembership(): void
    {
        $events = [];
        foreach (['a' => 150, 'b' => 200, 'outsider' => 150] as $id => $time) {
            foreach (['send', 'delivery', 'handler', 'ack'] as $operation) {
                $events[] = ['phase' => 'measure', 'operation' => $operation, 'outcome' => 'success', 'correlation' => $id, 'started_ns' => 100, 'ended_ns' => 'ack' === $operation ? $time : 110];
            }
        }
        $events[] = ['phase' => 'measure', 'operation' => 'ack', 'outcome' => 'success', 'correlation' => 'a', 'started_ns' => 100, 'ended_ns' => 160];
        $path = sys_get_temp_dir().'/sq-telemetry-analysis-'.bin2hex(random_bytes(6));
        try {
            $summary = Analysis::build($path, $events, ['a', 'b', 'missing'], 100, 200, Phase::Measure);
            $this->assertSame(2, $summary['unique_completions']);
            $this->assertSame(1, $summary['window_unique_completions']);
            $this->assertSame(1, $summary['duplicates']);
            $this->assertSame(1, $summary['unexpected']);
            $this->assertSame(1, $summary['unfinished']);
            $this->assertSame('fail', $summary['integrity_status']);
            $this->assertNull($summary['cohort_seconds']);
            $complete = Analysis::build($path.'-complete', $events, ['a', 'b'], 100, 200, Phase::Measure);
            $this->assertSame(100 / 1e9, $complete['cohort_seconds']);
        } finally {
            @unlink($path);
            @unlink($path.'-complete');
        }
    }

    public function testRecorderCapacityAndWriteFailureAreExplicit(): void
    {
        $writes = 0;
        $recorder = new Recorder(static function (string $bytes) use (&$writes): bool {
            ++$writes;

            return false;
        }, 1024, 'r', 'c');
        for ($i = 0; $i < 20; ++$i) {
            $recorder->record(Operation::Receive, Outcome::Error, Phase::Measure, (string) $i, '', 1, 2, 'failed');
            $this->assertLessThanOrEqual(1024, $recorder->counters()['buffered_bytes']);
        }
        $recorder->flush();
        $this->assertSame(20, $recorder->counters()['lost_records']);
        $this->assertSame($writes, $recorder->counters()['write_failures']);
        $oversized = new Recorder(static fn (): bool => true, 1, 'r', 'c');
        $oversized->record(Operation::Send, Outcome::Error, Phase::Measure, '1', 'a', 1, 2, '');
        $this->assertSame(1, $oversized->counters()['lost_records']);
        $this->assertSame(0, $oversized->counters()['buffered_bytes']);
    }

    public function testStreamingReadDoesNotMaterializeFile(): void
    {
        $path = tempnam(sys_get_temp_dir(), 'v4-events');
        file_put_contents($path, "{\"n\":1}\n{\"n\":2}\n");
        try {
            $events = Recorder::read($path);
            $this->assertInstanceOf(\Generator::class, $events);
            $events->rewind();
            $this->assertSame(['n' => 1], $events->current());
            $events->next();
            $this->assertSame(['n' => 2], $events->current());
        } finally {
            unlink($path);
        }
    }

    public function testHotPathObservationHasNoDatabaseDependency(): void
    {
        foreach ([ObservedTransport::class, Handler::class, Recorder::class, Control::class, \Ineersa\SqliteQueue\Bench\Runner::class] as $class) {
            $source = file_get_contents((new \ReflectionClass($class))->getFileName());
            $this->assertStringNotContainsString('PDO', $source);
            $this->assertStringNotContainsString('executeQuery', $source);
            $this->assertStringNotContainsString('SELECT', $source);
        }
    }

    public function testReceiveActiveDurationExcludesTimeYieldedToTheConsumer(): void
    {
        $bytes = '';
        $now = 100;
        $clock = new Clock(static function () use (&$now): int { return $now; });
        $recorder = new Recorder(static function (string $data) use (&$bytes): bool {
            $bytes .= $data;

            return true;
        }, 4096, 'run', 'consumer');
        $inner = $this->createStub(TransportInterface::class);
        $inner->method('get')->willReturnCallback(static function () use (&$now): \Generator {
            $now = 120;
            yield new Envelope(new ProbeMessage('id', Phase::Measure, str_repeat('x', 256), false));
            $now = 10010;
        });
        $pair = stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        try {
            $iterator = (new ObservedTransport($inner, $recorder, new Control($pair[0]), $clock))->get();
            $iterator->rewind();
            $now = 10000;
            $iterator->next();
            $recorder->flush();
            $events = array_map(static fn (string $line): array => json_decode($line, true, flags: \JSON_THROW_ON_ERROR), explode("\n", trim($bytes)));
            $this->assertSame(120, $events[0]['ended_ns']);
            $this->assertSame(10010, $events[1]['ended_ns']);
            $this->assertSame(30, $events[1]['active_ns']);
            $this->assertSame('iterable-lifetime', $events[1]['span_scope']);
        } finally {
            fclose($pair[0]);
            fclose($pair[1]);
        }
    }
}
