<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Analysis;
use Ineersa\SqliteQueue\Bench\Clock;
use Ineersa\SqliteQueue\Bench\Control;
use Ineersa\SqliteQueue\Bench\IdleMetrics;
use Ineersa\SqliteQueue\Bench\LifecycleSubscriber;
use Ineersa\SqliteQueue\Bench\ObservedTransport;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\Recorder;
use Ineersa\SqliteQueue\Bench\RequestedEligibility;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Worker;

final class WakeupMetricsTest extends TestCase
{
    public function testIdleCountersExcludeWarmupAndPickupAtHalfOpenBoundaries(): void
    {
        $event = ['operation' => 'receive', 'outcome' => 'empty', 'role' => 'consumer', 'ended_ns' => 100];
        $events = [array_replace($event, ['ended_ns' => 99]), $event, array_replace($event, ['ended_ns' => 199]), array_replace($event, ['ended_ns' => 200]), ['operation' => 'send', 'started_ns' => 200]];
        $result = IdleMetrics::summarize($events, 100, 200);
        $this->assertSame(2, $result['receive_empty']);
        $this->assertSame(2, $result['receive_attempts']);
        $this->assertSame(0, $result['publication_attempts']);
        $this->assertSame('pass', $result['integrity_status']);
        $this->assertNull($result['wait_registrations']);
        $this->assertNull($result['wait_wakes']);
        $events[] = ['operation' => 'send', 'started_ns' => 150];
        $this->assertSame('fail', IdleMetrics::summarize($events, 100, 200)['integrity_status']);
    }

    public function testRequestedLatenessUsesExplicitAnchorsAndKeepsNegativeValues(): void
    {
        $anchor = RequestedEligibility::at(10000000, 1700000000000, 50);
        $this->assertSame(60000000, $anchor['requested_monotonic_ns']);
        $this->assertSame(1700000000050, $anchor['requested_wall_ms']);
        $base = ['phase' => 'measure', 'correlation' => 'actual-id', 'payload_bytes' => 256, 'outcome' => 'success', 'started_ns' => 10000000];
        $events = [
            $base + ['operation' => 'send', 'ended_ns' => 20000000, 'eligibility' => $anchor],
            $base + ['operation' => 'delivery', 'ended_ns' => 55000000],
            array_replace($base, ['operation' => 'handler', 'outcome' => 'enter', 'started_ns' => 56000000, 'ended_ns' => 56000000]),
            $base + ['operation' => 'handler', 'ended_ns' => 57000000],
            $base + ['operation' => 'ack', 'ended_ns' => 58000000],
        ];
        $path = tempnam(sys_get_temp_dir(), 'sq-wakeup-');
        try {
            $result = Analysis::build($path, $events, ['actual-id'], 10000000, 100000000, Phase::Measure);
            $this->assertSame('pass', $result['integrity_status']);
            $this->assertSame(-5.0, $result['latencies']['requested_delivery_lateness_ms']['p50']);
            $this->assertSame(-4.0, $result['latencies']['requested_handler_lateness_ms']['p50']);
            $this->assertSame(1, $result['latencies']['requested_delivery_lateness_ms']['count']);
            $database = new \SQLite3($path);
            try {
                $this->assertSame(1700000000000, $database->querySingle('SELECT anchor_wall FROM observations'));
            } finally {
                $database->close();
            }
        } finally {
            unlink($path);
        }
    }

    public function testIdleRejectsUnexpectedDeliveryAndReceiveFailureIndependently(): void
    {
        $delivery = IdleMetrics::summarize([['operation' => 'delivery', 'ended_ns' => 150]], 100, 200);
        $this->assertSame(1, $delivery['unexpected_work_records']);
        $this->assertSame('fail', $delivery['integrity_status']);
        $failure = IdleMetrics::summarize([['operation' => 'receive', 'role' => 'consumer', 'outcome' => 'error', 'ended_ns' => 150]], 100, 200);
        $this->assertSame(1, $failure['receive_errors']);
        $this->assertSame('fail', $failure['integrity_status']);
    }

    public function testIdleBarrierWaitsForAnActualIdleWorkerEvent(): void
    {
        $pair = stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        $producer = new Control($pair[0]);
        $consumer = new Control($pair[1]);
        $recorder = new Recorder(static fn (string $bytes): bool => true, 4096, 'run', 'consumer');
        $transport = new ObservedTransport($this->createStub(TransportInterface::class), $recorder, $consumer, new Clock(static fn (): int => 100));
        $subscriber = new LifecycleSubscriber($recorder, $consumer, $transport);
        $worker = $this->createStub(Worker::class);
        try {
            $producer->send('arm-idle:proof');
            $subscriber->running(new WorkerRunningEvent($worker, false));
            $this->assertFalse($producer->hasPacket());
            $subscriber->running(new WorkerRunningEvent($worker, true));
            $packet = $producer->receivePacket();
            $this->assertSame('arm-idle:proof', $packet['id']);
            $this->assertTrue($packet['worker_idle']);
            $this->assertNull($packet['wait_registration']);
        } finally {
            $producer->close();
            $consumer->close();
        }
    }
}
