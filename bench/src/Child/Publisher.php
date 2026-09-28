<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Child;

use Ineersa\SqliteQueue\Bench\Baseline;
use Ineersa\SqliteQueue\Bench\BenchMessage;
use Ineersa\SqliteQueue\Bench\Config;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class Publisher
{
    /** @var array<string, TransportInterface> */
    private array $transports = [];
    private int $position = 0;
    private bool $failed = false;

    public function __construct(private readonly Session $session)
    {
    }

    public function run(): void
    {
        $assignment = $this->session->assignment;
        $workload = $assignment->workload;

        if ($assignment->role === Role::Publisher) {
            $count = $workload['publishers'][$assignment->index]['count'];
            for ($index = 0; $index < $count; ++$index) {
                $queue = $workload['queues'][$index % count($workload['queues'])];
                $this->publish($queue, 0, true, $workload['pacing_us']);
            }
        } else {
            $this->prefill();
        }

        if ($this->failed) {
            throw new \RuntimeException('One or more sends failed; see raw send records.');
        }
    }

    private function prefill(): void
    {
        $workload = $this->session->assignment->workload;
        $prefill = $workload['prefill'];
        $pacing = $prefill['stagger_us'] ?? 0;

        foreach ($prefill['queues'] ?? $workload['queues'] as $queue) {
            for ($index = 0; $index < $prefill['per_queue']; ++$index) {
                $this->publish($queue, $prefill['delay_ms'] ?? 0, true, $pacing);
            }
        }
        foreach ($workload['probes'] ?? [] as $probe) {
            $this->publish($probe['queue'], $probe['delay_ms'], false, $pacing);
        }
    }

    private function publish(string $queue, int $delay, bool $measured, int $pacing): void
    {
        $assignment = $this->session->assignment;
        $target = $this->session->startedAt + $this->position * $pacing * 1000;
        $this->waitUntil($target);
        $position = $this->position++;
        $message = BenchMessage::generate(
            $assignment->label() . '-' . $position,
            $queue,
            Config::payloadSize($position),
        );
        $transport = $this->transports[$queue] ??= Baseline::transport($this->session->connection, $queue);
        $invokedAt = hrtime(true);
        $invokedWall = microtime(true);
        $error = null;
        $transportId = null;

        try {
            $result = $transport->send(new Envelope($message, [new DelayStamp($delay)]));
            $confirmedAt = hrtime(true);
            $transportId = $result->last(TransportMessageIdStamp::class)?->getId();
        } catch (\Throwable $exception) {
            $confirmedAt = hrtime(true);
            $error = $exception::class . ': ' . $exception->getMessage();
            $this->failed = true;
        }

        $this->session->recorder->sample([
            'kind' => 'send',
            'msg' => $message->corrId,
            'queue' => $queue,
            'transport_id' => $transportId,
            'ok' => $error === null,
            'error' => $error,
            'size' => $message->size,
            'measured' => $measured,
            't_invoke_ns' => $invokedAt,
            't_confirm_ns' => $confirmedAt,
            'duration_ms' => ($confirmedAt - $invokedAt) / 1e6,
            'schedule_lag_ms' => $pacing ? ($invokedAt - $target) / 1e6 : null,
            'requested_deadline_wall' => $invokedWall + $delay / 1000,
            'delay_ms' => $delay,
        ]);

        if ($error === null && $assignment->workload['name'] === 'roundtrip') {
            $remaining = $assignment->workload['timeout_s'] - (hrtime(true) - $this->session->startedAt) / 1e9;
            $this->session->awaitFile($assignment->path('acks/' . $message->corrId), max(0.01, $remaining));
        }
    }

    private function waitUntil(int $target): void
    {
        do {
            $this->session->assertWithinRunBound();
            $remaining = $target - hrtime(true);
            if ($remaining > 0) {
                usleep((int) min(1000, max(1, $remaining / 1000)));
            }
        } while ($remaining > 0);
    }
}
