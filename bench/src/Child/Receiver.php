<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Child;

use Doctrine\DBAL\Connection;
use Ineersa\SqliteQueue\Bench\{Baseline, BenchMessage, Config};
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;

/** Measures public transport calls without changing the Doctrine claim SQL. */
final class Receiver implements ReceiverInterface
{
    public array $polls = ['polls' => 0, 'empty_polls' => 0, 'fetched' => 0, 'poll_ms' => 0.0];
    public array $entries = [];
    public bool $failed = false;
    private array $transports = [];
    private int $next = 0;

    public function __construct(private Connection $connection, array $queues, private Recorder $recorder, private string $directory, private int $index, private bool $roundtrip = false)
    {
        foreach ($queues as $queue) {
            $this->transports[$queue] = Baseline::transport($connection, $queue);
        }
    }

    public function get(): iterable
    {
        $queues = array_keys($this->transports);
        foreach ($queues as $_) {
            $queue = $queues[$this->next++ % count($queues)];
            $start = hrtime(true);
            $envelopes = iterator_to_array($this->transports[$queue]->get());
            $this->polls['poll_ms'] += (hrtime(true) - $start) / 1e6;
            ++$this->polls['polls'];
            $this->polls['fetched'] += count($envelopes);
            if (!$envelopes) {
                ++$this->polls['empty_polls'];
            } else {
                yield from $envelopes;
                return;
            }
        }
        // Positive evidence that the real worker reached an empty poll.
        if (!is_file($this->directory . '/idle-' . $this->index)) {
            file_put_contents($this->directory . '/idle-' . $this->index, (string) hrtime(true));
        }
    }

    public function enter(BenchMessage $message): void
    {
        $this->entries[$message->corrId] = ['t_handler_ns' => hrtime(true), 'handler_wall' => microtime(true), 'payload_ok' => $message->matchesRegeneratedPayload()];
        if (!$this->entries[$message->corrId]['payload_ok']) {
            throw new \RuntimeException('Payload mismatch: ' . $message->corrId);
        }
    }

    public function ack(Envelope $envelope): void
    {
        $this->finish($envelope, 'ack');
    }

    public function reject(Envelope $envelope): void
    {
        $this->failed = true;
        $this->finish($envelope, 'reject');
    }

    private function finish(Envelope $envelope, string $outcome): void
    {
        $message = $envelope->getMessage();
        if (!$message instanceof BenchMessage) {
            throw new \RuntimeException('Unexpected message type');
        }
        $entry = $this->entries[$message->corrId] ?? ['t_handler_ns' => hrtime(true), 'handler_wall' => microtime(true), 'payload_ok' => false];
        // The receiver has committed by this point. No transaction is held during the handler.
        $rowId = $envelope->last(TransportMessageIdStamp::class)?->getId();
        $stored = $this->connection->fetchOne('SELECT available_at FROM messenger_messages WHERE id = ?', [$rowId]);
        $deadline = $stored === false ? null : (float) (new \DateTimeImmutable((string) $stored, new \DateTimeZone('UTC')))->format('U.u');
        $start = hrtime(true);
        $error = null;
        try {
            $this->transports[$message->queue]->$outcome($envelope);
        } catch (\Throwable $e) {
            $error = $e::class . ': ' . $e->getMessage();
            $this->failed = true;
            throw $e;
        } finally {
            $end = hrtime(true);
            if ($this->roundtrip && $outcome === 'ack' && $error === null) {
                file_put_contents($this->directory . '/acks/' . $message->corrId, 'ack');
            }
            $this->recorder->sample($entry + [
                'kind' => 'deliver', 'msg' => $message->corrId, 'queue' => $message->queue, 'transport_id' => $rowId,
                'outcome' => $error === null ? $outcome : 'error', 'error' => $error,
                't_ack_invoke_ns' => $start, 't_ack_confirm_ns' => $error === null ? $end : null,
                'ack_duration_ms' => ($end - $start) / 1e6, 'handling_ms' => ($end - $entry['t_handler_ns']) / 1e6,
                'stored_deadline_wall' => $deadline,
                'lateness_ms' => $deadline === null ? null : ($entry['handler_wall'] - $deadline) * 1000,
                'delivered_before_deadline' => $deadline !== null && $entry['handler_wall'] < $deadline,
            ]);
            unset($this->entries[$message->corrId]);
        }
    }
}
