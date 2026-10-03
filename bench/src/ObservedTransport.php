<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Transport\TransportInterface;

/** Observation only. The WAIT subscriber is wired to the original broker transport. */
final class ObservedTransport implements TransportInterface
{
    private Phase $phase = Phase::Warmup;

    public function __construct(private readonly TransportInterface $inner, private readonly Recorder $recorder, private readonly Control $control, private readonly Clock $clock)
    {
    }

    public function get(): iterable
    {
        $id = $this->recorder->nextId();
        $start = $this->clock->now();
        $activeStart = $start;
        $activeNanoseconds = 0;
        $suspended = false;
        $count = 0;
        $outcome = Outcome::Error;
        $error = '';
        $details = [];
        // A failed iterable must finish its telemetry before propagating the exception.
        $failure = null;
        try {
            // Consume the iterable inside the observed span; do not time generator creation alone.
            foreach ($this->inner->get() as $envelope) {
                ++$count;
                $message = $this->message($envelope);
                $this->phase = $message->phase;
                $now = $this->clock->now();
                $activeNanoseconds += $now - $activeStart;
                $this->recorder->record(Operation::Delivery, Outcome::Success, $this->phase, $id, $message->id, $start, $now, '', \strlen($message->payload));
                $suspended = true;
                yield $envelope;
                $suspended = false;
                $activeStart = $this->clock->now();
            }
            $outcome = 0 === $count ? Outcome::Empty : Outcome::Success;
        } catch (\Throwable $e) {
            $error = $e::class.': '.$e->getMessage();
            $details = ErrorDetails::from($e);
            $failure = $e;
        } finally {
            $end = $this->clock->now();
            if (!$suspended) {
                $activeNanoseconds += $end - $activeStart;
            }
            $this->recorder->record(Operation::Receive, $outcome, $this->phase, $id, '', $start, $end, $error, activeNanoseconds: $activeNanoseconds, details: $details);
        }
        if (null !== $failure) {
            throw $failure;
        }
    }

    public function send(Envelope $envelope): Envelope
    {
        $message = $this->message($envelope);
        $id = $this->recorder->nextId();
        $start = $this->clock->now();
        $stamp = $envelope->last(DelayStamp::class);
        $eligibility = $stamp instanceof DelayStamp ? RequestedEligibility::at($start, (int) floor(microtime(true) * 1000), $stamp->getDelay()) : [];
        $outcome = Outcome::Error;
        $error = '';
        $details = [];
        try {
            $result = $this->inner->send($envelope);
            $outcome = Outcome::Success;

            return $result;
        } catch (\Throwable $e) {
            $error = $e::class.': '.$e->getMessage();
            $details = ErrorDetails::from($e);
            throw $e;
        } finally {
            $this->recorder->record(Operation::Send, $outcome, $message->phase, $id, $message->id, $start, $this->clock->now(), $error, \strlen($message->payload), scheduledNs: $message->scheduledNs, details: $details, eligibility: $eligibility);
        }
    }

    public function ack(Envelope $envelope): void
    {
        $this->settle($envelope, Operation::Ack);
    }

    public function setPhase(Phase $phase): void
    {
        $this->phase = $phase;
    }

    public function reject(Envelope $envelope): void
    {
        $this->settle($envelope, Operation::Reject);
    }

    private function settle(Envelope $envelope, Operation $operation): void
    {
        $message = $this->message($envelope);
        $id = $this->recorder->nextId();
        $start = $this->clock->now();
        $outcome = Outcome::Error;
        $error = '';
        $details = [];
        try {
            if (Operation::Ack === $operation) {
                $this->inner->ack($envelope);
            } else {
                $this->inner->reject($envelope);
            }
            $outcome = Outcome::Success;
        } catch (\Throwable $e) {
            $error = $e::class.': '.$e->getMessage();
            $details = ErrorDetails::from($e);
            throw $e;
        } finally {
            $this->recorder->record($operation, $outcome, $message->phase, $id, $message->id, $start, $this->clock->now(), $error, \strlen($message->payload), scheduledNs: $message->scheduledNs, details: $details);
        }
        // Execution ACK remains telemetry; only the result ACK signals workflow completion.
        if ($message instanceof ApplicationMessage) {
            return;
        }
        // Control I/O is deliberately outside the public ACK span.
        $start = $this->clock->now();
        $outcome = Outcome::Error;
        $error = '';
        $details = [];
        try {
            $this->control->send($message->id);
            $outcome = Outcome::Success;
        } catch (\Throwable $e) {
            $error = $e::class.': '.$e->getMessage();
            $details = ErrorDetails::from($e);
            throw $e;
        } finally {
            $this->recorder->record(Operation::Control, $outcome, $message->phase, $this->recorder->nextId(), $message->id, $start, $this->clock->now(), $error);
        }
    }

    private function message(Envelope $envelope): BenchmarkMessage
    {
        $message = $envelope->getMessage();
        if (!$message instanceof BenchmarkMessage) {
            throw new \LogicException('Benchmark transport only observes BenchmarkMessage.');
        }

        return $message;
    }
}
