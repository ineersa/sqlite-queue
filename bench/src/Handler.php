<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Symfony\Component\Messenger\MessageBusInterface;

final readonly class Handler
{
    public function __construct(private Recorder $recorder, private MessageBusInterface $bus, private Clock $clock)
    {
    }

    public function __invoke(BenchmarkMessage $message): void
    {
        $id = $this->recorder->nextId();
        $start = $this->clock->now();
        $this->recorder->record(Operation::Handler, Outcome::Enter, $message->phase, $id, $message->id, $start, $start, '', \strlen($message->payload));
        $outcome = Outcome::Error;
        $error = '';
        try {
            if (!\in_array(\strlen($message->payload), [Config::SMALL_PAYLOAD_BYTES, Config::LARGE_PAYLOAD_BYTES], true)) {
                throw new \RuntimeException('Unexpected payload stratum.');
            }
            if ($message->workMilliseconds > 0) {
                usleep($message->workMilliseconds * 1000);
            }
            if ($message->followUp) {
                $this->bus->dispatch(new ResultMessage($message->id.WorkflowAnalysis::RESULT_SUFFIX, $message->phase, $message->payload, false));
            }
            $outcome = Outcome::Success;
        } catch (\Throwable $e) {
            $error = $e::class.': '.$e->getMessage();
            throw $e;
        } finally {
            $this->recorder->record(Operation::Handler, $outcome, $message->phase, $id, $message->id, $start, $this->clock->now(), $error, \strlen($message->payload));
        }
    }
}
