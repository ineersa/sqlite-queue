<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Child;

use Ineersa\SqliteQueue\Bench\BenchMessage;
use Ineersa\SqliteQueue\Bench\Config;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Worker;

final readonly class Consumer
{
    public function __construct(private Session $session)
    {
    }

    public function run(): void
    {
        $assignment = $this->session->assignment;
        $receiver = new Receiver(
            $this->session,
            $assignment->workload['queues'],
            $this->session->recorder,
            $assignment->directory,
            $assignment->index,
            'roundtrip' === $assignment->workload['name'],
            $assignment->backend(),
        );
        $bus = new MessageBus([
            new HandleMessageMiddleware(new HandlersLocator([BenchMessage::class => [$receiver->enter(...)]])),
        ]);
        $events = new EventDispatcher();
        $events->addListener(WorkerRunningEvent::class, function (WorkerRunningEvent $event) use ($receiver): void {
            if ($this->session->isStopped() || $receiver->failed) {
                $event->getWorker()->stop();

                return;
            }
            if ($event->isWorkerIdle() && $receiver->waitsForNotifications()) {
                $receiver->wait();
            }
        });
        $worker = new Worker(['baseline' => $receiver], $bus, $events);

        try {
            $worker->run([
                'sleep' => $receiver->waitsForNotifications() ? 0 : Config::POLL_SLEEP_US,
                'time_limit' => $assignment->workload['timeout_s'],
            ]);
            if ($receiver->failed) {
                throw new \RuntimeException('Consumer delivery failed.');
            }
        } finally {
            $this->session->pollTotals = $receiver->polls;
        }
    }
}
