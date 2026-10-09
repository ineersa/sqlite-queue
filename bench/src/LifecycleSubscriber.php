<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;

final class LifecycleSubscriber implements EventSubscriberInterface
{
    private bool $ready = false;
    // A requested idle barrier may arrive on a non-idle WorkerRunningEvent.
    private ?string $idleRequest = null;

    /** @param list<ObservedTransport> $transports transports share the actor recorder and phase boundaries */
    public function __construct(private readonly Recorder $recorder, private readonly Control $control, private readonly array $transports)
    {
    }

    public static function getSubscribedEvents(): array
    {
        // Announce the first empty receive before the production subscriber runs WAIT.
        return [WorkerStartedEvent::class => 'started', WorkerRunningEvent::class => ['running', 100], WorkerStoppedEvent::class => 'stopped'];
    }

    public function started(WorkerStartedEvent $event): void
    {
        $idle = \is_callable([$event, 'getIdleTimeout']) ? $event->getIdleTimeout() : null;
        Runtime::saveText(Runtime::environment('BENCH_TELEMETRY').'.worker.json', json_encode(['pid' => getmypid(), 'idle_timeout_microseconds' => $idle, 'receivers' => $event->getWorker()->getMetadata()->getTransportNames()], \JSON_THROW_ON_ERROR));
    }

    public function running(WorkerRunningEvent $event): void
    {
        if ($this->control->hasPacket()) {
            $packet = $this->control->receive();
            if (str_starts_with($packet, 'arm-idle:')) {
                $this->idleRequest = $packet;
            } elseif ('hold' === $packet) {
                // Concurrent warmup uses an observable barrier so both stock workers handle work.
                $this->control->send('hold');
                if ('resume' !== $this->control->receive()) {
                    throw new \RuntimeException('Consumer warmup resume missing.');
                }
                $this->control->send('resume');
            } elseif (str_starts_with($packet, 'snapshot:')) {
                $this->control->sendPacket(['id' => $packet, 'pid' => getmypid(), 'used_bytes' => memory_get_usage(false), 'reserved_bytes' => memory_get_usage(true), 'peak_bytes' => memory_get_peak_usage(true), 'monotonic_ns' => hrtime(true)]);
            } else {
                $phase = Phase::from($packet);
                foreach ($this->transports as $transport) {
                    $transport->setPhase($phase);
                }
                $this->recorder->flush();
                Runtime::saveJson(Runtime::environment('BENCH_TELEMETRY').'.counters.json', $this->recorder->counters());
                $this->control->send($packet);
            }
        }
        if (null !== $this->idleRequest && $event->isWorkerIdle()) {
            $this->control->sendPacket(['id' => $this->idleRequest, 'pid' => getmypid(), 'monotonic_ns' => hrtime(true), 'worker_idle' => true, 'wait_registration' => null, 'coverage' => 'WorkerRunningEvent idle after empty receive, before native WAIT listener']);
            $this->idleRequest = null;
        }
        if (!$this->ready && $event->isWorkerIdle()) {
            $this->ready = true;
            $this->control->send('ready');
        }
    }

    public function stopped(): void
    {
        $this->recorder->flush();
        Runtime::saveJson(Runtime::environment('BENCH_TELEMETRY').'.counters.json', $this->recorder->counters() + ['finalized' => true, 'pid' => getmypid(), 'finalized_ns' => hrtime(true)]);
    }
}
