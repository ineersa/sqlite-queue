<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Exception\TransportException as ClientTransportException;
use Ineersa\SqliteQueue\Messenger\DTO\ConsumeWaitPendingDTO;
use Ineersa\SqliteQueue\Messenger\DTO\ConsumeWaitSessionDTO;
use Ineersa\SqliteQueue\Protocol\Limits;
use Psr\Container\ContainerInterface;
use Symfony\Component\Clock\ClockInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleErrorEvent;
use Symfony\Component\Console\Event\ConsoleSignalEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Command\ConsumeMessagesCommand;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Worker;

/**
 * Arms bounded Transport::wait() for stock messenger:consume on one sqlite-queue receiver.
 *
 * Activation requires exactly one literal receiver name that resolves through the Messenger
 * receiver locator to our Transport. When the user omits --sleep, the subscriber mutates the
 * sleep InputOption default to 0 so Command::run() rebind yields idleTimeout 0. Explicit
 * --sleep=0 arms WAIT without mutating the default. Explicit positive --sleep, including
 * fractional values such as 0.5, keeps native polling. Mixed, regex, and --all consumes are
 * left untouched.
 *
 * Default wait budget is 1000ms, matching the stock Messenger sleep default. The budget is capped
 * by any native --time-limit deadline. Invalid budgets are rejected. Stop listeners may
 * mark the worker stopped before this callback runs; without a public shouldStop getter the
 * worst-case idle stop latency is one wait budget unless ConsoleEvents::SIGNAL cancels first.
 */
final class NativeConsumeWaitSubscriber implements EventSubscriberInterface
{
    private const int DEFAULT_WAIT_BUDGET_MILLISECONDS = 1_000;

    /** @var \WeakMap<Command, ConsumeWaitPendingDTO> */
    private \WeakMap $pendingByCommand;

    /** @var \WeakMap<Worker, ConsumeWaitSessionDTO> */
    private \WeakMap $sessionsByWorker;

    /** @var \WeakMap<Command, ConsumeWaitSessionDTO> */
    private \WeakMap $sessionsByCommand;

    public function __construct(
        private readonly ContainerInterface $receiverLocator,
        private readonly ClockInterface $clock,
    ) {
        $this->pendingByCommand = new \WeakMap();
        $this->sessionsByWorker = new \WeakMap();
        $this->sessionsByCommand = new \WeakMap();
    }

    public static function getSubscribedEvents(): array
    {
        return [
            ConsoleEvents::COMMAND => ['onConsoleCommand', 0],
            ConsoleEvents::SIGNAL => ['onConsoleSignal', 0],
            ConsoleEvents::TERMINATE => ['onConsoleTerminate', 0],
            ConsoleEvents::ERROR => ['onConsoleError', 0],
            WorkerStartedEvent::class => ['onWorkerStarted', 0],
            WorkerRunningEvent::class => ['onWorkerRunning', -10],
            WorkerStoppedEvent::class => ['onWorkerStopped', 0],
        ];
    }

    public function onConsoleCommand(ConsoleCommandEvent $event): void
    {
        $this->restoreAllPending();

        $command = $event->getCommand();
        if (!$command instanceof ConsumeMessagesCommand) {
            return;
        }
        if (!$command->getDefinition()->hasOption('sleep')) {
            return;
        }

        $input = $event->getInput();
        if ($input->hasOption('all') && true === $input->getOption('all')) {
            return;
        }

        $receivers = $input->getArgument('receivers');
        if (!\is_array($receivers) || 1 !== \count($receivers) || !\is_string($receivers[0]) || '' === $receivers[0]) {
            return;
        }

        $explicitSleep = $this->explicitSleepSeconds($input->hasParameterOption(['--sleep'], true)
            ? $input->getParameterOption(['--sleep'], false, true)
            : null);
        if (ExplicitSleep::Positive === $explicitSleep) {
            return;
        }
        if (ExplicitSleep::Invalid === $explicitSleep) {
            return;
        }

        $name = $receivers[0];
        // Symfony 8.1 interprets receiver arguments as anchored regular expressions.
        // Symfony 8.0 uses literal names; use the same conservative activation on both.
        // Do not change its sleep default when one name could select several receivers.
        if (preg_quote($name, '{') !== $name) {
            return;
        }
        if (!$this->receiverLocator->has($name)) {
            return;
        }
        $receiver = $this->receiverLocator->get($name);
        if (!$receiver instanceof Transport) {
            return;
        }

        $option = $command->getDefinition()->getOption('sleep');
        $originalDefault = $option->getDefault();
        $mutateDefault = ExplicitSleep::Omitted === $explicitSleep;
        $pending = new ConsumeWaitPendingDTO(
            $command,
            $receiver,
            $name,
            $originalDefault,
            $mutateDefault,
            self::DEFAULT_WAIT_BUDGET_MILLISECONDS,
            $this->timeLimitSeconds($input->getOption('time-limit')),
        );
        $this->pendingByCommand[$command] = $pending;
        if ($mutateDefault) {
            $option->setDefault(0);
        }
    }

    public function onWorkerStarted(WorkerStartedEvent $event): void
    {
        $pending = $this->takePending();
        if (null === $pending) {
            return;
        }

        $names = $event->getWorker()->getMetadata()->getTransportNames();
        if ([0 => $pending->receiverName] !== $names) {
            $this->restoreSleepDefault($pending);

            return;
        }

        $idleTimeout = $this->idleTimeoutMicroseconds($event);
        if (null !== $idleTimeout && 0 !== $idleTimeout) {
            $this->restoreSleepDefault($pending);

            return;
        }

        $session = new ConsumeWaitSessionDTO(
            $pending->command,
            $pending->transport,
            $pending->receiverName,
            $pending->originalSleepDefault,
            $pending->sleepDefaultMutated,
            new DeferredCancellation(),
            $pending->waitBudgetMilliseconds,
            $this->deadline($event, $pending->timeLimitSeconds),
            $event->getWorker(),
        );
        $this->sessionsByWorker[$event->getWorker()] = $session;
        $this->sessionsByCommand[$pending->command] = $session;
    }

    public function onWorkerRunning(WorkerRunningEvent $event): void
    {
        $session = $this->sessionsByWorker[$event->getWorker()] ?? null;
        if (null === $session) {
            return;
        }

        if (!$event->isWorkerIdle()) {
            return;
        }
        if ($session->stop->isCancelled()) {
            return;
        }

        $timeoutMilliseconds = $this->waitTimeoutMilliseconds($session);
        if ($timeoutMilliseconds < 1) {
            $event->getWorker()->stop();

            return;
        }

        try {
            $session->transport->wait($timeoutMilliseconds, $session->stop->getCancellation());
        } catch (TransportException|ClientTransportException $error) {
            if ($this->isStopCancellation($session, $error)) {
                return;
            }

            throw $error;
        }
    }

    public function onConsoleSignal(ConsoleSignalEvent $event): void
    {
        if (\defined('SIGALRM') && \SIGALRM === $event->getHandlingSignal()) {
            return;
        }

        $command = $event->getCommand();
        if (null === $command) {
            return;
        }
        $session = $this->sessionsByCommand[$command] ?? null;
        $session?->stop->cancel();
    }

    public function onWorkerStopped(WorkerStoppedEvent $event): void
    {
        $session = $this->sessionsByWorker[$event->getWorker()] ?? null;
        if (null === $session) {
            return;
        }
        unset($this->sessionsByWorker[$event->getWorker()]);
        unset($this->sessionsByCommand[$session->command]);
        $session->stop->cancel();
        $this->restoreSleepDefault($session);
    }

    public function onConsoleTerminate(ConsoleTerminateEvent $event): void
    {
        $this->releaseCommand($event->getCommand());
    }

    public function onConsoleError(ConsoleErrorEvent $event): void
    {
        $this->releaseCommand($event->getCommand());
    }

    /**
     * @param Command|null $command consoleErrorEvent may expose a null command when failure
     *                              happens before command resolution; pending state is still cleared
     */
    private function releaseCommand(?Command $command): void
    {
        if (null === $command) {
            $this->restoreAllPending();

            return;
        }

        $pending = $this->pendingByCommand[$command] ?? null;
        if (null !== $pending) {
            unset($this->pendingByCommand[$command]);
            $this->restoreSleepDefault($pending);
        }

        $session = $this->sessionsByCommand[$command] ?? null;
        if (null === $session) {
            return;
        }
        unset($this->sessionsByCommand[$command]);
        unset($this->sessionsByWorker[$session->worker]);
        $session->stop->cancel();
        $this->restoreSleepDefault($session);
    }

    private function restoreAllPending(): void
    {
        foreach ($this->pendingByCommand as $command => $pending) {
            unset($this->pendingByCommand[$command]);
            $this->restoreSleepDefault($pending);
        }
    }

    private function takePending(): ?ConsumeWaitPendingDTO
    {
        foreach ($this->pendingByCommand as $command => $pending) {
            unset($this->pendingByCommand[$command]);
            foreach ($this->pendingByCommand as $extraCommand => $extraPending) {
                unset($this->pendingByCommand[$extraCommand]);
                $this->restoreSleepDefault($extraPending);
            }

            return $pending;
        }

        return null;
    }

    private function restoreSleepDefault(ConsumeWaitPendingDTO|ConsumeWaitSessionDTO $state): void
    {
        if ($state->sleepDefaultMutated) {
            $state->command->getDefinition()->getOption('sleep')->setDefault($state->originalSleepDefault);
        }
    }

    private function waitTimeoutMilliseconds(ConsumeWaitSessionDTO $session): int
    {
        $timeout = $session->waitBudgetMilliseconds;
        if ($timeout < 1) {
            throw new \InvalidArgumentException('Wait budget must be positive milliseconds.');
        }
        if ($timeout > Limits::MAX_WAIT_MILLISECONDS) {
            throw new \InvalidArgumentException('Wait budget exceeds the protocol maximum.');
        }
        if (null === $session->deadline) {
            return $timeout;
        }

        $remainingSeconds = $session->deadline - $this->nowSeconds();
        if ($remainingSeconds <= 0) {
            return 0;
        }

        $budgetSeconds = $timeout / 1000;
        $waitSeconds = min($budgetSeconds, $remainingSeconds);

        return (int) ceil($waitSeconds * 1000);
    }

    // Keep the version-dependent event API structural: these methods exist only in 8.1+.
    private function idleTimeoutMicroseconds(object $event): ?int
    {
        // Symfony 8.0 does not expose these worker options on its started event.
        if (\is_callable([$event, 'getIdleTimeout'])) {
            return $event->getIdleTimeout();
        }

        return null;
    }

    private function deadline(object $event, ?int $timeLimitSeconds): ?float
    {
        if (\is_callable([$event, 'getDeadline'])) {
            $deadline = $event->getDeadline();
            if (null !== $deadline) {
                return $deadline;
            }
        }
        if (null === $timeLimitSeconds) {
            return null;
        }

        return $this->nowSeconds() + $timeLimitSeconds;
    }

    private function nowSeconds(): float
    {
        return (float) $this->clock->now()->format('U.u');
    }

    private function timeLimitSeconds(mixed $timeLimit): ?int
    {
        if (null === $timeLimit || false === $timeLimit || '' === $timeLimit) {
            return null;
        }
        if (\is_string($timeLimit) && !is_numeric($timeLimit)) {
            return null;
        }
        $seconds = (int) $timeLimit;
        if ($seconds <= 0) {
            return null;
        }

        return $seconds;
    }

    private function explicitSleepSeconds(mixed $rawSleep): ExplicitSleep
    {
        if (null === $rawSleep) {
            return ExplicitSleep::Omitted;
        }
        if (false === $rawSleep || !is_numeric($rawSleep)) {
            return ExplicitSleep::Invalid;
        }
        $seconds = (float) $rawSleep;
        if (!is_finite($seconds)) {
            return ExplicitSleep::Invalid;
        }
        if ($seconds > 0.0) {
            return ExplicitSleep::Positive;
        }
        if (0.0 === $seconds) {
            return ExplicitSleep::Zero;
        }

        return ExplicitSleep::Invalid;
    }

    private function isStopCancellation(ConsumeWaitSessionDTO $session, \Throwable $error): bool
    {
        if (!$session->stop->isCancelled()) {
            return false;
        }
        for ($current = $error; null !== $current; $current = $current->getPrevious()) {
            if ($current instanceof CancelledException) {
                return true;
            }
        }

        return false;
    }
}
