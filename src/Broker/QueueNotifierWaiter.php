<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\DeferredFuture;

/**
 * One outstanding WAIT exchange.
 *
 * Zero duration is an explicit readiness probe: it registers, rechecks, and settles without an
 * event-loop timeout timer. Positive durations arm a Revolt delay and never compare wall-clock
 * expiry against the controlled clock.
 *
 * Known watch membership is constructor state. Zero-duration probes start with every selected key
 * pending and settle false only after each selected queue has a fresh not-ready sample.
 *
 * @internal
 */
final class QueueNotifierWaiter
{
    public bool $settled = false;
    public ?string $cancellationId = null;
    public ?string $timeoutId = null;

    /**
     * Zero-duration probes settle false only after every selected queue has reported not-ready.
     *
     * @var list<string>
     */
    public array $pendingProbeKeys;

    /**
     * @param DeferredFuture<bool> $deferred
     * @param list<string>         $keys     Watch keys this waiter occupies until settlement
     */
    public function __construct(
        public readonly DeferredFuture $deferred,
        public readonly int $durationMilliseconds,
        public readonly array $keys,
    ) {
        $this->pendingProbeKeys = 0 === $durationMilliseconds ? $keys : [];
    }
}
