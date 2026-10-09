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
 * @internal
 */
final class QueueNotifierWaiter
{
    public bool $settled = false;
    public ?string $cancellationId = null;
    public ?string $timeoutId = null;

    /** @var list<string> Watch keys this waiter occupies until settlement. */
    public array $keys = [];

    /**
     * Zero-duration probes settle false only after every selected queue has reported not-ready.
     *
     * @var list<string>
     */
    public array $pendingProbeKeys = [];

    /**
     * @param DeferredFuture<bool> $deferred
     */
    public function __construct(
        public readonly DeferredFuture $deferred,
        public readonly int $durationMilliseconds,
    ) {
    }
}
