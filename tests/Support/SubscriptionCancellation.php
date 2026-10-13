<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Amp\Cancellation;
use Amp\DeferredFuture;

/** Exposes admission's cancellation subscription as an explicit test barrier. */
final readonly class SubscriptionCancellation implements Cancellation
{
    /** @param DeferredFuture<null> $subscribed */
    public function __construct(private Cancellation $cancellation, private DeferredFuture $subscribed)
    {
    }

    public function subscribe(\Closure $callback): string
    {
        $id = $this->cancellation->subscribe($callback);
        $this->subscribed->complete();

        return $id;
    }

    public function unsubscribe(string $id): void
    {
        $this->cancellation->unsubscribe($id);
    }

    public function isRequested(): bool
    {
        return $this->cancellation->isRequested();
    }

    public function throwIfRequested(): void
    {
        $this->cancellation->throwIfRequested();
    }
}
