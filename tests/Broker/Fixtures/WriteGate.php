<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\DeferredFuture;
use Amp\Future;

/**
 * Positive barrier for a response write that must stay pending until the test releases it.
 *
 * Production write timeouts close the socket; they do not complete this gate, so they cannot
 * free the blocked writer behind the test's back.
 */
final class WriteGate
{
    private readonly DeferredFuture $entered;
    private readonly DeferredFuture $release;

    public function __construct()
    {
        $this->entered = new DeferredFuture();
        $this->release = new DeferredFuture();
    }

    public function markEntered(int $bytes): void
    {
        if (!$this->entered->isComplete()) {
            $this->entered->complete($bytes);
        }
    }

    /** @return Future<int> */
    public function entered(): Future
    {
        return $this->entered->getFuture();
    }

    public function isReleased(): bool
    {
        return $this->release->isComplete();
    }

    /** @return Future<null> */
    public function released(): Future
    {
        return $this->release->getFuture();
    }

    public function release(): void
    {
        if (!$this->release->isComplete()) {
            $this->release->complete(null);
        }
    }
}
