<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class Cancellation
{
    private bool $requested = false;

    public function request(): void
    {
        $this->requested = true;
    }

    public function isRequested(): bool
    {
        return $this->requested;
    }

    public function throwIfRequested(): void
    {
        if ($this->requested) {
            throw new \RuntimeException('Benchmark interrupted');
        }
    }
}
