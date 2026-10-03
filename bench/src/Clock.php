<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** A replaceable monotonic clock for public-operation boundaries. */
final readonly class Clock
{
    /** @param \Closure(): int $read */
    public function __construct(private \Closure $read)
    {
    }

    public static function system(): self
    {
        return new self(static fn (): int => hrtime(true));
    }

    public function now(): int
    {
        return ($this->read)();
    }
}
