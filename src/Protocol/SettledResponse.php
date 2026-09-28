<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final readonly class SettledResponse implements Response
{
    public function __construct(public int $id)
    {
    }

    public function id(): int
    {
        return $this->id;
    }
}
