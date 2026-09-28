<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final readonly class FailedResponse implements Response
{
    public function __construct(public int $id, public ErrorCode $code)
    {
    }

    public function id(): int
    {
        return $this->id;
    }
}
