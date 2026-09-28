<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final readonly class SentResponse implements Response
{
    public function __construct(public int $id, public int $messageId)
    {
    }

    public function id(): int
    {
        return $this->id;
    }
}
