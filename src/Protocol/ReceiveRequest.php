<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final readonly class ReceiveRequest implements Request
{
    public function __construct(public int $id, public string $queue)
    {
    }

    public function operation(): Operation
    {
        return Operation::Receive;
    }

    public function id(): int
    {
        return $this->id;
    }
}
