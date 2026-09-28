<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final readonly class AcknowledgeRequest implements Request
{
    public function __construct(public int $id, public string $receipt)
    {
    }

    public function operation(): Operation
    {
        return Operation::Acknowledge;
    }

    public function id(): int
    {
        return $this->id;
    }
}
