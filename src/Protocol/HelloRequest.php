<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final readonly class HelloRequest implements Request
{
    public function __construct(public int $id)
    {
    }

    public function operation(): Operation
    {
        return Operation::Hello;
    }

    public function id(): int
    {
        return $this->id;
    }
}
