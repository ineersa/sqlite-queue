<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final readonly class SendRequest implements Request
{
    public function __construct(
        public int $id,
        public string $queue,
        public int $delay,
        public string $body,
        public string $headers,
    ) {
    }

    public function operation(): Operation
    {
        return Operation::Send;
    }

    public function id(): int
    {
        return $this->id;
    }
}
