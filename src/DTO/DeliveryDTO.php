<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\DTO;

final readonly class DeliveryDTO
{
    public function __construct(
        public int $id,
        public string $queue,
        public string $body,
        public string $headers,
        public string $receipt,
        public int $availableAt,
        public int $reservedUntil,
    ) {
    }
}
