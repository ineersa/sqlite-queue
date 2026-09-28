<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Ineersa\SqliteQueue\Delivery;

final readonly class ReceivedResponse implements Response
{
    public function __construct(public int $id, public Delivery $delivery)
    {
    }

    public function id(): int
    {
        return $this->id;
    }
}
