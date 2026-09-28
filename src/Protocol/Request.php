<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

interface Request
{
    public function operation(): Operation;

    public function id(): int;
}
