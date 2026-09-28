<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

interface Response
{
    public function id(): int;
}
