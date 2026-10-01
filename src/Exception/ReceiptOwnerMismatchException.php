<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

final class ReceiptOwnerMismatchException extends InvalidReceiptException
{
    public function __construct()
    {
        parent::__construct('The reservation belongs to a different client connection.');
    }
}
