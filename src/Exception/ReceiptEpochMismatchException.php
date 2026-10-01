<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

final class ReceiptEpochMismatchException extends InvalidReceiptException
{
    public function __construct()
    {
        parent::__construct('The reservation belongs to a different queue engine epoch.');
    }
}
