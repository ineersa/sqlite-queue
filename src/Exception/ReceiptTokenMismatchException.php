<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

final class ReceiptTokenMismatchException extends InvalidReceiptException
{
    public function __construct()
    {
        parent::__construct('The receipt token does not match the current reservation token.');
    }
}
