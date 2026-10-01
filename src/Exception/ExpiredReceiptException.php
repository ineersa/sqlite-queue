<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

final class ExpiredReceiptException extends InvalidReceiptException
{
    public function __construct()
    {
        parent::__construct('The reservation visibility deadline has expired.');
    }
}
