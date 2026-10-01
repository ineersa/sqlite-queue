<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

final class MalformedReceiptException extends InvalidReceiptException
{
    public function __construct()
    {
        parent::__construct('Receipt must contain a positive supported message ID and a 64-character lowercase hexadecimal token.');
    }
}
