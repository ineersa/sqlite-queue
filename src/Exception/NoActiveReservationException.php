<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

final class NoActiveReservationException extends InvalidReceiptException
{
    public function __construct()
    {
        parent::__construct('The receipt does not identify an active reservation. The message is absent or unreserved.');
    }
}
