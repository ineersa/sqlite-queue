<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

final class ProtocolException extends \RuntimeException
{
    public function __construct(public readonly ErrorCode $errorCode, string $message)
    {
        parent::__construct($message);
    }
}
