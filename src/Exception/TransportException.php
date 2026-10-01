<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

/**
 * No confirmation was obtained. Never replay the operation implicitly.
 *
 * SQLite can commit before its result reaches the broker or before the broker's reply
 * reaches the client. If that connection fails, the caller cannot distinguish a committed
 * operation with a lost confirmation from a failure before commit.
 */
final class TransportException extends \RuntimeException
{
}
