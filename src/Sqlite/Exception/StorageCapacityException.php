<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite\Exception;

/**
 * Local admission refused before channel dispatch.
 *
 * Capacity exhaustion is not a storage-lane failure and must not kill the worker.
 */
final class StorageCapacityException extends \RuntimeException
{
}
