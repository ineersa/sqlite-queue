<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite\Exception;

/**
 * Storage lane failed after dispatch or during IPC.
 *
 * Outcome may be unknown. Never replay the operation.
 */
final class StorageFailureException extends \RuntimeException
{
}
