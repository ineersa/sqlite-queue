<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\SqlExecutor;
use Ineersa\SqliteQueue\Sqlite\NativeQueueClock;
use Ineersa\SqliteQueue\Sqlite\QueueClock;
use Ineersa\SqliteQueue\Sqlite\QueueStorageOperation;
use Ineersa\SqliteQueue\Sqlite\QueueWorkerOperation;
use Symfony\Component\Clock\NativeClock;

/** Holds a real worker transaction at an explicit socket barrier, never by sleeping. */
final class DeadlineQueueWorkerOperation extends QueueWorkerOperation
{
    public const string CONTROL_ENVIRONMENT = 'SQLITE_QUEUE_DEADLINE_CONTROL';

    public function __construct(SqlExecutor $sql, QueueClock $clock, private readonly string $controlPath)
    {
        parent::__construct($sql, $clock);
    }

    public static function create(SqlExecutor $sql): self
    {
        $path = getenv(self::CONTROL_ENVIRONMENT);
        if (false === $path || '' === $path) {
            throw new \RuntimeException('Deadline worker requires its control path.');
        }

        return new self($sql, new NativeQueueClock(new NativeClock()), $path);
    }

    public function run(array $parameters): OperationResultDTO
    {
        $result = parent::run($parameters);
        if (!is_file($this->controlPath)) {
            return $result;
        }
        $plan = json_decode(file_get_contents($this->controlPath), true, flags: \JSON_THROW_ON_ERROR);
        $operation = QueueStorageOperation::from($parameters['operation']);
        if ($operation->value !== $plan['operation']) {
            return $result;
        }
        // Consume once: an accidental replay cannot masquerade as the same blocked request.
        unlink($this->controlPath);
        $socket = stream_socket_client($plan['uri']);
        if (false === $socket) {
            throw new \RuntimeException('Could not connect the deadline barrier.');
        }
        try {
            fwrite($socket, $operation->value."\n");
            if ("release\n" !== fgets($socket)) {
                throw new \RuntimeException('Deadline barrier closed without releasing the worker.');
            }
        } finally {
            fclose($socket);
        }

        return $result;
    }
}
