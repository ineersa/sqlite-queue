<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class ErrorDetails
{
    /** @return array<string, mixed> */
    public static function from(\Throwable $error): array
    {
        $chain = [];
        $sqlstate = null;
        $sqlite = null;
        for ($i = 0; $i < 16; ++$i) {
            $chain[] = ['class' => $error::class, 'message' => $error->getMessage(), 'code' => $error->getCode()];
            if ($error instanceof \PDOException && \is_array($error->errorInfo)) {
                $sqlstate = $error->errorInfo[0] ?? $sqlstate;
                $sqlite = $error->errorInfo[1] ?? $sqlite;
            }
            if (\is_callable([$error, 'getSQLState'])) {
                $sqlstate = $error->getSQLState();
            }
            $previous = $error->getPrevious();
            if (null === $previous) {
                break;
            }
            $error = $previous;
        }

        return ['exception_chain' => $chain, 'sqlstate' => $sqlstate, 'sqlite_code' => $sqlite, 'extended_sqlite_code' => null, 'transaction_state' => null, 'internal_retry_number' => null];
    }
}
