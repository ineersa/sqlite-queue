<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerOperationEnum;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerStatusEnum;

/*
 * Minimal worker for SqliteWorkerContextFactoryTest.
 *
 * Answers the production init handshake, then blocks until the parent kills it.
 * It never opens SQLite: factory lifecycle coverage does not need storage.
 */
return static function (Channel $channel): null {
    $request = $channel->receive();
    if (!\is_array($request)
        || 1 !== ($request['id'] ?? null)
        || SqliteWorkerOperationEnum::Init->value !== ($request['op'] ?? null)
        || !\is_array($request['data'] ?? null)
        || !\is_string($request['data']['synchronous'] ?? null)
    ) {
        throw new \RuntimeException('Probe expected a validated init request.');
    }

    $channel->send([
        'id' => 1,
        'op' => SqliteWorkerOperationEnum::Init->value,
        'status' => SqliteWorkerStatusEnum::Ok->value,
        'result' => [
            'journal_mode' => 'wal',
            'synchronous' => $request['data']['synchronous'],
            'busy_timeout' => 5_000,
            'wal_autocheckpoint' => 1_000,
            'sqlite_version' => '3.31.0',
        ],
    ]);

    while (null !== $channel->receive()) {
    }

    return null;
};
