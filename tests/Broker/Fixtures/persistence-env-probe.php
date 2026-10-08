<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerOperationEnum;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerStatusEnum;

/*
 * Reports what the persistence child received from the broker process.
 *
 * Answers the production init handshake first, then sends the environment report
 * that SqliteWorkerContextFactoryTest receives after create() returns. The first
 * argument names the environment variable the caller set, and the second names an
 * ini directive the caller configured. The child exits immediately after the report.
 */
return static function (Channel $channel) use ($argv): null {
    $marker = $argv[1] ?? '';
    $directive = $argv[2] ?? '';

    $request = $channel->receive();
    if (!\is_array($request)
        || 1 !== ($request['id'] ?? null)
        || SqliteWorkerOperationEnum::Init->value !== ($request['op'] ?? null)
        || !\is_array($request['data'] ?? null)
        || !\is_string($request['data']['synchronous'] ?? null)
    ) {
        throw new \RuntimeException('Environment probe expected a validated init request.');
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

    $channel->send([
        'php_binary' => \PHP_BINARY,
        'sapi' => \PHP_SAPI,
        'marker' => getenv($marker),
        'tmpdir' => getenv('TMPDIR'),
        'config' => \ini_get($directive),
        'sqlite3' => \extension_loaded('sqlite3'),
    ]);

    return null;
};
