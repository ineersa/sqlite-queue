<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerOperationEnum;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerStatusEnum;

/*
 * Reports what the persistence child received from the broker process.
 *
 * Writes environment evidence before init completes, then answers Close through
 * the normal protocol so the proxy remains the sole response reader.
 */
return static function (Channel $channel) use ($argv): null {
    $marker = $argv[1] ?? '';
    $directive = $argv[2] ?? '';
    $reportPath = $argv[3] ?? throw new \RuntimeException('Environment probe requires a report path.');

    $request = $channel->receive();
    if (!\is_array($request)
        || 1 !== ($request['id'] ?? null)
        || SqliteWorkerOperationEnum::Init->value !== ($request['op'] ?? null)
        || !\is_array($request['data'] ?? null)
        || !\is_string($request['data']['synchronous'] ?? null)
    ) {
        throw new \RuntimeException('Environment probe expected a validated init request.');
    }

    $report = [
        'php_binary' => \PHP_BINARY,
        'sapi' => \PHP_SAPI,
        'marker' => getenv($marker),
        'tmpdir' => getenv('TMPDIR'),
        'config' => \ini_get($directive),
        'sqlite3' => \extension_loaded('sqlite3'),
    ];
    if (false === file_put_contents($reportPath, json_encode($report, \JSON_THROW_ON_ERROR))) {
        throw new \RuntimeException('Environment probe could not write its report.');
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

    $close = $channel->receive();
    if (!\is_array($close) || SqliteWorkerOperationEnum::Close->value !== ($close['op'] ?? null)) {
        throw new \RuntimeException('Environment probe expected Close.');
    }
    $channel->send([
        'id' => $close['id'],
        'op' => SqliteWorkerOperationEnum::Close->value,
        'status' => SqliteWorkerStatusEnum::Ok->value,
        'result' => true,
    ]);

    return null;
};
