<?php

declare(strict_types=1);

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerOperationEnum;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerStatusEnum;

/*
 * Scripted peer for parent pipelining proofs.
 *
 * Completes initialization, then withholds reply A until request B arrives.
 */
return static function (Channel $channel): void {
    $init = $channel->receive();
    if (!is_array($init) || 1 !== ($init['id'] ?? null)) {
        throw new RuntimeException('Scripted peer expected init id 1.');
    }
    $channel->send([
        'id' => 1,
        'op' => SqliteWorkerOperationEnum::Init->value,
        'status' => SqliteWorkerStatusEnum::Ok->value,
        'result' => [
            'journal_mode' => 'wal',
            'synchronous' => SqliteSynchronousMode::Normal->value,
            'busy_timeout' => 5_000,
            'wal_autocheckpoint' => 1_000,
            'sqlite_version' => 'scripted',
        ],
    ]);

    $first = $channel->receive();
    if (!is_array($first) || 2 !== ($first['id'] ?? null)) {
        throw new RuntimeException('Scripted peer expected first request id 2.');
    }
    $second = $channel->receive();
    if (!is_array($second) || 3 !== ($second['id'] ?? null)) {
        throw new RuntimeException('Scripted peer expected second request id 3 before replying.');
    }

    foreach ([$first, $second] as $request) {
        $channel->send([
            'id' => $request['id'],
            'op' => $request['op'],
            'status' => SqliteWorkerStatusEnum::Ok->value,
            'result' => match ($request['op']) {
                SqliteWorkerOperationEnum::Send->value => (int) $request['id'] - 1,
                SqliteWorkerOperationEnum::EarliestEligibility->value => null,
                SqliteWorkerOperationEnum::Settle->value,
                SqliteWorkerOperationEnum::Close->value => true,
                default => throw new RuntimeException('Unsupported scripted operation.'),
            },
        ]);
    }

    while (true) {
        $request = $channel->receive();
        if (!is_array($request)) {
            throw new RuntimeException('Scripted peer expected array requests.');
        }
        if (SqliteWorkerOperationEnum::Close->value === ($request['op'] ?? null)) {
            $channel->send([
                'id' => $request['id'],
                'op' => SqliteWorkerOperationEnum::Close->value,
                'status' => SqliteWorkerStatusEnum::Ok->value,
                'result' => true,
            ]);

            return;
        }
        $channel->send([
            'id' => $request['id'],
            'op' => $request['op'],
            'status' => SqliteWorkerStatusEnum::Ok->value,
            'result' => match ($request['op']) {
                SqliteWorkerOperationEnum::Send->value => (int) $request['id'] - 1,
                SqliteWorkerOperationEnum::EarliestEligibility->value => null,
                SqliteWorkerOperationEnum::Settle->value => true,
                default => throw new RuntimeException('Unsupported scripted operation.'),
            },
        ]);
    }
};
