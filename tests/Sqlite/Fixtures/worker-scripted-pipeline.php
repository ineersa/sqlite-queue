<?php

declare(strict_types=1);

use Amp\ByteStream\StreamChannel;
use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerOperationEnum;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerStatusEnum;

use function Amp\Socket\connect;

/*
 * Scripted peer for parent pipelining proofs.
 *
 * Holds the first two replies until the parent releases a separate control socket.
 * A Close received first is acknowledged on that socket but never answered on IPC.
 */
return static function (Channel $channel) use ($argv): void {
    $socket = connect($argv[1]);
    $control = new StreamChannel($socket, $socket);
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
    if (SqliteWorkerOperationEnum::Close->value === ($first['op'] ?? null)) {
        $control->send([$first]);
        $control->receive();

        throw new RuntimeException('A withheld Close must end through worker termination.');
    }
    $second = $channel->receive();
    if (!is_array($second) || 3 !== ($second['id'] ?? null)) {
        throw new RuntimeException('Scripted peer expected second request id 3 before replying.');
    }
    $control->send([$first, $second]);
    if (true !== $control->receive()) {
        throw new RuntimeException('Scripted peer expected an explicit reply release.');
    }

    foreach ([$first, $second] as $request) {
        $channel->send([
            'id' => $request['id'],
            'op' => $request['op'],
            'status' => SqliteWorkerStatusEnum::Ok->value,
            'result' => match ($request['op']) {
                SqliteWorkerOperationEnum::Send->value => (int) $request['id'] - 1,
                SqliteWorkerOperationEnum::Claim->value,
                SqliteWorkerOperationEnum::EarliestEligibility->value => null,
                SqliteWorkerOperationEnum::Settle->value,
                SqliteWorkerOperationEnum::Close->value => true,
                default => throw new RuntimeException('Unsupported scripted operation.'),
            },
        ]);
    }

    $nextId = 4;
    while (true) {
        $request = $channel->receive();
        if (!is_array($request) || $nextId++ !== ($request['id'] ?? null)) {
            throw new RuntimeException('Scripted peer expected contiguous request IDs.');
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
                SqliteWorkerOperationEnum::Claim->value,
                SqliteWorkerOperationEnum::EarliestEligibility->value => null,
                SqliteWorkerOperationEnum::Settle->value => true,
                default => throw new RuntimeException('Unsupported scripted operation.'),
            },
        ]);
    }
};
