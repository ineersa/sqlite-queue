<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\Sync\Channel;

/*
 * Reports what the persistence child received from the broker process.
 *
 * The first argument names the environment variable the caller set, and the second names an ini
 * directive the caller configured, so both assertions stay tied to the caller's own values. The
 * child exits immediately: SqliteWorkerContextFactoryTest joins it to prove a clean exit.
 */
return static function (Channel $channel) use ($argv): null {
    $marker = $argv[1] ?? '';
    $directive = $argv[2] ?? '';

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
