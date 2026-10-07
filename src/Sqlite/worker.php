<?php

declare(strict_types=1);

use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerRuntime;

return static function (Channel $channel): void {
    SqliteWorkerRuntime::run(
        $channel,
        static fn (): int => (int) floor(microtime(true) * 1000),
    );
};
