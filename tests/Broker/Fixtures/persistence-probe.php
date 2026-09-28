<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker\Fixtures;

use Amp\Sync\Channel;

/*
 * Minimal worker for PersistenceFactoryTest.
 *
 * Reports readiness over the parent channel and then blocks until the parent kills it.
 */
return static function (Channel $channel): null {
    $channel->send('ready');

    while (null !== $channel->receive()) {
    }

    return null;
};
