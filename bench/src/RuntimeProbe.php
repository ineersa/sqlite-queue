<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Amp\Sync\Channel;

// Runs through Amp's unmodified process context, exactly as the vendor SQLite worker does.
return static fn (Channel $channel): array => RuntimeProfile::current() + ['inheritance_probe_pid' => getmypid()];
