<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message;

final class NativeProbeMessage
{
    public function __construct(public readonly string $body)
    {
    }
}
