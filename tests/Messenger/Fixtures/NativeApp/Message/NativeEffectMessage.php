<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message;

final readonly class NativeEffectMessage
{
    public function __construct(public string $body)
    {
    }
}
