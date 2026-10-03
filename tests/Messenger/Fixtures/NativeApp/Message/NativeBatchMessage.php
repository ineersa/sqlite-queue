<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message;

final readonly class NativeBatchMessage
{
    public function __construct(public string $body)
    {
    }
}
