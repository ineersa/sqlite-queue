<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message;

final readonly class NativeParallelMessage
{
    public const string RELEASE = "release\n";

    public function __construct(
        public string $body,
        public string $barrierEndpoint,
        public NativeParallelOutcome $outcome,
    ) {
    }
}
