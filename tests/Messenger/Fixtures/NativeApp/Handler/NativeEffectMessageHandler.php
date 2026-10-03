<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Handler;

use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeEffectMessage;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
final readonly class NativeEffectMessageHandler
{
    public function __construct(private string $recordPath)
    {
    }

    public function __invoke(NativeEffectMessage $message): void
    {
        if (false === file_put_contents($this->recordPath, $message->body."\n", \FILE_APPEND)) {
            throw new \RuntimeException('Could not record handler effect.');
        }
        fwrite(\STDOUT, "effect-recorded\n");
        fflush(\STDOUT);
        if ("release\n" !== fgets(\STDIN)) {
            throw new \RuntimeException('Handler barrier was not released.');
        }
    }
}
