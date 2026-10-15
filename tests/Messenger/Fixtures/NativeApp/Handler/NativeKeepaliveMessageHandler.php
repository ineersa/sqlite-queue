<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Handler;

use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeKeepaliveMessage;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
final class NativeKeepaliveMessageHandler
{
    public function __invoke(NativeKeepaliveMessage $message): void
    {
        fwrite(\STDOUT, "handler-ready\n");
        while (false !== $command = fgets(\STDIN)) {
            if ("release\n" === $command) {
                return;
            }
            if ("keepalive\n" !== $command) {
                throw new \RuntimeException('Unexpected keepalive handler command.');
            }
            // Exercise Symfony's installed SIGALRM handler, not the transport directly.
            posix_kill(getmypid(), \SIGALRM);
            fwrite(\STDOUT, "keepalive-complete\n");
        }
        throw new \RuntimeException('Keepalive handler was not released.');
    }
}
