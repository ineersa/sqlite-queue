<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Handler;

use Amp\ByteStream\BufferedReader;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeParallelMessage;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeParallelOutcome;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

use function Amp\Socket\connect;

#[AsMessageHandler]
final class NativeParallelMessageHandler
{
    public function __invoke(NativeParallelMessage $message): void
    {
        $socket = connect($message->barrierEndpoint);
        try {
            $socket->write(json_encode(['body' => $message->body, 'pid' => getmypid()], \JSON_THROW_ON_ERROR)."\n");
            if (rtrim(NativeParallelMessage::RELEASE) !== (new BufferedReader($socket))->readUntil("\n")) {
                throw new \RuntimeException('Parallel handler barrier closed without release.');
            }
            if (NativeParallelOutcome::Failure === $message->outcome) {
                throw new \RuntimeException('Parallel handler deliberately failed: '.$message->body);
            }
        } finally {
            $socket->close();
        }
    }
}
