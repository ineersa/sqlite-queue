<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Handler;

use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeBatchMessage;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;
use Symfony\Component\Messenger\Handler\Acknowledger;
use Symfony\Component\Messenger\Handler\BatchHandlerInterface;
use Symfony\Component\Messenger\Handler\BatchHandlerTrait;

#[AsMessageHandler]
final class NativeBatchMessageHandler implements BatchHandlerInterface
{
    use BatchHandlerTrait;

    public function __construct(private readonly string $recordPath)
    {
    }

    /** Null acknowledger is Messenger's synchronous dispatch contract. */
    public function __invoke(NativeBatchMessage $message, ?Acknowledger $ack = null): mixed
    {
        return $this->handle($message, $ack);
    }

    public static function supportsDeferredIdleFlush(): bool
    {
        // Messenger 8.0 flushes every idle batch; 8.1 retains batches until their idle deadline.
        return method_exists(BatchHandlerTrait::class, 'getIdleTimeout');
    }

    /** Null disables idle flushing so force-flush on shutdown is the only completion barrier. */
    private function getIdleTimeout(): ?int
    {
        return null;
    }

    /** @param list<array{NativeBatchMessage, Acknowledger}> $jobs */
    private function process(array $jobs): void
    {
        foreach ($jobs as [$message, $ack]) {
            if (false === file_put_contents($this->recordPath, json_encode(['body' => $message->body], \JSON_THROW_ON_ERROR)."\n", \FILE_APPEND)) {
                throw new \RuntimeException('Could not record the batch handler effect.');
            }
            $ack->ack();
        }
    }
}
