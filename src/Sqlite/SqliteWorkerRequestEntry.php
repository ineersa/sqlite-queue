<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Amp\Cancellation;
use Amp\DeferredFuture;

/**
 * One admitted parent request.
 *
 * @internal
 */
final class SqliteWorkerRequestEntry
{
    public ?int $id = null;
    public bool $dispatched = false;
    public bool $writeComplete = false;
    public bool $responseComplete = false;
    public bool $removed = false;
    public bool $inFlightCharged = false;
    public bool $admissionHeld = false;
    public bool $callerConsumed = false;
    public ?string $timeoutId = null;
    public ?string $cancellationId = null;
    public ?Cancellation $cancellation = null;
    /** @var array<string, mixed>|null */
    public ?array $data;
    /** @var DeferredFuture<mixed> */
    public readonly DeferredFuture $completion;

    /**
     * @param array<string, mixed> $data
     */
    public function __construct(
        public readonly SqliteWorkerOperationEnum $operation,
        array $data,
        public readonly int $payloadBytes,
        public readonly bool $lifecycle,
        public readonly ?string $expectedQueue,
    ) {
        $this->data = $data;
        $this->completion = new DeferredFuture();
    }
}
