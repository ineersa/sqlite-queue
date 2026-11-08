<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger\Stamp;

use Ineersa\SqliteQueue\Messenger\Transport;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Delivery receipt bound to the Transport instance that claimed it.
 *
 * Non-sendable so a retry or republish cannot carry a foreign reservation.
 * Process serialization detaches the copy; only the original parent stamp can
 * settle or renew the reservation. Messenger restores that original on completion.
 */
final class DeliveryReceiptStamp implements NonSendableStampInterface
{
    public readonly Transport|ReceiptTransportState $transport;

    public function __construct(
        public readonly string $receipt,
        Transport $transport,
    ) {
        $this->transport = $transport;
    }

    /** @return array{receipt: string} */
    public function __serialize(): array
    {
        return ['receipt' => $this->receipt];
    }

    /** @param array<string, mixed> $data */
    public function __unserialize(array $data): void
    {
        $receipt = $data['receipt'] ?? null;
        if (!\is_string($receipt)) {
            throw new \UnexpectedValueException('Serialized delivery receipt must be a string.');
        }

        $this->receipt = $receipt;
        $this->transport = ReceiptTransportState::Detached;
    }
}
