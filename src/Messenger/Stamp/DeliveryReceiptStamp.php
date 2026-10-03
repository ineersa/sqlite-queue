<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger\Stamp;

use Ineersa\SqliteQueue\Messenger\Transport;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * Delivery receipt bound to the Transport instance that claimed it.
 *
 * Non-sendable so a retry or republish cannot carry a foreign reservation.
 */
final class DeliveryReceiptStamp implements NonSendableStampInterface
{
    public function __construct(
        public readonly string $receipt,
        public readonly Transport $transport,
    ) {
    }
}
