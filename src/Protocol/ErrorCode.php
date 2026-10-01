<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Ineersa\SqliteQueue\Exception\ExpiredReceiptException;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Exception\MalformedReceiptException;
use Ineersa\SqliteQueue\Exception\NoActiveReservationException;
use Ineersa\SqliteQueue\Exception\ReceiptEpochMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptOwnerMismatchException;
use Ineersa\SqliteQueue\Exception\ReceiptTokenMismatchException;

enum ErrorCode: string
{
    case FrameTooLarge = 'frame_too_large';
    case InvalidRequest = 'invalid_request';
    case UnsupportedProtocolVersion = 'unsupported_protocol_version';
    case MalformedReceipt = 'malformed_receipt';
    case NoActiveReservation = 'no_active_reservation';
    case ReceiptOwnerMismatch = 'receipt_owner_mismatch';
    case ReceiptEpochMismatch = 'receipt_epoch_mismatch';
    case ReceiptTokenMismatch = 'receipt_token_mismatch';
    case ExpiredReceipt = 'expired_receipt';
    case InvalidQueueName = 'invalid_queue_name';
    case BrokerShuttingDown = 'broker_shutting_down';

    public static function fromReceiptException(InvalidReceiptException $error): self
    {
        return match (true) {
            $error instanceof MalformedReceiptException => self::MalformedReceipt,
            $error instanceof NoActiveReservationException => self::NoActiveReservation,
            $error instanceof ReceiptOwnerMismatchException => self::ReceiptOwnerMismatch,
            $error instanceof ReceiptEpochMismatchException => self::ReceiptEpochMismatch,
            $error instanceof ReceiptTokenMismatchException => self::ReceiptTokenMismatch,
            $error instanceof ExpiredReceiptException => self::ExpiredReceipt,
            default => throw new \LogicException('Receipt exception has no wire error code.'),
        };
    }

    /** Null means this wire error is not a receipt rejection. */
    public function receiptException(): ?InvalidReceiptException
    {
        return match ($this) {
            self::MalformedReceipt => new MalformedReceiptException(),
            self::NoActiveReservation => new NoActiveReservationException(),
            self::ReceiptOwnerMismatch => new ReceiptOwnerMismatchException(),
            self::ReceiptEpochMismatch => new ReceiptEpochMismatchException(),
            self::ReceiptTokenMismatch => new ReceiptTokenMismatchException(),
            self::ExpiredReceipt => new ExpiredReceiptException(),
            default => null,
        };
    }
}
