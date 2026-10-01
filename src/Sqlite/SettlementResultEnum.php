<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Result of a fenced DELETE, diagnosed under the same SQLite write transaction. */
enum SettlementResultEnum: string
{
    case Settled = 'settled';
    case NoActiveReservation = 'no_active_reservation';
    case OwnerMismatch = 'owner_mismatch';
    case EpochMismatch = 'epoch_mismatch';
    case TokenMismatch = 'token_mismatch';
    case Expired = 'expired';
}
