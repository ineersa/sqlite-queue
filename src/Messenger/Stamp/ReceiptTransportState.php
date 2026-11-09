<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger\Stamp;

/** A serialized receipt has no authority to settle or renew its parent's reservation. */
enum ReceiptTransportState: string
{
    case Detached = 'detached';
}
