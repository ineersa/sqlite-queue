<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

/** Internal worker channel operations. Not part of the public wire protocol. */
enum SqliteWorkerOperationEnum: string
{
    case Init = 'init';
    case Send = 'send';
    case Claim = 'claim';
    case Settle = 'settle';
    case EarliestEligibility = 'earliest_eligibility';
    case Close = 'close';
}
