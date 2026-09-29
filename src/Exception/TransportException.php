<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

/** No confirmation was obtained. The operation may have committed; never replay it implicitly. */
final class TransportException extends \RuntimeException
{
}
