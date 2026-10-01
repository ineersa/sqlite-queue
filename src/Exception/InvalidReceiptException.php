<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

/** Base for specific, recoverable receipt rejections. */
abstract class InvalidReceiptException extends \RuntimeException
{
}
