<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

/** The receipt is expired, unknown, or belongs to another session or engine epoch. */
final class InvalidReceipt extends \RuntimeException
{
}
