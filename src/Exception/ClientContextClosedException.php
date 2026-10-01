<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Exception;

/** The caller cancelled its client lifetime, independently of receipt validity. */
final class ClientContextClosedException extends \RuntimeException
{
}
