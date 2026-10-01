<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\ValueObject;

/** Validated logical queue identity. Construct from untrusted strings at the API boundary. */
final readonly class QueueName
{
    /** Queue names hold 1 to 255 ASCII letters, digits, dots, underscores, or hyphens, starting with a letter or digit. */
    public const string PATTERN = '/\A[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}\z/D';

    public function __construct(public string $value)
    {
        if (1 !== preg_match(self::PATTERN, $value)) {
            throw new \InvalidArgumentException('Queue names must contain 1 to 255 ASCII letters, digits, dots, underscores, or hyphens and start with a letter or digit.');
        }
    }
}
