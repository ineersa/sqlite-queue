<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

enum Operation: string
{
    case Hello = 'hello';
    case Send = 'send';
    case Receive = 'receive';
    case Acknowledge = 'acknowledge';
    case Reject = 'reject';
    case Wait = 'wait';

    /**
     * Operation-specific request fields beyond the common framing keys.
     *
     * @return list<string>
     */
    public function allowedFields(): array
    {
        return match ($this) {
            self::Hello => [],
            self::Send => [ControlField::Queue->value, ControlField::Delay->value],
            self::Receive => [ControlField::Queue->value],
            self::Acknowledge, self::Reject => [ControlField::Receipt->value],
            self::Wait => [ControlField::Queue->value, ControlField::WaitMilliseconds->value],
        };
    }
}
