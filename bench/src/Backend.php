<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

enum Backend: string
{
    case Doctrine = 'doctrine';
    case Broker = 'broker';

    public function table(): string
    {
        return match ($this) {
            self::Doctrine => Config::MESSENGER_TABLE,
            self::Broker => 'queue_messages',
        };
    }
}
