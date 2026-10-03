<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class Schedule
{
    /** @return list<Backend> */
    public static function order(int $repetition): array
    {
        if ($repetition < 0) {
            throw new \InvalidArgumentException('Repetition must be nonnegative.');
        }

        return 0 === $repetition % 2 ? [Backend::Doctrine, Backend::Broker] : [Backend::Broker, Backend::Doctrine];
    }
}
