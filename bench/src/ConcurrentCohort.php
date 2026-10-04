<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class ConcurrentCohort
{
    public const PUBLISHERS = 3;
    public const CONSUMERS = 2;
    public const MESSAGES_PER_PUBLISHER = 1000;
    public const SMOKE_PER_PUBLISHER = 4;
    public const COHORT_TIMEOUT_SECONDS = 180;
    public const RUN_TIMEOUT_SECONDS = 240;
    public const WARMUP_PER_PUBLISHER = 2;
    public const PUBLISHER_ACTORS = ['publisher-0', 'publisher-1', 'publisher-2'];
    public const CONSUMER_ACTORS = ['consumer-0', 'consumer-1'];

    public static function identity(string $actor, Phase $phase, int $index): string
    {
        if (!\in_array($actor, self::PUBLISHER_ACTORS, true)) {
            throw new \InvalidArgumentException('Unknown concurrent publisher.');
        }
        if ($index < 0 || $index >= self::MESSAGES_PER_PUBLISHER) {
            throw new \InvalidArgumentException('Concurrent message index is outside declared cohort.');
        }
        if (!\in_array($phase, [Phase::Warmup, Phase::Measure], true)) {
            throw new \InvalidArgumentException('Concurrent message phase must be warmup or measure.');
        }

        return $phase->value.':'.$actor.':'.$index;
    }

    /** @return \Generator<int, string> */
    public static function ids(Phase $phase, int $count): \Generator
    {
        if ($count < 1 || $count > self::MESSAGES_PER_PUBLISHER) {
            throw new \InvalidArgumentException('Concurrent per-publisher count must be within 1..1000.');
        }
        foreach (self::PUBLISHER_ACTORS as $actor) {
            for ($index = 0; $index < $count; ++$index) {
                yield self::identity($actor, $phase, $index);
            }
        }
    }
}
