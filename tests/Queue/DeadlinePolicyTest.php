<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Queue;

use Ineersa\SqliteQueue\Sqlite\DeadlinePolicy;
use Ineersa\SqliteQueue\Sqlite\QueueClock;
use PHPUnit\Framework\TestCase;

final class DeadlinePolicyTest extends TestCase
{
    public function testZeroAndMaximumDeadlinesAreRepresentable(): void
    {
        $this->assertSame(0, DeadlinePolicy::deadline(0, 0));
        $this->assertSame(\PHP_INT_MAX, DeadlinePolicy::deadline(\PHP_INT_MAX - 1, 1));
    }

    public function testNegativeClockIsRejected(): void
    {
        $clock = new class implements QueueClock {
            public function now(): int
            {
                return -1;
            }
        };
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Wall-clock milliseconds must be nonnegative.');
        DeadlinePolicy::now($clock);
    }

    public function testNegativeDurationIsRejected(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration must be nonnegative milliseconds.');
        DeadlinePolicy::deadline(0, -1);
    }

    public function testOverflowIsRejectedBeforeArithmetic(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Deadline exceeds the supported timestamp range.');
        DeadlinePolicy::deadline(\PHP_INT_MAX, 1);
    }
}
