<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Broker;

use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Protocol\ProtocolLimits;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ProtocolLimitsTest extends TestCase
{
    public function testDefaultsPreservePublishedBudgets(): void
    {
        $limits = ProtocolLimits::defaults();
        $this->assertSame(Limits::MAX_PAYLOAD, $limits->maxPayload);
        $this->assertSame(Limits::MAX_CONNECTIONS, $limits->maxConnections);
        $this->assertSame(Limits::MAX_FRAME, $limits->maxFrame);
        $this->assertSame(Limits::MAX_PAYLOAD + ProtocolLimits::RESULT_METADATA_BYTES, $limits->maximumResultBytes());
    }

    public function testRaisedPayloadDerivesFrameBudget(): void
    {
        $limits = new ProtocolLimits(3_000_000, 80);
        $this->assertSame(3_000_000, $limits->maxPayload);
        $this->assertSame(80, $limits->maxConnections);
        $this->assertSame(3_000_000 + Limits::CONTROL_LENGTH_BYTES + Limits::MAX_CONTROL + ProtocolLimits::FRAME_HEADROOM, $limits->maxFrame);
    }

    public static function invalidLimits(): iterable
    {
        yield 'zero payload' => [0, 1, 'Maximum payload must be a positive byte count.'];
        yield 'zero connections' => [1, 0, 'Maximum connections must be a positive integer.'];
        yield 'uint32 overflow' => [ProtocolLimits::MAX_UINT32 - Limits::CONTROL_LENGTH_BYTES - Limits::MAX_CONTROL - ProtocolLimits::FRAME_HEADROOM + 1, 1, 'Maximum payload exceeds the framed uint32 length budget.'];
    }

    #[DataProvider('invalidLimits')]
    public function testInvalidLimitsAreRejected(int $payload, int $connections, string $message): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($message);
        new ProtocolLimits($payload, $connections);
    }
}
