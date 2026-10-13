<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Sqlite;

use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Sqlite\QueuePipeline;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class QueuePipelineTest extends TestCase
{
    public function testDepthOneAndFourUseIdenticalByteAndAdmissionBudgets(): void
    {
        $one = QueuePipeline::limits(1);
        $four = QueuePipeline::limits(4);
        $this->assertSame(1, $one->maximumInFlightOperations);
        $this->assertSame(4, $four->maximumInFlightOperations);
        $this->assertSame($one->maximumInFlightBytes, $four->maximumInFlightBytes);
        $this->assertSame($one->maximumAdmittedBytes, $four->maximumAdmittedBytes);
        $this->assertSame(128, $four->maximumAdmittedOperations);
        $this->assertSame($one->maximumErrorReplyBytes, $four->maximumErrorReplyBytes);
    }

    public function testAdmissionReservesEveryPublicConnectionAndReadinessSlot(): void
    {
        $this->assertGreaterThan(0, QueuePipeline::ELIGIBILITY_SLOTS);
        $this->assertSame(QueuePipeline::ADMITTED_OPERATIONS, Limits::MAX_CONNECTIONS + QueuePipeline::ELIGIBILITY_SLOTS);
        $this->assertSame(Limits::MAX_CONNECTIONS, QueuePipeline::ADMITTED_PAYLOAD_WINDOWS);
        $this->assertSame(Limits::MAX_CONNECTIONS * Limits::MAX_PAYLOAD + QueuePipeline::ADMITTED_OPERATIONS * QueuePipeline::SLOT_METADATA_BYTES, QueuePipeline::ADMITTED_BYTES);
    }

    public static function invalidDepths(): iterable
    {
        yield 'zero' => [0, 'Storage pipeline depth must be positive.'];
        yield 'negative' => [-1, 'Storage pipeline depth must be positive.'];
        yield 'too large' => [5, 'Storage pipeline depth must not exceed 4.'];
    }

    #[DataProvider('invalidDepths')]
    public function testInvalidDepthFailsBeforeAcquisition(int $depth, string $message): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($message);
        QueuePipeline::limits($depth);
    }
}
