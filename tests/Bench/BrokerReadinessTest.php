<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\BrokerReadiness;
use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Ineersa\SqliteQueue\Bench\Scenario;
use Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BrokerReadinessTest extends TestCase
{
    public function testRequestedDepthOneRejectsActualDepthFour(): void
    {
        $options = new RunOptionsDTO(Scenario::Concurrent, true, 1.0, SqliteSynchronousMode::Normal, pipelineDepth: 1);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Broker storage pipeline depth mismatch: requested 1, observed 4.');
        BrokerReadiness::validate(['storage_pipeline_depth' => 4, 'synchronous_effective' => 'normal'], $options);
    }

    public static function malformedDepths(): iterable
    {
        yield 'missing' => [[]];
        yield 'string' => [['storage_pipeline_depth' => '1']];
        yield 'null' => [['storage_pipeline_depth' => null]];
    }

    #[DataProvider('malformedDepths')]
    public function testReadinessRequiresObservedIntegerDepth(array $ready): void
    {
        $options = new RunOptionsDTO(Scenario::Concurrent, true, 1.0, SqliteSynchronousMode::Normal, pipelineDepth: 1);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Broker readiness must contain an integer storage pipeline depth.');
        BrokerReadiness::validate($ready, $options);
    }

    public function testAllWorkloadsValidateObservedDepthAndDurability(): void
    {
        foreach (Scenario::cases() as $scenario) {
            foreach ([1, 4] as $depth) {
                $options = new RunOptionsDTO($scenario, true, 1.0, SqliteSynchronousMode::Normal, pipelineDepth: $depth);
                BrokerReadiness::validate(['storage_pipeline_depth' => $depth, 'synchronous_effective' => 'normal'], $options);
                $this->addToAssertionCount(1);
            }
        }
    }

    public function testDepthMatchDoesNotHideDurabilityMismatch(): void
    {
        $options = new RunOptionsDTO(Scenario::Concurrent, true, 1.0, SqliteSynchronousMode::Full, pipelineDepth: 1);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Broker owning connection synchronous readback does not match the selected mode.');
        BrokerReadiness::validate(['storage_pipeline_depth' => 1, 'synchronous_effective' => 'normal'], $options);
    }
}
