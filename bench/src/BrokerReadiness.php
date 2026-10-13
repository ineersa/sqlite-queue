<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;

/** Verify observed broker settings before releasing any workload. */
final class BrokerReadiness
{
    /** @param array<array-key, mixed> $ready */
    public static function validate(array $ready, RunOptionsDTO $options): void
    {
        $depth = $ready['storage_pipeline_depth'] ?? null;
        if (!\is_int($depth)) {
            throw new \RuntimeException('Broker readiness must contain an integer storage pipeline depth.');
        }
        if ($depth !== $options->pipelineDepth) {
            throw new \RuntimeException(\sprintf('Broker storage pipeline depth mismatch: requested %d, observed %d.', $options->pipelineDepth, $depth));
        }
        if (($ready['synchronous_effective'] ?? null) !== $options->synchronous->value) {
            throw new \RuntimeException('Broker owning connection synchronous readback does not match the selected mode.');
        }
    }
}
