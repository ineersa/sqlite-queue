<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Fabpot\Amp\Sqlite\Worker\DTO\PipelineLimitsDTO;
use Ineersa\SqliteQueue\Protocol\Limits;

final class QueuePipeline
{
    public const int DEFAULT_DEPTH = 4;
    public const int MAXIMUM_DEPTH = 4;
    public const int ADMITTED_OPERATIONS = 128;
    /** Keep readiness fanout from consuming the slots reserved for public mutations. */
    public const int ELIGIBILITY_SLOTS = self::ADMITTED_OPERATIONS - Limits::MAX_CONNECTIONS;
    public const int IN_FLIGHT_PAYLOAD_WINDOWS = 2;
    /** One maximum payload per public connection; readiness probes carry no payload. */
    public const int ADMITTED_PAYLOAD_WINDOWS = Limits::MAX_CONNECTIONS;
    public const int SCALAR_RESULT_BYTES = 1_024;
    /** Reserve metadata for every slot, even when its operation has no payload.
     * Separate request/result reserves cover field names, identities, serialization and errors.
     * Each operation carries at most one maximum payload, in either request or reply.
     */
    public const int SLOT_METADATA_BYTES = 2 * QueueWorkerProtocol::RESULT_METADATA_BYTES + 2 * PipelineLimitsDTO::CHANNEL_OVERHEAD_BYTES;
    public const int IN_FLIGHT_BYTES = self::IN_FLIGHT_PAYLOAD_WINDOWS * Limits::MAX_PAYLOAD + self::MAXIMUM_DEPTH * self::SLOT_METADATA_BYTES;
    public const int ADMITTED_BYTES = self::ADMITTED_PAYLOAD_WINDOWS * Limits::MAX_PAYLOAD + self::ADMITTED_OPERATIONS * self::SLOT_METADATA_BYTES;

    public static function validateDepth(int $depth): void
    {
        if ($depth < 1) {
            throw new \InvalidArgumentException('Storage pipeline depth must be positive.');
        }
        if ($depth > self::MAXIMUM_DEPTH) {
            throw new \InvalidArgumentException('Storage pipeline depth must not exceed '.self::MAXIMUM_DEPTH.'.');
        }
    }

    public static function limits(int $depth): PipelineLimitsDTO
    {
        self::validateDepth($depth);

        return new PipelineLimitsDTO($depth, self::IN_FLIGHT_BYTES, self::ADMITTED_OPERATIONS, self::ADMITTED_BYTES, PipelineLimitsDTO::DEFAULT_ERROR_REPLY_BYTES);
    }
}
