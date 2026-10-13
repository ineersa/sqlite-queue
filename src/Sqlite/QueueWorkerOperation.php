<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Sqlite;

use Fabpot\Amp\Sqlite\SqliteBlob;
use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\Operation;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use Fabpot\Amp\Sqlite\Worker\SqlExecutor;
use Ineersa\SqliteQueue\Protocol\Limits;
use Symfony\Component\Clock\NativeClock;

class QueueWorkerOperation implements Operation
{
    public function __construct(private readonly SqlExecutor $sql, private readonly QueueClock $clock)
    {
    }

    public static function create(SqlExecutor $sql): self
    {
        return new self($sql, new NativeQueueClock(new NativeClock()));
    }

    public function run(array $parameters): OperationResultDTO
    {
        $operation = QueueStorageOperation::from(self::string($parameters, 'operation'));

        return match ($operation) {
            QueueStorageOperation::Insert => $this->insert(self::string($parameters, 'queue'), self::string($parameters, 'body'), self::string($parameters, 'headers'), self::integer($parameters, 'delay')),
            QueueStorageOperation::Claim => $this->claim(self::string($parameters, 'queue'), self::string($parameters, 'owner'), self::string($parameters, 'epoch'), self::string($parameters, 'token'), self::integer($parameters, 'visibility')),
            QueueStorageOperation::Settle => new OperationResultDTO(Outcome::Commit, ['settlement' => $this->settle(self::integer($parameters, 'id'), self::string($parameters, 'token'), self::string($parameters, 'owner'), self::string($parameters, 'epoch'))->value]),
            QueueStorageOperation::Eligibility => new OperationResultDTO(Outcome::ReadOnly, ['ready_at' => $this->eligibility(self::string($parameters, 'queue'))]),
        };
    }

    /** @param array<array-key, mixed> $parameters */
    private static function string(array $parameters, string $key): string
    {
        $value = $parameters[$key] ?? null;
        if (!\is_string($value)) {
            throw new \InvalidArgumentException('Worker parameter '.$key.' must be a string.');
        }

        return $value;
    }

    /** @param array<array-key, mixed> $parameters */
    private static function integer(array $parameters, string $key): int
    {
        $value = $parameters[$key] ?? null;
        if (!\is_int($value)) {
            throw new \InvalidArgumentException('Worker parameter '.$key.' must be an integer.');
        }

        return $value;
    }

    private function insert(string $queue, string $body, string $headers, int $delay): OperationResultDTO
    {
        $now = DeadlinePolicy::now($this->clock);
        try {
            $deadline = DeadlinePolicy::deadline($now, $delay);
        } catch (\InvalidArgumentException) {
            $error = $delay < 0 ? QueueValidationError::NegativeDelay : QueueValidationError::DeadlineOverflow;

            return new OperationResultDTO(Outcome::Rollback, ['validation_error' => $error->value]);
        }
        $result = $this->sql->execute(QueueSql::INSERT_SQL, [$queue, new SqliteBlob($body), new SqliteBlob($headers), $deadline]);
        $id = $result['last_insert_id'];
        if (null === $id) {
            throw new \RuntimeException('Insert did not return a message identity.');
        }
        if ($id <= 0) {
            throw new \RuntimeException('Insert did not return a positive message identity.');
        }

        return new OperationResultDTO(Outcome::Commit, ['id' => $id]);
    }

    private function claim(string $queue, string $ownerId, string $epoch, string $token, int $visibility): OperationResultDTO
    {
        $now = DeadlinePolicy::now($this->clock);
        $expires = DeadlinePolicy::deadline($now, $visibility);
        $result = $this->sql->execute(QueueSql::ELIGIBLE_SQL, [$queue, $now, $now]);
        $row = $result['rows'][0] ?? null;
        if (null === $row) {
            return new OperationResultDTO(Outcome::Rollback, ['message' => null]);
        }
        $body = self::payloadBytes($row['body'] ?? null);
        $headers = self::payloadBytes($row['headers'] ?? null);
        if (\strlen($body) + \strlen($headers) > Limits::MAX_PAYLOAD) {
            throw new \RuntimeException('Stored message payload exceeds the supported size.');
        }
        if (!is_numeric($row['id'] ?? null)) {
            throw new \RuntimeException('Stored message identity must be an integer.');
        }
        $id = (int) $row['id'];
        $result = $this->sql->execute(QueueSql::RESERVE_SQL, [$expires, $token, $ownerId, $epoch, $id, $queue, $now, $now]);
        $changed = $result['row_count'];
        if (0 === $changed) {
            return new OperationResultDTO(Outcome::Rollback, ['message' => null]);
        }
        if (1 !== $changed) {
            throw new \RuntimeException('Reservation UPDATE did not affect exactly one row.');
        }
        if (!is_numeric($row['available_at'] ?? null)) {
            throw new \RuntimeException('Stored availability deadline must be an integer.');
        }
        $availableAt = (int) $row['available_at'];
        if ($availableAt < 0) {
            throw new \RuntimeException('Stored availability deadline must be nonnegative.');
        }

        return new OperationResultDTO(Outcome::Commit, ['message' => ['id' => $id, 'body' => $body, 'headers' => $headers, 'available_at' => $availableAt, 'reserved_until' => $expires]]);
    }

    private function settle(int $id, string $token, string $ownerId, string $epoch): SettlementResultEnum
    {
        $now = DeadlinePolicy::now($this->clock);
        $result = $this->sql->execute(QueueSql::DELETE_SQL, [$id, $token, $ownerId, $epoch, $now]);
        $changed = $result['row_count'];
        if (0 === $changed) {
            return $this->settlementFailure($id, $token, $ownerId, $epoch, $now);
        }
        if (1 !== $changed) {
            throw new \RuntimeException('Settlement DELETE did not affect exactly one reservation.');
        }

        return SettlementResultEnum::Settled;
    }

    private function settlementFailure(int $id, string $token, string $ownerId, string $epoch, int $now): SettlementResultEnum
    {
        $result = $this->sql->execute(QueueSql::DIAGNOSE_SQL, [$id]);
        $row = $result['rows'][0] ?? null;
        if (null === $row || null === $row['reserved_until']) {
            return SettlementResultEnum::NoActiveReservation;
        }
        if ('text' !== ($row['owner_id_type'] ?? null) || !\is_string($row['owner_id'])) {
            throw new \RuntimeException('Stored reservation owner must be a string.');
        }
        if ('text' !== ($row['broker_epoch_type'] ?? null) || !\is_string($row['broker_epoch'])) {
            throw new \RuntimeException('Stored reservation epoch must be a string.');
        }
        if ('text' !== ($row['reservation_token_type'] ?? null) || !\is_string($row['reservation_token'])) {
            throw new \RuntimeException('Stored reservation token must be a string.');
        }
        if ('integer' !== ($row['reserved_until_type'] ?? null) || !is_numeric($row['reserved_until'])) {
            throw new \RuntimeException('Stored reservation expiry must be an integer.');
        }
        if ($ownerId !== $row['owner_id']) {
            return SettlementResultEnum::OwnerMismatch;
        }
        if ($epoch !== $row['broker_epoch']) {
            return SettlementResultEnum::EpochMismatch;
        }
        if ($token !== $row['reservation_token']) {
            return SettlementResultEnum::TokenMismatch;
        }
        if ((int) $row['reserved_until'] <= $now) {
            return SettlementResultEnum::Expired;
        }

        throw new \RuntimeException('Settlement DELETE failed despite a matching active reservation.');
    }

    private static function payloadBytes(mixed $value): string
    {
        if ($value instanceof SqliteBlob) {
            return $value->getBytes();
        }
        if (\is_string($value)) {
            return $value;
        }

        throw new \RuntimeException('Stored message is missing or has invalid payload types.');
    }

    private function eligibility(string $queue): ?int
    {
        $result = $this->sql->execute(QueueSql::EARLIEST_SQL, [$queue]);
        $row = $result['rows'][0] ?? null;
        if (null === $row) {
            return null;
        }
        if (!is_numeric($row['ready_at'] ?? null)) {
            throw new \RuntimeException('Stored eligibility deadline must be an integer.');
        }

        return (int) $row['ready_at'];
    }
}
