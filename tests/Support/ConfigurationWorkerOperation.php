<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\Operation;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use Fabpot\Amp\Sqlite\Worker\SqlExecutor;

/** Readback stays inside the executor's connection ownership. */
final readonly class ConfigurationWorkerOperation implements Operation
{
    public function __construct(private SqlExecutor $sql)
    {
    }

    public static function create(SqlExecutor $sql): self
    {
        return new self($sql);
    }

    public function run(array $parameters): OperationResultDTO
    {
        $result = $this->sql->execute('SELECT synchronous FROM pragma_synchronous', []);

        return new OperationResultDTO(Outcome::ReadOnly, ['value' => $result['rows'][0]['synchronous']]);
    }
}
