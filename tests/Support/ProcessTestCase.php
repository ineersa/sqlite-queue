<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use PHPUnit\Framework\TestCase;

use function Amp\async;

/**
 * Isolates database files for local storage tests.
 */
abstract class ProcessTestCase extends TestCase
{
    protected ?IsolatedDatabase $database = null;

    protected function setUp(): void
    {
        parent::setUp();
        $this->database = new IsolatedDatabase();
    }

    protected function tearDown(): void
    {
        $this->database?->remove();
        parent::tearDown();
    }

    final protected function runAsync(callable $operation): mixed
    {
        return async($operation)->await();
    }
}
