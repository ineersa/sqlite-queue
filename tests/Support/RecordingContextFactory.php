<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Support;

use Amp\Cancellation;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;

final class RecordingContextFactory implements ContextFactory
{
    /** @var list<ProcessContext> */
    public array $contexts = [];

    public function __construct(private readonly ProcessContextFactory $factory)
    {
    }

    /** Optional cancellation is the ContextFactory interface contract. */
    public function start(string|array $script, ?Cancellation $cancellation = null): ProcessContext
    {
        $context = $this->factory->start($script, $cancellation);
        $this->contexts[] = $context;

        return $context;
    }
}
