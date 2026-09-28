<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Broker;

use Amp\Cancellation;
use Amp\Future;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ProcessContext;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\TimeoutCancellation;

use function Amp\async;

/** Observe the existing driver's process, never replace its worker implementation. */
final class Persistence implements ContextFactory
{
    /** @var ProcessContext<mixed, mixed, mixed>|null */
    private ?ProcessContext $context = null;
    /** @var list<Future<void>> */
    private array $drains = [];

    /**
     * @param string|non-empty-list<string> $script
     *
     * @return ProcessContext<mixed, mixed, mixed>
     */
    public function start(string|array $script, ?Cancellation $cancellation = null): ProcessContext
    {
        $context = (new ProcessContextFactory(environment: ['PATH' => '/usr/bin:/bin', 'LANG' => 'C', 'TZ' => 'UTC']))->start($script, $cancellation);
        $this->context = $context;
        // Drain without logging: persistence diagnostics may contain SQL or data.
        foreach ([$context->getStdout(), $context->getStderr()] as $stream) {
            $this->drains[] = async(static function () use ($stream): void {
                while (null !== $stream->read()) {
                }
            });
        }

        return $context;
    }

    public function pid(): int
    {
        return $this->context?->getPid() ?? throw new \LogicException('Persistence has not started.');
    }

    /** The driver owns join(); observing pipe EOF avoids joining its context twice. */
    public function awaitExit(): void
    {
        foreach ($this->drains as $drain) {
            $drain->await();
        }
    }

    public function close(): void
    {
        if (null !== $this->context && !$this->context->isClosed()) {
            $this->context->close();
        }
        foreach ($this->drains as $drain) {
            $drain->await(new TimeoutCancellation(5));
        }
        $this->drains = [];
    }
}
