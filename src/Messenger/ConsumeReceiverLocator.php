<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

use Psr\Container\ContainerInterface;

/**
 * Forwards messenger:consume receiver lookups and notifies the wait subscriber of each selected name.
 *
 * Stock ConsumeMessagesCommand resolves receivers through this locator before it reads --sleep for
 * Worker options. The subscriber uses that moment to decide whether the selection includes our
 * Transport without reimplementing native receiver selection.
 */
final class ConsumeReceiverLocator implements ContainerInterface
{
    public function __construct(
        private readonly ContainerInterface $inner,
        private readonly NativeConsumeWaitSubscriber $subscriber,
    ) {
    }

    public function get(string $id): mixed
    {
        $receiver = $this->inner->get($id);
        $this->subscriber->onSelectedReceiver($id);

        return $receiver;
    }

    public function has(string $id): bool
    {
        return $this->inner->has($id);
    }
}
