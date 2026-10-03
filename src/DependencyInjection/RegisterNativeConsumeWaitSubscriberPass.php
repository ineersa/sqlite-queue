<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\DependencyInjection;

use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Symfony\Component\Clock\Clock;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

/**
 * Registers NativeConsumeWaitSubscriber after FrameworkBundle defines the receiver locator.
 *
 * Bundle loadExtension() can run before messenger.receiver_locator exists, so the subscriber
 * is attached in a late compiler pass instead of during extension load.
 */
final class RegisterNativeConsumeWaitSubscriberPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if ($container->hasDefinition(NativeConsumeWaitSubscriber::class)) {
            return;
        }
        if (!$container->hasDefinition('messenger.receiver_locator') && !$container->hasAlias('messenger.receiver_locator')) {
            return;
        }

        $definition = new Definition(NativeConsumeWaitSubscriber::class);
        $definition->setAutowired(false);
        $definition->setAutoconfigured(false);
        $definition->setArguments([
            new Reference('messenger.receiver_locator'),
            // The native Worker also constructs Clock, which delegates to Clock::get().
            new Definition(Clock::class),
        ]);
        $definition->addTag('kernel.event_subscriber');
        $container->setDefinition(NativeConsumeWaitSubscriber::class, $definition);
    }
}
