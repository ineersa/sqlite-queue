<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue;

use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\DependencyInjection\RegisterNativeConsumeWaitSubscriberPass;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Symfony\Component\Config\Definition\Configurator\DefinitionConfigurator;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;

/**
 * Registers the Messenger transport factory, native consume WAIT subscriber, and broker command.
 *
 * Standalone broker and client APIs do not boot this bundle. Symfony apps that enable Messenger
 * get factory autoconfiguration through the tagged TransportFactory and prompt idle WAIT for a
 * single sqlite-queue receiver consumed by the stock messenger:consume command.
 */
final class SqliteQueueBundle extends AbstractBundle
{
    public function getPath(): string
    {
        return \dirname(__DIR__);
    }

    public function build(ContainerBuilder $container): void
    {
        $container->addCompilerPass(new RegisterNativeConsumeWaitSubscriberPass());
    }

    public function configure(DefinitionConfigurator $definition): void
    {
        $definition->rootNode()->children()
            ->integerNode('visibility_timeout')->min(1)->defaultValue(BrokerCommand::DEFAULT_VISIBILITY_TIMEOUT_MILLISECONDS)->end()
        ->end();
    }

    /** @param array<array-key, mixed> $config */
    public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
    {
        $services = $container->services();

        $services->set(TransportFactory::class)
            ->autoconfigure(false)
            ->tag('messenger.transport_factory');

        $services->set(BrokerCommand::class)
            ->arg('$visibilityTimeoutMilliseconds', $config['visibility_timeout'])
            ->autoconfigure(false)
            ->tag('console.command', ['command' => 'sqlite-queue:broker']);
    }
}
