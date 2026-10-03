<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp;

use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Ineersa\SqliteQueue\SqliteQueueBundle;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Kernel;

final class NativeKernel extends Kernel
{
    use MicroKernelTrait;

    public function __construct(
        private readonly string $projectDir,
        string $environment = 'test',
        bool $debug = true,
    ) {
        parent::__construct($environment, $debug);
    }

    public function registerBundles(): iterable
    {
        return [
            new FrameworkBundle(),
            new SqliteQueueBundle(),
        ];
    }

    public function getProjectDir(): string
    {
        return $this->projectDir;
    }

    public function getCacheDir(): string
    {
        return $this->projectDir.'/var/cache/'.$this->environment;
    }

    public function getLogDir(): string
    {
        return $this->projectDir.'/var/log';
    }

    public function build(ContainerBuilder $container): void
    {
        parent::build($container);
        $container->addCompilerPass(new class implements CompilerPassInterface {
            public function process(ContainerBuilder $container): void
            {
                foreach ([
                    TransportFactory::class,
                    NativeConsumeWaitSubscriber::class,
                    BrokerCommand::class,
                    'messenger.transport.async',
                    'messenger.transport.async_terminal',
                    'messenger.default_bus',
                    'event_dispatcher',
                ] as $id) {
                    if ($container->hasDefinition($id)) {
                        $container->getDefinition($id)->setPublic(true);
                    }
                    if ($container->hasAlias($id)) {
                        $container->getAlias($id)->setPublic(true);
                    }
                }
            }
        });
    }
}
