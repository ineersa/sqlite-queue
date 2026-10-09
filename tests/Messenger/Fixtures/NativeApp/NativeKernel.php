<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp;

use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Ineersa\SqliteQueue\Messenger\TransportFactory;
use Ineersa\SqliteQueue\SqliteQueueBundle;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\Clock\Clock;
use Symfony\Component\Clock\ClockInterface;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;
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
                // Tests may replace the process clock with Clock::set(MockClock).
                $container->register('ineersa.sqlite_queue.test_clock', ClockInterface::class)
                    ->setFactory([Clock::class, 'get']);
                $container->getDefinition(BrokerCommand::class)->setArgument(
                    '$clock',
                    new Reference('ineersa.sqlite_queue.test_clock'),
                );
                // Use Symfony's durable, listable failure receiver without an application Doctrine bundle.
                $dbal = (new \Symfony\Component\DependencyInjection\Definition(\Doctrine\DBAL\Connection::class))
                    ->setFactory([\Doctrine\DBAL\DriverManager::class, 'getConnection'])
                    ->setArguments([['driver' => 'pdo_sqlite', 'path' => '%kernel.project_dir%/failed.sqlite']]);
                $connection = new \Symfony\Component\DependencyInjection\Definition(
                    \Symfony\Component\Messenger\Bridge\Doctrine\Transport\Connection::class,
                    [['queue_name' => 'failed'], $dbal],
                );
                $container->setDefinition('messenger.transport.failed', new \Symfony\Component\DependencyInjection\Definition(
                    \Symfony\Component\Messenger\Bridge\Doctrine\Transport\DoctrineTransport::class,
                    [$connection, new Reference('messenger.transport.native_php_serializer')],
                ));
                foreach ([
                    TransportFactory::class,
                    NativeConsumeWaitSubscriber::class,
                    BrokerCommand::class,
                    'messenger.transport.async',
                    'messenger.transport.reports',
                    'messenger.transport.async_terminal',
                    'messenger.transport.async_failure',
                    'messenger.transport.failed',
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
