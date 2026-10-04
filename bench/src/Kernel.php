<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Ineersa\SqliteQueue\Messenger\NativeConsumeWaitSubscriber;
use Ineersa\SqliteQueue\SqliteQueueBundle;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Argument\ServiceClosureArgument;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\HttpKernel\Kernel as BaseKernel;

final class Kernel extends BaseKernel
{
    use MicroKernelTrait;

    public function __construct(private readonly string $project)
    {
        parent::__construct('benchmark', false);
    }

    public function registerBundles(): iterable
    {
        return [new FrameworkBundle(), new SqliteQueueBundle()];
    }

    public function getProjectDir(): string
    {
        return $this->project;
    }

    public function getCacheDir(): string
    {
        // The lifecycle service selects its observed receiver at container compilation.
        return $this->project.'/cache/'.Runtime::environment('BENCH_RECEIVER');
    }

    public function getLogDir(): string
    {
        return $this->project.'/logs';
    }

    public function build(ContainerBuilder $container): void
    {
        parent::build($container);
        $container->register(Clock::class)->setFactory([Clock::class, 'system']);
        $container->register(Recorder::class)->setFactory([Runtime::class, 'recorder'])->setPublic(true);
        $container->register(Command\PublisherCommand::class)->setArguments([new Reference('messenger.default_bus'), new Reference(Recorder::class), new Reference(Control::class), new Reference(ObservedTransport::class)])->addTag('console.command');
        $container->register(Control::class)->setFactory([Runtime::class, 'control']);
        $container->register(DoctrineFactory::class)->addTag('messenger.transport_factory');
        $container->register(Handler::class)->setArguments([new Reference(Recorder::class), new Reference('messenger.default_bus'), new Reference(Clock::class)])->addTag('messenger.message_handler');
        $container->register(LifecycleSubscriber::class)->setArguments([new Reference(Recorder::class), new Reference(Control::class), new Reference('results' === Runtime::environment('BENCH_RECEIVER') ? ObservedTransport::class.'.results' : ObservedTransport::class)])->addTag('kernel.event_subscriber');
        $container->register(ObservedTransport::class)->setPublic(true)->setDecoratedService('messenger.transport.async')->setArguments([new Reference(ObservedTransport::class.'.inner'), new Reference(Recorder::class), new Reference(Control::class), new Reference(Clock::class)]);
        $container->register(ObservedTransport::class.'.results', ObservedTransport::class)->setDecoratedService('messenger.transport.results')->setArguments([new Reference(ObservedTransport::class.'.results.inner'), new Reference(Recorder::class), new Reference(Control::class), new Reference(Clock::class)]);
        $container->addCompilerPass(new class implements CompilerPassInterface {
            public function process(ContainerBuilder $container): void
            {
                // Stock command receives the observing wrapper. Production WAIT resolves the original Transport.
                $locator = new Definition(ServiceLocator::class, [['async' => new ServiceClosureArgument(new Reference(ObservedTransport::class.'.inner')), 'results' => new ServiceClosureArgument(new Reference(ObservedTransport::class.'.results.inner'))]]);
                $locator->addTag('container.service_locator');
                $container->getDefinition(NativeConsumeWaitSubscriber::class)->setArgument(0, $locator);
                $container->getAlias('messenger.default_bus')->setPublic(true);
            }
        }, priority: -100);
    }
}
