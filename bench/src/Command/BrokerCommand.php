<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Amp\DeferredCancellation;
use Ineersa\SqliteQueue\Bench\Backend;
use Ineersa\SqliteQueue\Bench\BrokerReadback;
use Ineersa\SqliteQueue\Bench\Config;
use Ineersa\SqliteQueue\Bench\RuntimeProfile;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Revolt\EventLoop;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

/** Benchmark-owned broker; readiness is emitted only after storage verifies WAL/FULL. */
final class BrokerCommand extends Command
{
    protected function configure(): void
    {
        $this->setName('broker')->addArgument('directory', InputArgument::REQUIRED);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $directory = $input->getArgument('directory');
        if (!\is_string($directory) || !is_dir($directory)) {
            throw new \InvalidArgumentException('Broker requires its repetition directory.');
        }
        $stop = new DeferredCancellation();
        $watchers = [
            EventLoop::onSignal(\SIGTERM, static function () use ($stop): void { $stop->cancel(); }),
            EventLoop::onSignal(\SIGINT, static function () use ($stop): void { $stop->cancel(); }),
            // Same bounded signal-dispatch wakeup as the package CLI.
            EventLoop::repeat(1, static function (): void {}),
        ];
        try {
            $profile = RuntimeProfile::current();
            if (is_file($directory.'/config.json')) {
                $config = json_decode(file_get_contents($directory.'/config.json'), true, 512, \JSON_THROW_ON_ERROR);
                $expected = $config['workload']['runtime_profile'] ?? null;
                if (!\is_array($expected)) {
                    throw new \RuntimeException('Broker assignment requires a runtime profile.');
                }
                RuntimeProfile::verifyCurrent($expected);
            }
            $broker = (new BrokerFactory($directory.'/queue.sqlite', Backend::endpoint($directory), visibilityTimeout: Config::REDELIVER_TIMEOUT_S * 1000))->create();

            return $broker->run(static function (array $event) use ($directory, $broker, $profile): void {
                if (!\is_int($event['persistence_pid'])) {
                    throw new \RuntimeException('Broker readiness requires a SQLite worker PID.');
                }
                $workerProfile = RuntimeProfile::worker($event['persistence_pid'], $profile);
                file_put_contents($directory.'/broker-runtime.json', json_encode(['broker' => $profile, 'sqlite_worker' => $workerProfile], \JSON_THROW_ON_ERROR));
                $durability = BrokerReadback::durability($broker, $directory.'/queue.sqlite');
                file_put_contents($directory.'/broker-durability.json', json_encode($durability, \JSON_THROW_ON_ERROR));
                file_put_contents($directory.'/ready/broker-0.ready', json_encode($event, \JSON_THROW_ON_ERROR));
            }, $stop->getCancellation());
        } finally {
            foreach ($watchers as $watcher) {
                EventLoop::cancel($watcher);
            }
            file_put_contents($directory.'/broker-final.json', json_encode([
                'rusage' => getrusage(),
                'reaped_children_rusage' => getrusage(1),
                'php_peak_bytes' => memory_get_peak_usage(true),
            ], \JSON_THROW_ON_ERROR));
        }
    }
}
