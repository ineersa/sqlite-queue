<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * The stock Doctrine transport, with a package-owned connection factory instead of DoctrineBundle.
 *
 * @implements TransportFactoryInterface<TransportInterface>
 */
final class DoctrineFactory implements TransportFactoryInterface
{
    /** @param array<array-key, mixed> $options */
    public function supports(string $dsn, array $options): bool
    {
        return \in_array($dsn, ['doctrine-benchmark://async', 'doctrine-benchmark://results'], true);
    }

    /** @param array<array-key, mixed> $options */
    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $mode = SqliteSynchronousMode::from(Runtime::environment('BENCH_SYNCHRONOUS'));
        $connection = Baseline::connect(Runtime::environment('BENCH_DATABASE'), $mode);
        $effective = Baseline::durability($connection);
        if (!Baseline::isDurabilityEquivalent($effective, $mode)) {
            $connection->close();
            throw new \RuntimeException('Owning Doctrine connection durability does not match the selected mode.');
        }
        $queue = str_ends_with($dsn, '/results') ? 'results' : 'async';
        Runtime::saveJson(Runtime::environment('BENCH_TELEMETRY').'.'.$queue.'.durability.json', ['desired' => $mode->value, 'effective' => $effective, 'authority' => 'actual Doctrine transport owning connection', 'pid' => getmypid()]);
        $transport = Baseline::transport($connection, str_ends_with($dsn, '/results') ? 'results' : 'async', $serializer);
        $transport->setup();

        return $transport;
    }
}
