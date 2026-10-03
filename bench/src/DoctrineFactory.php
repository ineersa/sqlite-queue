<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

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
        $connection = Baseline::connect(Runtime::environment('BENCH_DATABASE'));
        $transport = Baseline::transport($connection, str_ends_with($dsn, '/results') ? 'results' : 'async', $serializer);
        $transport->setup();

        return $transport;
    }
}
