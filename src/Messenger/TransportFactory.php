<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

use Amp\Cancellation;
use Ineersa\SqliteQueue\Client;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;

/**
 * Builds a Transport from `sqlite-queue://<queue>?endpoint=/abs/path.sock`.
 *
 * The DSN authority is the literal QueueName. endpoint is required via query or
 * options. timeout defaults to Client's 10s transport allowance. Explicit options
 * override query values. Symfony may inject transport_name metadata; credentials,
 * ports, fragments, and nonempty URL paths are rejected. Connection acquisition is
 * deferred until the first operation, with no retry after failed acquisition.
 *
 * @implements TransportFactoryInterface<Transport>
 */
final class TransportFactory implements TransportFactoryInterface
{
    private const string SCHEME = 'sqlite-queue';
    private const float DEFAULT_TIMEOUT_SECONDS = 10.0;

    /** @param array<mixed> $options */
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): Transport
    {
        if (!$this->supports($dsn, $options)) {
            throw new \InvalidArgumentException(\sprintf('Unsupported Messenger DSN scheme. Expected "%s://".', self::SCHEME));
        }

        $parts = parse_url($dsn);
        if (false === $parts) {
            throw new \InvalidArgumentException('The Messenger DSN is malformed.');
        }
        $this->assertNoCredentials($parts);
        $this->assertNoPort($parts);
        $this->assertNoFragment($parts);
        $this->assertNoPath($parts);

        if (!isset($parts['host']) || !\is_string($parts['host']) || '' === $parts['host']) {
            throw new \InvalidArgumentException('The Messenger DSN authority must be a queue name.');
        }
        $queue = new QueueName($parts['host']);

        $query = [];
        if (isset($parts['query'])) {
            if (!\is_string($parts['query'])) {
                throw new \InvalidArgumentException('The Messenger DSN query string is malformed.');
            }
            parse_str($parts['query'], $query);
        }

        if (\array_key_exists('transport_name', $options) && !\is_string($options['transport_name'])) {
            throw new \InvalidArgumentException('transport_name must be a string when provided.');
        }
        unset($options['transport_name']);

        $merged = $query;
        foreach ($options as $key => $value) {
            if (!\is_string($key) || '' === $key) {
                throw new \InvalidArgumentException('Transport option names must be non-empty strings.');
            }
            $merged[$key] = $value;
        }

        foreach (array_keys($merged) as $key) {
            if ('endpoint' !== $key && 'timeout' !== $key) {
                throw new \InvalidArgumentException(\sprintf('Unknown sqlite-queue transport option: %s.', $key));
            }
        }

        if (!\array_key_exists('endpoint', $merged)) {
            throw new \InvalidArgumentException('The sqlite-queue transport requires an endpoint path.');
        }
        $endpoint = $merged['endpoint'];
        if (!\is_string($endpoint)) {
            throw new \InvalidArgumentException('The sqlite-queue endpoint must be a string path.');
        }
        if ('' === $endpoint) {
            throw new \InvalidArgumentException('The sqlite-queue transport requires a non-empty endpoint path.');
        }
        if (!str_starts_with($endpoint, '/')) {
            throw new \InvalidArgumentException('The sqlite-queue endpoint must be an absolute filesystem path.');
        }

        $timeout = self::DEFAULT_TIMEOUT_SECONDS;
        if (\array_key_exists('timeout', $merged)) {
            $timeout = $this->positiveTimeout($merged['timeout']);
        }

        $connect = static fn (Cancellation $cancellation): Client => Client::connect($endpoint, $timeout, $cancellation);

        return new Transport(new BrokerConnection($connect), $queue, $serializer, new BrokerConnection($connect));
    }

    /** @param array<mixed> $options */
    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, self::SCHEME.'://');
    }

    /**
     * @param array<string, mixed> $parts
     */
    private function assertNoCredentials(array $parts): void
    {
        if (\array_key_exists('user', $parts) || \array_key_exists('pass', $parts)) {
            throw new \InvalidArgumentException('The sqlite-queue DSN must not include credentials.');
        }
    }

    /**
     * @param array<string, mixed> $parts
     */
    private function assertNoPort(array $parts): void
    {
        if (\array_key_exists('port', $parts)) {
            throw new \InvalidArgumentException('The sqlite-queue DSN must not include a port.');
        }
    }

    /**
     * @param array<string, mixed> $parts
     */
    private function assertNoFragment(array $parts): void
    {
        if (\array_key_exists('fragment', $parts)) {
            throw new \InvalidArgumentException('The sqlite-queue DSN must not include a fragment.');
        }
    }

    /**
     * @param array<string, mixed> $parts
     */
    private function assertNoPath(array $parts): void
    {
        if (!\array_key_exists('path', $parts)) {
            return;
        }
        $path = $parts['path'];
        if (!\is_string($path) || '' === $path) {
            return;
        }

        throw new \InvalidArgumentException('The sqlite-queue DSN must not include a URL path.');
    }

    private function positiveTimeout(mixed $timeout): float
    {
        if (\is_string($timeout)) {
            if (!is_numeric($timeout)) {
                throw new \InvalidArgumentException('timeout must be a number of seconds.');
            }
            $timeout = (float) $timeout;
        }
        if (!\is_int($timeout) && !\is_float($timeout)) {
            throw new \InvalidArgumentException('timeout must be a number of seconds.');
        }
        if ($timeout <= 0) {
            throw new \InvalidArgumentException('timeout must be positive.');
        }
        if (!is_finite($timeout)) {
            throw new \InvalidArgumentException('timeout must be finite.');
        }

        return (float) $timeout;
    }
}
