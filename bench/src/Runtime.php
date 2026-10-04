<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Symfony\Component\Filesystem\Filesystem;

/** Acquires observation resources before constructing initialized services. */
final class Runtime
{
    public static function recorder(): Recorder
    {
        $expected = json_decode(self::environment('BENCH_RUNTIME_PROFILE'), true, flags: \JSON_THROW_ON_ERROR);
        if (!\is_array($expected)) {
            throw new \RuntimeException('Invalid expected runtime profile.');
        }
        RuntimeProfile::verifyCurrent($expected);
        $path = self::environment('BENCH_TELEMETRY');
        $stream = fopen($path, 'ab');
        if (false === $stream) {
            throw new \RuntimeException('Cannot open telemetry.');
        }
        try {
            self::saveJson($path.'.runtime.json', RuntimeProfile::current());

            return new Recorder(static fn (string $bytes): bool => fwrite($stream, $bytes) === \strlen($bytes), Recorder::BUFFER_CAPACITY_BYTES, self::environment('BENCH_RUN'), self::environment('BENCH_ROLE'));
        } catch (\Throwable $error) {
            fclose($stream);
            throw $error;
        }
    }

    public static function control(): Control
    {
        $stream = stream_socket_client('unix://'.self::environment('BENCH_CONTROL'), $code, $error, Config::STARTUP_TIMEOUT_S);
        if (false === $stream) {
            throw new \RuntimeException('Cannot connect control channel: '.$error);
        }

        return new Control($stream);
    }

    public static function environment(string $name): string
    {
        $value = getenv($name);
        if (false === $value || '' === $value) {
            throw new \RuntimeException('Missing environment '.$name);
        }

        return $value;
    }

    /** @param array<array-key, mixed> $data */
    public static function saveJson(string $path, array $data): void
    {
        self::saveText($path, json_encode($data, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR)."\n");
    }

    public static function saveText(string $path, string $data): void
    {
        (new Filesystem())->dumpFile($path, $data);
    }
}
