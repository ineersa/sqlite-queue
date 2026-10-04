<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

final class Payload
{
    private const SEED = 'sqlite-queue-benchmark-payload';

    public static function generate(string $correlation): string
    {
        $root = str_ends_with($correlation, WorkflowAnalysis::RESULT_SUFFIX) ? substr($correlation, 0, -\strlen(WorkflowAnalysis::RESULT_SUFFIX)) : $correlation;
        if (1 !== preg_match('/^(?:warmup|measure|pickup|cycle:[1-9][0-9]*)(?::publisher-[0-2])?:([0-9]+)$/D', $root, $matches)) {
            throw new \RuntimeException('Invalid payload correlation identity.');
        }
        $lastDigit = (int) substr($matches[1], -1);
        $size = 0 === $lastDigit % 2 ? Config::SMALL_PAYLOAD_BYTES : Config::LARGE_PAYLOAD_BYTES;
        $pattern = hash('sha256', self::SEED.':'.$root);

        return substr(str_repeat($pattern, (int) ceil($size / \strlen($pattern))), 0, $size);
    }

    public static function verify(string $correlation, string $payload): void
    {
        $expected = self::generate($correlation);
        if (\strlen($payload) !== \strlen($expected)) {
            throw new \RuntimeException('Payload size does not match correlation identity.');
        }
        if ($payload !== $expected) {
            throw new \RuntimeException('Payload contents do not match deterministic generator.');
        }
    }
}
