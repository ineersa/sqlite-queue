<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Symfony\Component\Process\Process;

final class Manifest
{
    public static function save(string $root, string $directory, RunOptionsDTO $options): void
    {
        $revision = new Process(['git', 'rev-parse', 'HEAD'], $root);
        $revision->mustRun();
        Runtime::saveJson($directory.'/manifest.json', ['revision' => trim($revision->getOutput()), 'composer_lock_sha256' => hash_file('sha256', $root.'/composer.lock'), 'settings' => $options->configuration(), 'results' => ['doctrine' => 'doctrine/result.json', 'broker' => 'broker/result.json'], 'owning_connection_durability' => []]);
    }

    /** @param array<string, mixed> $evidence */
    public static function recordDurability(string $directory, string $run, array $evidence): void
    {
        $manifest = json_decode((string) file_get_contents($directory.'/manifest.json'), true, flags: \JSON_THROW_ON_ERROR);
        $desired = $manifest['settings']['synchronous_desired'];
        if ([] !== $evidence && (($evidence['desired'] ?? null) !== $desired || ($evidence['effective'] ?? null) !== $desired)) {
            throw new \RuntimeException('Owning synchronous evidence does not match selected mode.');
        }
        $manifest['owning_connection_durability'][$run] = $evidence;
        Runtime::saveJson($directory.'/manifest.json', $manifest);
    }
}
