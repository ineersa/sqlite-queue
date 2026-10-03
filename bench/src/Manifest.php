<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

final class Manifest
{
    /** All dirty paths, including unrelated user files, disqualify a formal capture. */
    public static function assertSourceMode(RunOptionsDTO $options, string $status): void
    {
        if (!$options->smoke && !$options->pilot && '' !== trim($status)) {
            throw new \RuntimeException('Formal captures require a completely clean worktree, including unrelated files. Use --pilot to preserve a diagnostic source snapshot.');
        }
    }

    public static function save(string $root, string $directory, RunOptionsDTO $options): void
    {
        $revision = new Process(['git', 'rev-parse', 'HEAD'], $root);
        $revision->mustRun();
        $status = new Process(['git', 'status', '--porcelain', '--untracked-files=all'], $root);
        $status->mustRun();
        self::assertSourceMode($options, $status->getOutput());
        $patch = new Process(['git', 'diff', 'HEAD', '--', 'bench/src', 'src', 'bin', '.castor', 'castor.php', 'composer.json', 'composer.lock', 'tests/Bench', 'bench/README.md', 'docs/benchmark-method.md'], $root);
        $patch->mustRun();
        $filesystem = new Filesystem();
        // Include tracked and untracked implementation and tests, not just a dirty flag.
        $filesystem->mirror($root.'/bench/src', $directory.'/source/bench/src');
        $filesystem->mirror($root.'/tests/Bench', $directory.'/source/tests/Bench');
        $filesystem->mirror($root.'/src', $directory.'/source/src');
        $filesystem->mirror($root.'/.castor', $directory.'/source/.castor');
        foreach (['bin/benchmark', 'bin/sqlite-queue', 'castor.php', '.castor/tasks.php', 'composer.json', 'composer.lock', 'bench/README.md', 'docs/benchmark-method.md'] as $file) {
            $filesystem->copy($root.'/'.$file, $directory.'/source/'.$file);
        }
        $filesystem->dumpFile($directory.'/source.patch', $patch->getOutput());
        $checksums = [];
        $files = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($directory.'/source', \FilesystemIterator::SKIP_DOTS));
        foreach ($files as $file) {
            if ($file instanceof \SplFileInfo && $file->isFile()) {
                $checksums[substr($file->getPathname(), \strlen($directory) + 1)] = hash_file('sha256', $file->getPathname());
            }
        }
        $clock = ClockProbe::run();
        if (!$clock['verified']) {
            throw new \RuntimeException('Cross-process monotonic clock verification failed.');
        }
        ksort($checksums);
        $excluded = [];
        foreach (explode("\n", $status->getOutput()) as $line) {
            if ('' === $line) {
                continue;
            }
            $path = substr($line, 3);
            $included = \in_array($path, ['castor.php', 'composer.json', 'composer.lock', 'bench/README.md', 'docs/benchmark-method.md'], true);
            foreach (['bench/src/', 'src/', 'bin/', '.castor/', 'tests/Bench/'] as $prefix) {
                $included = $included || str_starts_with($path, $prefix);
            }
            if (!$included) {
                $excluded[] = $path;
            }
        }
        Runtime::saveText($directory.'/git-status.txt', $status->getOutput());
        Runtime::saveJson($directory.'/manifest.json', [
            'method' => Config::METHOD_REVISION,
            'schema_version' => Config::SCHEMA_VERSION,
            'source_revision' => trim($revision->getOutput()),
            'diagnostic_dirty_capture' => '' !== trim($status->getOutput()),
            'mode' => $options->configuration()['mode'],
            'dirty_source_policy' => 'all paths recorded in git-status.txt; runtime sources and benchmark tests snapshotted; unrelated non-runtime files explicitly excluded from source snapshot; formal rejects any dirty path',
            'excluded_nonruntime_dirty_paths' => $excluded,
            'source_checksums' => $checksums,
            'source_sha256' => hash('sha256', json_encode($checksums, \JSON_THROW_ON_ERROR)),
            'lock_sha256' => hash_file('sha256', $root.'/composer.lock'),
            'config_sha256' => hash('sha256', json_encode($options->configuration(), \JSON_THROW_ON_ERROR)),
            'messenger_config_sha256' => hash_file('sha256', __DIR__.'/config/packages/messenger.yaml'),
            'configuration' => $options->configuration(),
            'schedule' => $options->schedule(),
            'warmup_messages_per_backend' => Runner::WARMUP_MESSAGES,
            'measured_messages_per_backend' => $options->finiteCohortMessages(),
            'in_flight_limit' => 1,
            'same_process_warmup' => true,
            'payload_seed' => 'repeated-x',
            'clock_probe' => $clock,
            'recording_capacity_bytes' => Recorder::BUFFER_CAPACITY_BYTES,
            'recording_flush_policy' => 'capacity, reset barrier, drain barrier, shutdown',
            'proposed_calibration_throughput_budget' => 0.05,
            'calibration_status' => Scenario::Calibration === $options->scenario ? 'scheduled' : 'not-requested',
            'lower_driver_diagnostics' => null,
            'resource_attribution' => 'phase boundaries only, disabled in declared calibration profiles',
            'php_version' => \PHP_VERSION,
            'environment' => Machine::describe($directory, $root),
            'runtime_verification_policy' => ['publisher' => 'verify-at-acquisition', 'consumer' => 'verify-at-acquisition', 'broker' => 'unavailable', 'persistence_worker' => 'unavailable'],
        ]);
    }
}
