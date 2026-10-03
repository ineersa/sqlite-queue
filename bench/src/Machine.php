<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * Machine, storage, dependency, and source-revision inventory for a run.
 *
 * A benchmark result without this block cannot be reproduced or compared, so the runner
 * always writes it into the run directory and the report repeats it.
 */
final class Machine
{
    /**
     * @return array<string, mixed>
     */
    public static function describe(string $databaseDirectory, string $rootDir): array
    {
        $clock = self::clock();
        $loadAverage = \function_exists('sys_getloadavg') ? sys_getloadavg() : false;
        $xdebugVersion = phpversion('xdebug');
        $runtime = RuntimeProfile::current();

        return [
            'machine' => [
                'uname' => php_uname('a'),
                'cpu_model' => self::cpuModel(),
                'cpu_count' => self::cpuCount(),
                'memory_total_kb' => self::memoryTotalKb(),
                'load_average' => false === $loadAverage ? [] : $loadAverage,
            ],
            'php' => [
                'version' => \PHP_VERSION,
                'binary' => \PHP_BINARY,
                'sapi' => \PHP_SAPI,
                'timezone' => date_default_timezone_get(),
                'opcache_cli' => \function_exists('opcache_get_status') && false !== @opcache_get_status(false),
                'xdebug_version' => false === $xdebugVersion ? null : $xdebugVersion,
                'xdebug_mode' => [] === $runtime['xdebug_effective_modes'] ? 'off' : implode(',', $runtime['xdebug_effective_modes']),
                'xdebug_ini_mode' => $runtime['xdebug_ini_mode'],
                'xdebug_mode_override' => $runtime['xdebug_mode_override'],
                'runtime_profile' => $runtime,
                'ini_file' => php_ini_loaded_file(),
                'extensions' => [
                    'sqlite3' => \extension_loaded('sqlite3'),
                    'pdo_sqlite' => \extension_loaded('pdo_sqlite'),
                    'posix' => \extension_loaded('posix'),
                    'pcntl' => \extension_loaded('pcntl'),
                ],
            ],
            'sqlite_library' => class_exists(\SQLite3::class) ? \SQLite3::version() : ['versionString' => 'unavailable'],
            'kernel_clock' => $clock,
            'storage' => self::storage($databaseDirectory),
            'database_directory' => $databaseDirectory,
            'packages' => self::packages(),
            'source' => self::source($rootDir),
        ];
    }

    /**
     * @return array{ticks_per_second: int, page_size_bytes: int, source: string}
     */
    public static function clock(): array
    {
        $ticks = self::command(['getconf', 'CLK_TCK']);
        $pageSize = self::command(['getconf', 'PAGESIZE']);

        $ticksPerSecond = \is_string($ticks) && ctype_digit(trim($ticks)) ? (int) trim($ticks) : 100;
        $pageSizeBytes = \is_string($pageSize) && ctype_digit(trim($pageSize)) ? (int) trim($pageSize) : 4096;

        return [
            'ticks_per_second' => $ticksPerSecond,
            'page_size_bytes' => $pageSizeBytes,
            'source' => \is_string($ticks) && \is_string($pageSize) ? 'getconf' : 'assumed-defaults',
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public static function storage(string $directory): array
    {
        $resolved = realpath($directory);
        $real = false === $resolved ? $directory : $resolved;
        $mounts = @file_get_contents('/proc/mounts');
        $best = null;

        if (\is_string($mounts)) {
            foreach (explode("\n", $mounts) as $line) {
                $parts = preg_split('/\s+/', trim($line));
                if (false === $parts || \count($parts) < 4) {
                    continue;
                }

                [$source, $mountPoint, $filesystem, $options] = $parts;
                $mountPoint = str_replace('\\040', ' ', $mountPoint);

                if (!str_starts_with($real.'/', rtrim($mountPoint, '/').'/')) {
                    continue;
                }

                if (null === $best || \strlen($mountPoint) > \strlen($best['mount'])) {
                    $best = [
                        'mount' => $mountPoint,
                        'filesystem' => $filesystem,
                        'source' => $source,
                        'options' => $options,
                    ];
                }
            }
        }

        return [
            'storage' => $best,
            'free_bytes' => false === ($free = @disk_free_space($real)) ? null : $free,
            'total_bytes' => false === ($total = @disk_total_space($real)) ? null : $total,
        ];
    }

    /**
     * @return array<string, string|null>
     */
    public static function packages(): array
    {
        $versions = [];

        foreach ([
            'doctrine/dbal',
            'symfony/doctrine-messenger',
            'symfony/messenger',
        ] as $package) {
            $versions[$package] = self::packageVersion($package);
        }

        $versions['php'] = \PHP_VERSION;

        return $versions;
    }

    /**
     * @return array{commit: string|null, branch: string|null, dirty: bool|null, files_modified: int|null}
     */
    public static function source(string $rootDir): array
    {
        $commit = self::command(['git', '-C', $rootDir, 'rev-parse', 'HEAD']);
        $branch = self::command(['git', '-C', $rootDir, 'rev-parse', '--abbrev-ref', 'HEAD']);
        $status = self::command(['git', '-C', $rootDir, 'status', '--porcelain']);

        if (!\is_string($status)) {
            return ['commit' => null, 'branch' => null, 'dirty' => null, 'files_modified' => null];
        }

        $lines = array_values(array_filter(explode("\n", trim($status)), static fn (string $line): bool => '' !== trim($line)));

        return [
            'commit' => \is_string($commit) ? trim($commit) : null,
            'branch' => \is_string($branch) ? trim($branch) : null,
            'dirty' => [] !== $lines,
            'files_modified' => \count($lines),
        ];
    }

    private static function packageVersion(string $package): ?string
    {
        if (!class_exists(\Composer\InstalledVersions::class)) {
            return null;
        }

        if (!\Composer\InstalledVersions::isInstalled($package)) {
            return null;
        }

        return \Composer\InstalledVersions::getPrettyVersion($package)
            .' @ '.substr((string) \Composer\InstalledVersions::getReference($package), 0, 12);
    }

    private static function cpuModel(): ?string
    {
        $contents = @file_get_contents('/proc/cpuinfo');
        if (!\is_string($contents)) {
            return null;
        }

        if (1 === preg_match('/^model name\s*:\s*(.+)$/m', $contents, $match)) {
            return trim($match[1]);
        }

        return null;
    }

    private static function cpuCount(): ?int
    {
        $contents = @file_get_contents('/proc/cpuinfo');
        if (\is_string($contents)) {
            $count = preg_match_all('/^processor\s*:/m', $contents);

            return $count > 0 ? $count : null;
        }

        return null;
    }

    private static function memoryTotalKb(): ?int
    {
        $contents = @file_get_contents('/proc/meminfo');
        if (\is_string($contents) && 1 === preg_match('/^MemTotal:\s+(\d+)/m', $contents, $match)) {
            return (int) $match[1];
        }

        return null;
    }

    /**
     * Runs a small external command and returns stdout, or null when it is unavailable.
     *
     * @param list<string> $argv
     */
    private static function command(array $argv): ?string
    {
        $process = new \Symfony\Component\Process\Process($argv, env: Process::environment(), timeout: 5);
        try {
            $process->run();
        } catch (\Symfony\Component\Process\Exception\ExceptionInterface) {
            return null;
        }

        return $process->isSuccessful() ? $process->getOutput() : null;
    }
}
