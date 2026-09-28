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

        return [
            'machine' => [
                'uname' => \php_uname('a'),
                'cpu_model' => self::cpuModel(),
                'cpu_count' => self::cpuCount(),
                'memory_total_kb' => self::memoryTotalKb(),
                'load_average' => \function_exists('sys_getloadavg') ? (\sys_getloadavg() ?: []) : [],
            ],
            'php' => [
                'version' => \PHP_VERSION,
                'binary' => \PHP_BINARY,
                'sapi' => \PHP_SAPI,
                'timezone' => \date_default_timezone_get(),
                'opcache_cli' => \function_exists('opcache_get_status') && false !== @\opcache_get_status(false),
                'xdebug_version' => phpversion('xdebug') ?: null,
                'xdebug_mode' => ini_get('xdebug.mode'),
                'ini_file' => php_ini_loaded_file(),
                'extensions' => [
                    'sqlite3' => \extension_loaded('sqlite3'),
                    'pdo_sqlite' => \extension_loaded('pdo_sqlite'),
                    'posix' => \extension_loaded('posix'),
                    'pcntl' => \extension_loaded('pcntl'),
                ],
            ],
            'sqlite_library' => \class_exists(\SQLite3::class) ? \SQLite3::version() : ['versionString' => 'unavailable'],
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

        $ticksPerSecond = \is_string($ticks) && \ctype_digit(\trim($ticks)) ? (int) \trim($ticks) : 100;
        $pageSizeBytes = \is_string($pageSize) && \ctype_digit(\trim($pageSize)) ? (int) \trim($pageSize) : 4096;

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
        $real = \realpath($directory) ?: $directory;
        $mounts = @\file_get_contents('/proc/mounts');
        $best = null;

        if (\is_string($mounts)) {
            foreach (\explode("\n", $mounts) as $line) {
                $parts = \preg_split('/\s+/', \trim($line)) ?: [];
                if (\count($parts) < 4) {
                    continue;
                }

                [$source, $mountPoint, $filesystem, $options] = $parts;
                $mountPoint = \str_replace('\\040', ' ', $mountPoint);

                if (!\str_starts_with($real . '/', \rtrim($mountPoint, '/') . '/')) {
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
            'free_bytes' => @\disk_free_space($real) ?: null,
            'total_bytes' => @\disk_total_space($real) ?: null,
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

        $lines = \array_values(\array_filter(\explode("\n", \trim($status)), static fn (string $line): bool => '' !== \trim($line)));

        return [
            'commit' => \is_string($commit) ? \trim($commit) : null,
            'branch' => \is_string($branch) ? \trim($branch) : null,
            'dirty' => [] !== $lines,
            'files_modified' => \count($lines),
        ];
    }

    private static function packageVersion(string $package): ?string
    {
        if (!\class_exists(\Composer\InstalledVersions::class)) {
            return null;
        }

        if (!\Composer\InstalledVersions::isInstalled($package)) {
            return null;
        }

        return \Composer\InstalledVersions::getPrettyVersion($package)
            . ' @ ' . \substr((string) \Composer\InstalledVersions::getReference($package), 0, 12);
    }

    private static function cpuModel(): ?string
    {
        $contents = @\file_get_contents('/proc/cpuinfo');
        if (!\is_string($contents)) {
            return null;
        }

        if (\preg_match('/^model name\s*:\s*(.+)$/m', $contents, $match) === 1) {
            return \trim($match[1]);
        }

        return null;
    }

    private static function cpuCount(): ?int
    {
        $contents = @\file_get_contents('/proc/cpuinfo');
        if (\is_string($contents)) {
            $count = \preg_match_all('/^processor\s*:/m', $contents);

            return $count > 0 ? $count : null;
        }

        return null;
    }

    private static function memoryTotalKb(): ?int
    {
        $contents = @\file_get_contents('/proc/meminfo');
        if (\is_string($contents) && \preg_match('/^MemTotal:\s+(\d+)/m', $contents, $match) === 1) {
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
        if (!\function_exists('proc_open')) {
            return null;
        }

        $pipes = [];
        $handle = @\proc_open($argv, [1 => ['pipe', 'wb'], 2 => ['file', '/dev/null', 'wb']], $pipes, null, ['PATH' => '/usr/bin:/bin']);

        if (!\is_resource($handle)) {
            return null;
        }

        $output = \stream_get_contents($pipes[1]);
        \fclose($pipes[1]);
        $code = \proc_close($handle);

        if (0 !== $code || !\is_string($output)) {
            return null;
        }

        return $output;
    }
}
