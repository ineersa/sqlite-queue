<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Fixed known-role registry. Records stream to disk; no whole-host scan or retained series. */
final class Resources
{
    public const SAMPLE_INTERVAL_NS = 200000000;
    /** @var array<string, array{pid: int, start_ticks: int}> */
    private array $registry = [];
    private int $next = 0;
    private int $lost = 0;

    /** @param \Closure(string): (string|false) $read
     * @param \Closure(string): bool $write
     */
    public function __construct(private readonly Clock $clock, private readonly \Closure $read, private readonly \Closure $write, private readonly int $ticksPerSecond, private readonly int $pageBytes, private readonly bool $periodic)
    {
        if ($ticksPerSecond < 1) {
            throw new \InvalidArgumentException('CPU tick frequency must be positive.');
        }
        if ($pageBytes < 1) {
            throw new \InvalidArgumentException('Memory page size must be positive.');
        }
    }

    public function register(Role $role, int $pid): void
    {
        $stat = $this->stat($pid);
        if ([] === $stat) {
            throw new \RuntimeException('Cannot register process identity for '.$role->value);
        }
        $this->registry[$role->value] = ['pid' => $pid, 'start_ticks' => $stat['start_ticks']];
    }

    public function tick(string $phase): void
    {
        $now = $this->clock->now();
        if ($this->periodic && $now >= $this->next) {
            $this->capture($phase, false);
            $this->next = $now + self::SAMPLE_INTERVAL_NS;
        }
    }

    /** @param array<string, array<string, mixed>> $actors Cooperative PHP snapshots, empty when unavailable
     * @param array<string, mixed> $context optional fixed-size cycle metadata, absent outside retention
     */
    public function capture(string $phase, bool $detailed, array $actors = [], array $context = []): void
    {
        foreach ($this->registry as $role => $identity) {
            $row = $this->snapshot($role, $identity, $phase, $detailed);
            $actor = $actors[$role] ?? [];
            if (($actor['pid'] ?? null) === $identity['pid'] && 'observed' === $row['coverage']) {
                foreach (['used_bytes' => 'php_used_bytes', 'reserved_bytes' => 'php_reserved_bytes', 'peak_bytes' => 'php_peak_bytes'] as $field => $target) {
                    $row[$target] = \is_int($actor[$field] ?? null) ? $actor[$field] : null;
                }
                $row['php_snapshot_ns'] = $actor['monotonic_ns'] ?? null;
            }
            $this->append($row + $context);
        }
    }

    /** @return array<string, mixed> */
    public function coverage(): array
    {
        return ['registry' => $this->registry, 'lost_samples' => $this->lost, 'interval_milliseconds' => self::SAMPLE_INTERVAL_NS / 1000000, 'pss_policy' => 'phase-only', 'product_total' => 'partial: publisher and observer share a process', 'process_exit_accounting' => 'unavailable', 'broker_php_and_gauges' => 'unavailable'];
    }

    /** @param iterable<array<string, mixed>> $rows
     * @return array<string, mixed>
     */
    public static function summarize(iterable $rows): array
    {
        $previous = [];
        $phases = [];
        $deltas = [];
        foreach ($rows as $row) {
            $role = $row['role'];
            $phase = $row['phase'];
            $phases[$phase][$role] = $row;
            $before = $previous[$role] ?? [];
            if ('observed' === ($row['coverage'] ?? '') && 'observed' === ($before['coverage'] ?? '') && $row['start_ticks'] === $before['start_ticks'] && $row['pid'] === $before['pid']) {
                $delta = [];
                $beforeTime = $before['monotonic_ns'] ?? null;
                $afterTime = $row['monotonic_ns'] ?? null;
                $delta['elapsed_seconds'] = \is_int($beforeTime) && \is_int($afterTime) ? ($afterTime - $beforeTime) / 1e9 : null;
                foreach (['user_seconds', 'system_seconds'] as $field) {
                    $delta[$field] = $row[$field] - $before[$field];
                }
                $delta['io'] = null;
                if (\is_array($row['io']) && \is_array($before['io'])) {
                    $delta['io'] = [];
                    foreach ($before['io'] as $field => $value) {
                        $delta['io'][$field] = isset($row['io'][$field]) ? $row['io'][$field] - $value : null;
                    }
                }
                $prior = $deltas[$before['phase']][$role] ?? [];
                foreach (['user_seconds', 'system_seconds'] as $field) {
                    $delta[$field] += $prior[$field] ?? 0;
                }
                if (isset($prior['elapsed_seconds']) && null !== $delta['elapsed_seconds']) {
                    $delta['elapsed_seconds'] += $prior['elapsed_seconds'];
                }
                if (\is_array($delta['io']) && \is_array($prior['io'] ?? null)) {
                    foreach ($delta['io'] as $field => $value) {
                        $delta['io'][$field] = null === $value || null === ($prior['io'][$field] ?? null) ? null : $value + $prior['io'][$field];
                    }
                }
                $deltas[$before['phase']][$role] = $delta;
            }
            $previous[$role] = $row;
        }
        foreach ($deltas as $phase => $roles) {
            foreach ($roles as $role => $delta) {
                $phases[$phase][$role]['until_next_boundary'] = $delta;
            }
        }

        return ['phase_roles' => $phases, 'coverage' => 'boundary-to-boundary only; pre-first-sample and process-exit work unavailable', 'memory_policy' => 'current snapshots, not active peaks or retention evidence'];
    }

    /** @param array{pid: int, start_ticks: int} $identity
     * @return array<string, mixed>
     */
    private function snapshot(string $role, array $identity, string $phase, bool $detailed): array
    {
        $pid = $identity['pid'];
        $stat = $this->stat($pid);
        $row = ['role' => $role, 'pid' => $pid, 'start_ticks' => $identity['start_ticks'], 'phase' => $phase, 'monotonic_ns' => $this->clock->now(), 'coverage' => 'unavailable', 'user_seconds' => null, 'system_seconds' => null, 'rss_bytes' => null, 'pss_bytes' => null, 'private_bytes' => null, 'io' => null, 'php_used_bytes' => null, 'php_reserved_bytes' => null, 'php_peak_bytes' => null];
        if ([] === $stat) {
            return $row;
        }
        if ($stat['start_ticks'] !== $identity['start_ticks']) {
            $row['coverage'] = 'pid-reused';

            return $row;
        }
        $row['coverage'] = 'observed';
        $row['user_seconds'] = $stat['user_ticks'] / $this->ticksPerSecond;
        $row['system_seconds'] = $stat['system_ticks'] / $this->ticksPerSecond;
        $row['rss_bytes'] = $stat['rss_pages'] * $this->pageBytes;
        $io = ($this->read)('/proc/'.$pid.'/io');
        if (false !== $io) {
            $values = [];
            foreach (explode("\n", trim($io)) as $line) {
                if (preg_match('/^([a-z_]+): ([0-9]+)$/D', $line, $match)) {
                    $values[$match[1]] = (int) $match[2];
                }
            }
            $row['io'] = $values;
        }
        if ($detailed) {
            $smaps = ($this->read)('/proc/'.$pid.'/smaps_rollup');
            if (false !== $smaps) {
                $private = [];
                foreach (['Pss', 'Private_Clean', 'Private_Dirty'] as $field) {
                    if (preg_match('/^'.$field.':\s+([0-9]+) kB$/m', $smaps, $match)) {
                        $private[$field] = (int) $match[1] * 1024;
                    }
                }
                $row['pss_bytes'] = $private['Pss'] ?? null;
                if (isset($private['Private_Clean'], $private['Private_Dirty'])) {
                    $row['private_bytes'] = $private['Private_Clean'] + $private['Private_Dirty'];
                }
            }
        }
        if (Role::ObserverPublisher->value === $role) {
            $row['php_used_bytes'] = memory_get_usage(false);
            $row['php_reserved_bytes'] = memory_get_usage(true);
            $row['php_peak_bytes'] = memory_get_peak_usage(true);
        }
        $after = $this->stat($pid);
        if ([] === $after || $after['start_ticks'] !== $identity['start_ticks']) {
            return ['role' => $role, 'pid' => $pid, 'start_ticks' => $identity['start_ticks'], 'phase' => $phase, 'monotonic_ns' => $this->clock->now(), 'coverage' => 'identity-changed-during-snapshot'];
        }

        return $row;
    }

    /** @return array<string, int> */
    private function stat(int $pid): array
    {
        $data = ($this->read)('/proc/'.$pid.'/stat');
        if (false === $data) {
            return [];
        }
        $end = strrpos($data, ')');
        if (false === $end) {
            return [];
        }
        $fields = preg_split('/\s+/', trim(substr($data, $end + 1)));
        if (false === $fields) {
            return [];
        }
        $result = [];
        foreach (['user_ticks' => 11, 'system_ticks' => 12, 'start_ticks' => 19, 'rss_pages' => 21] as $name => $index) {
            if (!isset($fields[$index]) || !ctype_digit($fields[$index])) {
                return [];
            }
            $result[$name] = (int) $fields[$index];
        }

        return $result;
    }

    /** @param array<string, mixed> $row */
    private function append(array $row): void
    {
        try {
            $ok = ($this->write)(json_encode($row, \JSON_THROW_ON_ERROR)."\n");
        } catch (\Throwable) {
            $ok = false;
        }
        if (!$ok) {
            ++$this->lost;
        }
    }
}
