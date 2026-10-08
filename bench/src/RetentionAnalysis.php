<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Streaming matched-state summaries retain one fixed-size accumulator per known role. */
final class RetentionAnalysis
{
    private const MEMORY_FIELDS = ['rss_bytes', 'pss_bytes', 'private_bytes', 'php_used_bytes', 'php_reserved_bytes', 'php_peak_bytes'];

    /** @param iterable<array<string, mixed>> $rows
     * @return array<string, mixed>
     */
    public static function summarize(iterable $rows): array
    {
        $roles = [];
        foreach ($rows as $row) {
            $cycle = $row['retention_cycle'] ?? null;
            $historical = $row['historical_completions'] ?? null;
            $role = $row['role'] ?? null;
            if (!\is_int($cycle) || !\is_int($historical) || !\is_string($role) || null === Role::tryFrom($role)) {
                continue;
            }
            if (!isset($roles[$role])) {
                $roles[$role] = ['pid' => $row['pid'] ?? null, 'start_ticks' => $row['start_ticks'] ?? null, 'topology' => $row['topology'] ?? null, 'matched_points' => 0, 'excluded_points' => 0, 'identity_changes' => 0, 'last_cycle' => null, 'historical_completions' => 0, 'metrics' => []];
                foreach (self::MEMORY_FIELDS as $field) {
                    $roles[$role]['metrics'][$field] = new MemoryTrend();
                }
            }
            $accumulator = &$roles[$role];
            $sameIdentity = ($row['pid'] ?? null) === $accumulator['pid'] && ($row['start_ticks'] ?? null) === $accumulator['start_ticks'];
            if (!$sameIdentity) {
                ++$accumulator['identity_changes'];
            }
            $inventory = $row['inventory'] ?? [];
            $empty = \is_array($inventory) && 0 === ($inventory['remaining'] ?? null) && 0 === ($inventory['ready'] ?? null) && 0 === ($inventory['inflight'] ?? null);
            if (!$sameIdentity || !$empty || true !== ($row['equivalent_empty_point'] ?? null) || 'observed' !== ($row['coverage'] ?? null) || ($row['topology'] ?? null) !== $accumulator['topology']) {
                ++$accumulator['excluded_points'];
                unset($accumulator);
                continue;
            }
            ++$accumulator['matched_points'];
            $accumulator['last_cycle'] = $cycle;
            $accumulator['historical_completions'] = $historical;
            foreach (self::MEMORY_FIELDS as $field) {
                $value = $row[$field] ?? null;
                if (!\is_int($value)) {
                    continue;
                }
                $accumulator['metrics'][$field]->observe($value, $historical);
            }
            unset($accumulator);
        }
        foreach ($roles as &$role) {
            foreach ($role['metrics'] as $field => $metric) {
                $role['metrics'][$field] = $metric->summary();
            }
        }
        unset($role);

        return ['roles' => $roles, 'interpretation' => 'matched audited-empty states with stable observed process identities and declared client topology; endpoint trends only, no leak-free verdict', 'live_state_coverage' => 'inventory and process identities observed; broker PHP, sessions, pending operations, WAIT registrations, watched queues, timers and buffers unavailable', 'forced_gc' => false, 'process_restarts' => false, 'observer_scope' => 'publisher and coordinator share a process; post-drain analysis allocations contribute to that role'];
    }
}
