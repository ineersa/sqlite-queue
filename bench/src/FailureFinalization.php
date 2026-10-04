<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Runs only after cleanup; incomplete actor evidence remains partial, never a clean pass. */
final class FailureFinalization
{
    /** @param list<string> $roles
     * @return array<string, mixed>
     */
    public static function analyze(string $directory, array $roles, Phase $phase, int $start, int $end, bool $application): array
    {
        $issues = [];
        $requestedPhase = $phase;
        $journal = $directory.'/'.(Phase::Warmup === $phase ? 'warmup-expected-ids.txt' : 'expected-ids.txt');
        if (Phase::Warmup !== $phase && is_file($journal) && 0 === filesize($journal) && is_file($directory.'/warmup-expected-ids.txt')) {
            $phase = Phase::Warmup;
            $journal = $directory.'/warmup-expected-ids.txt';
            if (is_file($directory.'/phases.json')) {
                try {
                    $phases = json_decode((string) file_get_contents($directory.'/phases.json'), true, flags: \JSON_THROW_ON_ERROR);
                    if (\is_array($phases) && \is_int($phases['warmup'] ?? null)) {
                        $start = $phases['warmup'];
                        $end = \is_int($phases['reset'] ?? null) ? $phases['reset'] : $end;
                    }
                } catch (\Throwable) {
                    $issues['phase-boundaries']['unreadable'] = true;
                }
            }
        }
        if (!is_file($journal) || $end <= $start) {
            return ['integrity_status' => IntegrityStatus::Unknown->value, 'accounting_status' => AccountingStatus::Partial->value, 'failure_analysis' => ['coverage' => 'unavailable: expected journal or phase boundaries missing']];
        }
        $ids = static function () use ($journal, $application): \Generator {
            $file = new \SplFileObject($journal, 'rb');
            while (!$file->eof()) {
                $id = trim($file->fgets());
                if ('' !== $id) {
                    yield $id;
                    if ($application) {
                        yield $id.WorkflowAnalysis::RESULT_SUFFIX;
                    }
                }
            }
        };
        $events = static function () use ($directory, $roles, &$issues): \Generator {
            foreach ($roles as $role) {
                yield from self::availableEvents($directory.'/'.$role.'.operations.jsonl', $role, $issues);
            }
        };
        try {
            $path = $directory.'/failure-analysis.sqlite';
            $accounting = Analysis::build($path, $events(), $ids(), $start, $end, $phase);
            if ($application) {
                $accounting = WorkflowAnalysis::apply($path, $accounting, $start, $end);
            }
        } catch (\Throwable $error) {
            return ['integrity_status' => IntegrityStatus::Unknown->value, 'accounting_status' => AccountingStatus::Partial->value, 'failure_analysis' => ['coverage' => 'offline analysis failed', 'error' => $error::class.': '.$error->getMessage(), 'actor_issues' => $issues]];
        }
        $terminal = self::terminalFooters($directory, $roles);
        $footers = $terminal['footers'];
        $issues = array_replace_recursive($issues, $terminal['issues']);
        $accounting['telemetry_footers'] = $footers;
        $accounting['accounting_status'] = [] === $issues ? AccountingStatus::Complete->value : AccountingStatus::Partial->value;
        if ([] !== $issues && IntegrityStatus::Pass->value === $accounting['integrity_status']) {
            $accounting['integrity_status'] = IntegrityStatus::Unknown->value;
        }
        if (Phase::Warmup === $phase) {
            $accounting['warmup_integrity_status'] = $accounting['integrity_status'];
            $accounting['accounting_status'] = AccountingStatus::Partial->value;
            if (IntegrityStatus::Pass->value === $accounting['integrity_status']) {
                $accounting['integrity_status'] = IntegrityStatus::Unknown->value;
            }
        }
        $accounting['failure_analysis'] = ['requested_phase' => $requestedPhase->value, 'phase' => $phase->value, 'window_start_ns' => $start, 'window_end_ns' => $end, 'coverage' => 'available records only, after process cleanup; no replay or inventory assumptions', 'actor_issues' => $issues, 'original_failure_preserved' => true];

        return $accounting;
    }

    /** @param list<string> $roles
     * @return array{footers: array<string, array<string, mixed>>, issues: array<string, array<string, mixed>>}
     */
    public static function terminalFooters(string $directory, array $roles): array
    {
        $issues = [];
        $footers = [];
        foreach ($roles as $role) {
            $path = $directory.'/'.$role.'.operations.jsonl.counters.json';
            try {
                if (!is_file($path)) {
                    throw new \RuntimeException('Actor footer missing.');
                }
                $footer = json_decode((string) file_get_contents($path), true, flags: \JSON_THROW_ON_ERROR);
                if (!\is_array($footer)) {
                    throw new \RuntimeException('Actor footer must be an object.');
                }
                if (true !== ($footer['finalized'] ?? null)) {
                    throw new \RuntimeException('Actor footer is not terminal; a phase snapshot cannot prove complete telemetry.');
                }
                foreach (['lost_records', 'write_failures', 'buffered_bytes'] as $field) {
                    if (!\is_int($footer[$field] ?? null) || $footer[$field] < 0) {
                        throw new \RuntimeException('Actor footer lacks valid '.$field.'.');
                    }
                    if ($footer[$field] > 0) {
                        $issues[$role][$field] = $footer[$field];
                    }
                }
                if (!\is_array($footer['empty_receives_by_phase'] ?? null)) {
                    throw new \RuntimeException('Actor footer lacks empty receive aggregates.');
                }
                if (($footer['empty_duration_bin_upper_ns'] ?? null) !== Recorder::EMPTY_DURATION_BINS_NS) {
                    throw new \RuntimeException('Actor footer has unexpected empty receive bins.');
                }
                foreach ($footer['empty_receives_by_phase'] as $phase => $aggregate) {
                    if (!\is_string($phase) || null === Phase::tryFrom($phase) || !\is_array($aggregate)) {
                        throw new \RuntimeException('Actor footer has invalid empty receive phase.');
                    }
                    foreach (['count', 'sum_active_ns', 'max_active_ns', 'first_started_ns', 'last_ended_ns'] as $field) {
                        if (!\is_int($aggregate[$field] ?? null) || $aggregate[$field] < 0) {
                            throw new \RuntimeException('Invalid empty receive aggregate '.$field.'.');
                        }
                    }
                    $histogram = $aggregate['histogram'] ?? [];
                    if (!\is_array($histogram) || \count($histogram) !== \count(Recorder::EMPTY_DURATION_BINS_NS) + 1) {
                        throw new \RuntimeException('Invalid empty receive histogram size.');
                    }
                    foreach ($histogram as $count) {
                        if (!\is_int($count) || $count < 0) {
                            throw new \RuntimeException('Invalid empty receive histogram count.');
                        }
                    }
                    if (array_sum($histogram) !== $aggregate['count']) {
                        throw new \RuntimeException('Empty receive histogram does not reconcile with count.');
                    }
                    if ($aggregate['last_ended_ns'] < $aggregate['first_started_ns']) {
                        throw new \RuntimeException('Empty receive aggregate boundaries are reversed.');
                    }
                    if ($aggregate['sum_active_ns'] < $aggregate['max_active_ns']) {
                        throw new \RuntimeException('Empty receive aggregate maximum exceeds its duration sum.');
                    }
                }
                $footers[$role] = $footer;
            } catch (\Throwable $error) {
                $issues[$role]['footer_error'] = $error::class.': '.$error->getMessage();
            }
        }

        return ['footers' => $footers, 'issues' => $issues];
    }

    /** @param array<string, array<string, mixed>> $issues
     * @return \Generator<int, array<string, mixed>>
     */
    private static function availableEvents(string $path, string $role, array &$issues): \Generator
    {
        if (!is_file($path)) {
            $issues[$role]['telemetry_missing'] = true;

            return;
        }
        $stream = fopen($path, 'rb');
        if (false === $stream) {
            $issues[$role]['telemetry_unreadable'] = true;

            return;
        }
        try {
            while (false !== ($line = fgets($stream, Recorder::BUFFER_CAPACITY_BYTES + 1))) {
                if ('' === trim($line)) {
                    continue;
                }
                if (!str_ends_with($line, "\n")) {
                    $issues[$role]['interrupted_or_oversized_records'] = ($issues[$role]['interrupted_or_oversized_records'] ?? 0) + 1;
                    while (!str_ends_with($line, "\n") && !feof($stream)) {
                        $line = fgets($stream, Recorder::BUFFER_CAPACITY_BYTES + 1);
                        if (false === $line) {
                            break;
                        }
                    }
                    continue;
                }
                try {
                    $event = json_decode($line, true, flags: \JSON_THROW_ON_ERROR);
                    if (!\is_array($event)) {
                        throw new \RuntimeException('Event must be an object.');
                    }
                    foreach (['phase', 'operation', 'outcome', 'correlation'] as $field) {
                        if (!\is_string($event[$field] ?? null)) {
                            throw new \RuntimeException('Event lacks string '.$field.'.');
                        }
                    }
                    if (null === Phase::tryFrom($event['phase']) || null === Operation::tryFrom($event['operation']) || null === Outcome::tryFrom($event['outcome'])) {
                        throw new \RuntimeException('Unknown event phase, operation or outcome.');
                    }
                    foreach (['started_ns', 'ended_ns'] as $field) {
                        if (!\is_int($event[$field] ?? null)) {
                            throw new \RuntimeException('Event lacks integer '.$field.'.');
                        }
                    }
                    yield $event;
                } catch (\Throwable) {
                    $issues[$role]['invalid_records'] = ($issues[$role]['invalid_records'] ?? 0) + 1;
                }
            }
        } finally {
            fclose($stream);
        }
    }
}
