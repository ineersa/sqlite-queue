<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * Samples CPU time, resident memory, and process count across the owned process tree.
 *
 * Every pid ever observed is remembered, so the report can prove after teardown that no
 * owned process survived, including one that was reparented away from the coordinator.
 */
final class Resources
{
    /** @var array<int, float> last observed CPU seconds per pid */
    private array $lastCpu = [];

    /** @var array<int, int> highest resident set size per pid, in KiB */
    private array $peakRss = [];

    /** @var array<int, true> every pid observed as part of the owned tree */
    private array $observed = [];

    /** @var list<array{wall: float, cpu_seconds: float, rss_kb: int, processes: int}> */
    private array $samples = [];

    private float $cpuSeconds = 0.0;

    private int $peakTreeRssKb = 0;

    private int $peakProcesses = 0;

    public function __construct(private int $rootPid)
    {
    }

    /**
     * Takes one sample of the owned tree and accumulates CPU accounting.
     */
    public function sample(): void
    {
        if (!ProcessTree::available()) {
            return;
        }

        $snapshot = ProcessTree::snapshot();
        $pids = ProcessTree::descendants($this->rootPid, $snapshot);

        $cpuSeconds = 0.0;
        $rssKb = 0;

        foreach ($pids as $pid) {
            $process = $snapshot[$pid];
            $this->observed[$pid] = true;

            $previous = $this->lastCpu[$pid] ?? null;
            if (null !== $previous && $process['cpu_seconds'] >= $previous) {
                $this->cpuSeconds += $process['cpu_seconds'] - $previous;
            }
            $this->lastCpu[$pid] = $process['cpu_seconds'];

            $this->peakRss[$pid] = \max($this->peakRss[$pid] ?? 0, $process['rss_kb']);

            $cpuSeconds += $process['cpu_seconds'];
            $rssKb += $process['rss_kb'];
        }

        $this->peakTreeRssKb = \max($this->peakTreeRssKb, $rssKb);
        $this->peakProcesses = \max($this->peakProcesses, \count($pids));

        $this->samples[] = [
            'wall' => Clock::wall(),
            'cpu_seconds' => $cpuSeconds,
            'rss_kb' => $rssKb,
            'processes' => \count($pids),
        ];
    }

    /**
     * Accumulated CPU seconds of every process the tree ever held.
     */
    public function cpuSeconds(): float
    {
        return $this->cpuSeconds;
    }

    /**
     * CPU seconds consumed between two sampler readings.
     *
     * Used for the idle rate: the tree must look distinct from a busy tree while it waits.
     *
     * @return array{cpu_seconds: float, milliseconds: float, cpu_percent: float, samples: int}
     */
    public function cpuBetween(float $startWall, float $endWall): array
    {
        $first = null;
        $last = null;
        $count = 0;

        foreach ($this->samples as $sample) {
            if ($sample['wall'] < $startWall || $sample['wall'] > $endWall) {
                continue;
            }

            $first ??= $sample;
            $last = $sample;
            ++$count;
        }

        if (null === $first || null === $last || $first === $last) {
            return ['cpu_seconds' => 0.0, 'milliseconds' => 0.0, 'cpu_percent' => 0.0, 'samples' => $count];
        }

        $cpuSeconds = \max(0.0, $last['cpu_seconds'] - $first['cpu_seconds']);
        $milliseconds = ($last['wall'] - $first['wall']) * 1000;

        return [
            'cpu_seconds' => $cpuSeconds,
            'milliseconds' => $milliseconds,
            'cpu_percent' => $milliseconds > 0 ? 100 * $cpuSeconds / ($milliseconds / 1000) : 0.0,
            'samples' => $count,
        ];
    }

    public function peakTreeRssKb(): int
    {
        return $this->peakTreeRssKb;
    }

    public function peakProcesses(): int
    {
        return $this->peakProcesses;
    }

    /**
     * @return list<int>
     */
    public function observedPids(): array
    {
        return \array_map('intval', \array_keys($this->observed));
    }

    /**
     * @return array{root_pid: int, observed_pids: list<int>, peak_processes: int, peak_tree_rss_kb: int, cpu_seconds: float, samples: int}
     */
    public function summary(): array
    {
        return [
            'root_pid' => $this->rootPid,
            'observed_pids' => $this->observedPids(),
            'peak_processes' => $this->peakProcesses,
            'peak_tree_rss_kb' => $this->peakTreeRssKb,
            'cpu_seconds' => $this->cpuSeconds,
            'samples' => \count($this->samples),
        ];
    }
}
