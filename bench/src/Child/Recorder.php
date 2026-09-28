<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Child;

use Ineersa\SqliteQueue\Bench\Clock;
use Ineersa\SqliteQueue\Bench\Config;
use Ineersa\SqliteQueue\Bench\SampleStore;

/**
 * Writes one child process's header, samples, errors, and footer to its own sample file.
 *
 * The recorder counts what it writes, so the report can compare observed records with the
 * workload's expected counts instead of trusting either side.
 */
final class Recorder
{
    private SampleStore $store;

    /** @var array<string, int> */
    private array $counts = [];

    /** @var list<array{where: string, class: string, message: string}> */
    private array $errors = [];

    private bool $budgetExceeded = false;

    private int $written = 0;

    private int $originNs;

    public function __construct(
        string $path,
        private string $role,
        private string $argument,
    ) {
        $this->store = new SampleStore($path);
        $this->originNs = Clock::monotonicNs();
    }

    /**
     * @param array<string, mixed> $extra
     */
    public function header(array $extra = []): void
    {
        $this->store->append([
            'kind' => 'header',
            'role' => $this->role,
            'argument' => $this->argument,
            'proc' => \getmypid(),
            'monotonic_origin_ns' => $this->originNs,
            'anchor' => Clock::anchor(),
            'sample_budget' => Config::SAMPLE_BUDGET_PER_PROCESS,
        ] + $extra);
    }

    /**
     * @param array<string, mixed> $record
     */
    public function sample(array $record): void
    {
        $kind = (string) ($record['kind'] ?? 'unknown');

        if ($this->written >= Config::SAMPLE_BUDGET_PER_PROCESS) {
            if (!$this->budgetExceeded) {
                $this->budgetExceeded = true;
                $this->store->append([
                    'kind' => 'budget',
                    'proc' => \getmypid(),
                    'limit' => Config::SAMPLE_BUDGET_PER_PROCESS,
                    'first_dropped_kind' => $kind,
                ]);
            }

            return;
        }

        ++$this->written;
        $this->counts[$kind] = ($this->counts[$kind] ?? 0) + 1;
        $this->store->append($record + ['proc' => \getmypid()]);
    }

    public function error(string $where, \Throwable $error): void
    {
        $this->counts['error'] = ($this->counts['error'] ?? 0) + 1;

        if (\count($this->errors) < 20) {
            $this->errors[] = [
                'where' => $where,
                'class' => $error::class,
                'message' => \substr($error->getMessage(), 0, 500),
            ];
        }

        if ($this->written < Config::SAMPLE_BUDGET_PER_PROCESS) {
            ++$this->written;
            $this->store->append([
                'kind' => 'error',
                'proc' => \getmypid(),
                'where' => $where,
                'class' => $error::class,
                'message' => \substr($error->getMessage(), 0, 500),
            ]);
        }
    }

    /**
     * @param array<string, mixed> $extra
     */
    public function foot(array $extra = []): void
    {
        $this->store->append([
            'kind' => 'foot',
            'proc' => \getmypid(),
            'role' => $this->role,
            'counts' => $this->counts,
            'errors' => $this->errors,
            'rusage' => \getrusage(),
            'budget_exceeded' => $this->budgetExceeded,
            'anchor' => Clock::anchor(),
        ] + $extra);

        $this->store->close();
    }
}
