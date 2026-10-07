<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process as ChildProcess;

/** A finite, released cohort. Actor traffic is drained together, never serially by role. */
final readonly class ConcurrentRun
{
    public function __construct(private RunOptionsDTO $options)
    {
    }

    /** @return array<string, mixed> */
    public function run(string $root, string $directory, Backend $backend): array
    {
        $filesystem = new Filesystem();
        $runDeadline = hrtime(true) + ConcurrentCohort::RUN_TIMEOUT_SECONDS * 1000000000;
        $filesystem->mirror(__DIR__.'/config', $directory.'/config');
        $short = '/tmp/sqbench-'.bin2hex(random_bytes(6));
        $filesystem->mkdir($short, 0700);
        $server = stream_socket_server('unix://'.$short.'/control.sock', $code, $error);
        if (false === $server) {
            $filesystem->remove($short);
            throw new \RuntimeException('Concurrent control bind failed: '.$error);
        }
        $units = [];
        foreach (['CLK_TCK', 'PAGESIZE'] as $unit) {
            $probe = new ChildProcess(['getconf', $unit]);
            $probe->mustRun();
            $units[] = (int) trim($probe->getOutput());
        }
        $resources = new Resources(Clock::system(), static fn (string $path): string|false => @file_get_contents($path), static fn (string $bytes): bool => file_put_contents($directory.'/resources.jsonl', $bytes, \FILE_APPEND) === \strlen($bytes), $units[0], $units[1]);
        $resources->register(Role::ObserverPublisher, (int) getmypid());
        $database = $directory.'/queue.sqlite';
        $processes = [];
        $controls = [];
        $durability = [];
        $start = 0;
        $end = 0;
        $failure = '';
        $stage = Phase::Boot;
        $count = $this->options->smoke ? ConcurrentCohort::SMOKE_PER_PUBLISHER : ConcurrentCohort::MESSAGES_PER_PUBLISHER;
        $journal = CohortJournal::create($directory.'/expected-ids.txt');
        foreach (ConcurrentCohort::ids(Phase::Measure, $count) as $id) {
            $journal->append($id);
        }
        $journal->flush();
        try {
            $resources->capture($stage->value);
            if (Backend::Broker === $backend) {
                $broker = new ChildProcess([\PHP_BINARY, $root.'/bin/sqlite-queue', 'broker', '--database='.$database, '--endpoint='.$short.'/broker.sock', '--redeliver-timeout='.Config::REDELIVER_TIMEOUT_S, '--synchronous='.$this->options->synchronous->value, '--no-ansi'], env: Process::environment(), timeout: Config::STARTUP_TIMEOUT_S);
                $processes['broker'] = $broker;
                $broker->start();
                if (!$broker->waitUntil(static fn (string $type, string $data): bool => str_contains($data, '"event":"ready"'))) {
                    throw new \RuntimeException('Concurrent broker readiness missing.');
                }
                $broker->setTimeout(ConcurrentCohort::RUN_TIMEOUT_SECONDS);
                foreach (explode("\n", $broker->getOutput()) as $line) {
                    $ready = json_decode($line, true);
                    if (\is_array($ready) && 'ready' === ($ready['event'] ?? null)) {
                        if (($ready['synchronous_effective'] ?? null) !== $this->options->synchronous->value) {
                            throw new \RuntimeException('Broker durability mismatch.');
                        }
                        $resources->register(Role::Broker, $ready['pid']);
                        $resources->register(Role::Persistence, $ready['persistence_pid']);
                        $durability['broker'] = $ready;
                        Runtime::saveJson($directory.'/broker.ready.json', $ready);
                        break;
                    }
                }
                if (!isset($durability['broker'])) {
                    throw new \RuntimeException('Broker owning identities missing.');
                }
            }
            $environment = ['BENCH_PROJECT' => $directory, 'BENCH_DATABASE' => $database, 'BENCH_DSN' => Backend::Broker === $backend ? 'sqlite-queue://async?endpoint='.$short.'/broker.sock' : 'doctrine-benchmark://async', 'BENCH_RESULT_DSN' => Backend::Broker === $backend ? 'sqlite-queue://results?endpoint='.$short.'/broker.sock' : 'doctrine-benchmark://results', 'BENCH_CONTROL' => $short.'/control.sock', 'BENCH_RUN' => basename(\dirname($directory)), 'BENCH_RECEIVER' => 'async', 'BENCH_SYNCHRONOUS' => $this->options->synchronous->value, 'BENCH_COHORT_MESSAGES' => (string) $count];
            $roles = [Role::Consumer0, Role::Consumer1, Role::Publisher0, Role::Publisher1, Role::Publisher2];
            foreach ($roles as $role) {
                $actor = $role->value;
                $publisher = str_starts_with($actor, 'publisher-');
                $arguments = [\PHP_BINARY, __DIR__.'/console.php', $publisher ? 'concurrent:publish' : 'messenger:consume'];
                if (!$publisher) {
                    $arguments[] = 'async';
                    if (Backend::Doctrine === $backend) {
                        $arguments[] = '--sleep=0.05';
                    }
                }
                $arguments[] = '--no-ansi';
                $arguments[] = '--no-interaction';
                $process = new ChildProcess($arguments, env: Process::environment($environment + ['BENCH_ROLE' => $actor, 'BENCH_TELEMETRY' => $directory.'/'.$actor.'.operations.jsonl']), timeout: ConcurrentCohort::RUN_TIMEOUT_SECONDS);
                $processes[$actor] = $process;
                $process->start();
                $pid = $process->getPid();
                if (!\is_int($pid)) {
                    throw new \RuntimeException('Concurrent actor PID missing.');
                }
                $resources->register($role, $pid);
                $stream = stream_socket_accept($server, Config::STARTUP_TIMEOUT_S);
                if (false === $stream) {
                    throw new \RuntimeException('Concurrent actor acquisition failed: '.$actor.' '.$process->getErrorOutput());
                }
                $control = new Control($stream);
                $controls[$actor] = $control;
                if ('ready' !== $control->receive()) {
                    throw new \RuntimeException('Concurrent actor readiness missing: '.$actor);
                }
            }
            $stage = Phase::Connected;
            $resources->capture($stage->value);
            // Hold both native consumers, then let each process drain its own warmup round.
            foreach (['consumer-0', 'consumer-1'] as $actor) {
                $controls[$actor]->send('hold');
                if ('hold' !== $controls[$actor]->receive()) {
                    throw new \RuntimeException('Consumer hold barrier missing.');
                }
            }
            $stage = Phase::Warmup;
            $warmupStart = hrtime(true);
            $resources->capture($stage->value);
            foreach ([0, 1] as $round) {
                $consumer = 'consumer-'.$round;
                $controls[$consumer]->send('resume');
                if ('resume' !== $controls[$consumer]->receive()) {
                    throw new \RuntimeException('Consumer resume barrier missing.');
                }
                foreach (['publisher-0', 'publisher-1', 'publisher-2'] as $actor) {
                    $controls[$actor]->send('warmup:'.$round);
                }
                $expected = [];
                foreach (['publisher-0', 'publisher-1', 'publisher-2'] as $actor) {
                    $expected[$actor]['published:warmup:'.$round] = true;
                    $expected[$consumer][ConcurrentCohort::identity($actor, Phase::Warmup, $round)] = true;
                }
                $this->collect($controls, $processes, $expected, hrtime(true) + (int) (Config::STARTUP_TIMEOUT_S * 1e9));
                $controls[$consumer]->send('hold');
                if ('hold' !== $controls[$consumer]->receive()) {
                    throw new \RuntimeException('Warmup consumer hold missing.');
                }
            }
            $warmupEnd = hrtime(true);
            foreach (['publisher-0', 'publisher-1', 'publisher-2', 'consumer-0', 'consumer-1'] as $actor) {
                if (Backend::Doctrine === $backend) {
                    $file = $directory.'/'.$actor.'.operations.jsonl.async.durability.json';
                    $readback = json_decode((string) file_get_contents($file), true, flags: \JSON_THROW_ON_ERROR);
                    if (($readback['desired'] ?? null) !== $this->options->synchronous->value || !Baseline::isDurabilityEquivalent($readback['effective'], $this->options->synchronous)) {
                        throw new \RuntimeException('Concurrent owning durability mismatch: '.$actor);
                    }
                    $durability[$actor] = $readback;
                }
            }
            Runtime::saveJson($directory.'/durability.json', $durability);
            $stage = Phase::Reset;
            $resources->capture($stage->value);
            // Consumers remain gated until the finite-cohort timer is armed.
            $stage = Phase::Measure;
            $start = hrtime(true);
            $resources->capture($stage->value);
            foreach (['consumer-0', 'consumer-1'] as $actor) {
                $controls[$actor]->send('resume');
                if ('resume' !== $controls[$actor]->receive()) {
                    throw new \RuntimeException('Measured consumer resume missing.');
                }
            }
            foreach (['publisher-0', 'publisher-1', 'publisher-2'] as $actor) {
                $controls[$actor]->send('release');
            }
            $expected = [];
            foreach (['publisher-0', 'publisher-1', 'publisher-2'] as $actor) {
                $expected[$actor]['published:measure'] = true;
            }
            // Completion IDs may arrive on either consumer. The journal remains disk-backed.
            $this->collect($controls, $processes, $expected, min($runDeadline, $start + ConcurrentCohort::COHORT_TIMEOUT_SECONDS * 1000000000), $count * ConcurrentCohort::PUBLISHERS);
            $end = hrtime(true);
            foreach (['publisher-0', 'publisher-1', 'publisher-2'] as $actor) {
                $controls[$actor]->send('finish');
                $processes[$actor]->wait();
                if (0 !== $processes[$actor]->getExitCode()) {
                    throw new \RuntimeException('Publisher finalization failed: '.$actor);
                }
            }
            $stage = Phase::Drain;
            $resources->capture($stage->value);
            foreach (['consumer-0', 'consumer-1'] as $actor) {
                $controls[$actor]->send(Phase::Drain->value);
                if (Phase::Drain->value !== $controls[$actor]->receive()) {
                    throw new \RuntimeException('Concurrent drain barrier missing.');
                }
            }
            $stage = Phase::Audit;
            $resources->capture($stage->value);
            $audit = new ChildProcess([\PHP_BINARY, $root.'/bin/benchmark', 'audit', $database, $backend->value, '--no-ansi'], env: Process::environment(), timeout: Config::STARTUP_TIMEOUT_S);
            $audit->mustRun();
            Runtime::saveText($directory.'/audit.json', $audit->getOutput());
            $inventory = json_decode($audit->getOutput(), true, flags: \JSON_THROW_ON_ERROR);
            if (0 !== ($inventory['remaining'] ?? -1)) {
                throw new \RuntimeException('Concurrent audit inventory is not empty.');
            }
            Runtime::saveJson($directory.'/warmup-bounds.json', ['start' => $warmupStart, 'end' => $warmupEnd]);
        } catch (\Throwable $error) {
            $failure = $error::class.': '.$error->getMessage();
        } finally {
            $failureStage = $stage->value;
            $resources->capture(Phase::Shutdown->value);
            $deadline = hrtime(true) + (int) (Config::KILL_GRACE_S * 1e9);
            foreach (array_reverse($processes, true) as $actor => $process) {
                try {
                    $process->stop(max(0, ($deadline - hrtime(true)) / 1e9));
                    Runtime::saveText($directory.'/'.$actor.'.stderr.log', $process->getErrorOutput());
                    if (0 !== $process->getExitCode()) {
                        $failure .= ' Actor exit '.$actor.': '.$process->getExitCode().'.';
                    }
                } catch (\Throwable $error) {
                    $failure .= ' Shutdown '.$actor.': '.$error->getMessage();
                }
            }
            foreach ($controls as $control) {
                $control->close();
            }
            fclose($server);
            $cleanup = $resources->cleanup(static fn (int $pid): bool => posix_kill($pid, \SIGKILL), Config::KILL_GRACE_S);
            if ($cleanup['complete']) {
                $filesystem->remove($short);
            } else {
                $failure .= ' Owned cleanup incomplete; next backend blocked.';
            }
            $journal->close();
        }
        $actors = ['publisher-0', 'publisher-1', 'publisher-2', 'consumer-0', 'consumer-1'];
        $terminal = FailureFinalization::terminalFooters($directory, $actors);
        $accounting = FailureFinalization::analyze($directory, $actors, Phase::Measure, $start, $end > 0 ? $end : hrtime(true), false);
        if (0 !== ($accounting['public_errors'] ?? 0) || 0 !== ($accounting['observer_errors'] ?? 0)) {
            $failure .= ' Recorded public-operation or observer errors.';
        }
        if ([] !== $terminal['issues']) {
            $failure .= ' Terminal telemetry incomplete.';
        }
        if (is_file($directory.'/warmup-bounds.json')) {
            $bounds = json_decode((string) file_get_contents($directory.'/warmup-bounds.json'), true, flags: \JSON_THROW_ON_ERROR);
            if (!\is_int($bounds['start'] ?? null) || !\is_int($bounds['end'] ?? null)) {
                throw new \RuntimeException('Warmup bounds are invalid.');
            }
            $events = static function () use ($directory, $actors): \Generator {
                foreach ($actors as $actor) {
                    yield from Recorder::read($directory.'/'.$actor.'.operations.jsonl');
                }
            };
            $warmup = Analysis::build($directory.'/warmup-analysis.sqlite', $events(), ConcurrentCohort::ids(Phase::Warmup, ConcurrentCohort::WARMUP_PER_PUBLISHER), $bounds['start'], $bounds['end'], Phase::Warmup);
            $accounting['warmup_integrity_status'] = $warmup['integrity_status'];
            if (IntegrityStatus::Pass->value !== $warmup['integrity_status']) {
                $failure .= ' Warmup integrity failed.';
            }
        }
        $accounting['terminal_telemetry'] = $terminal;
        $accounting['resources'] = Resources::summarize(Recorder::read($directory.'/resources.jsonl'));
        $accounting['resources']['coverage_details'] = $resources->coverage();
        $accounting['resources']['sampling_policy'] = 'phase boundaries only; exited publishers unavailable at post-drain boundary';
        $accounting['telemetry_footers'] = $terminal['footers'];
        $accounting['aggregated_empty_receives_by_phase'] = [];
        foreach ($terminal['footers'] as $footer) {
            foreach ($footer['empty_receives_by_phase'] ?? [] as $phase => $aggregate) {
                $accounting['aggregated_empty_receives_by_phase'][$phase] = ($accounting['aggregated_empty_receives_by_phase'][$phase] ?? 0) + $aggregate['count'];
            }
        }
        $accounting['receive_counter_scope'] = 'actor phase aggregates, not exact parent-window membership';
        $accounting['measurement_started_ns'] = $start;
        $accounting['final_completion_observed_ns'] = $end;
        $accounting['finite_cohort_completions_per_second'] = isset($accounting['cohort_seconds']) && $accounting['cohort_seconds'] > 0 ? $accounting['unique_completions'] / $accounting['cohort_seconds'] : null;
        if (is_file($directory.'/failure-analysis.sqlite')) {
            $analysis = new \SQLite3($directory.'/failure-analysis.sqlite', \SQLITE3_OPEN_READONLY);
            try {
                $accounting['first_receive_return_ns'] = $analysis->querySingle('SELECT min(delivery) FROM observations');
                $accounting['last_ack_return_ns'] = $analysis->querySingle('SELECT max(ack_return) FROM observations');
            } finally {
                $analysis->close();
            }
        }
        if (is_file($directory.'/audit.json')) {
            $auditResult = json_decode((string) file_get_contents($directory.'/audit.json'), true, flags: \JSON_THROW_ON_ERROR);
            $accounting['audit_remaining'] = $auditResult['remaining'];
        }
        $accounting['owning_connection_durability'] = [] === $durability ? [] : ['desired' => $this->options->synchronous->value, 'effective' => $this->options->synchronous->value, 'connections' => $durability, 'authority' => 'actual owning connections before cohort release'];
        $accounting['configuration'] = $this->options->configuration();
        $accounting['cleanup'] = $cleanup;
        $accounting['failure'] = $failure;
        $accounting['failure_stage'] = '' === $failure ? null : $failureStage;
        $accounting['execution_status'] = '' === $failure ? ExecutionStatus::Complete->value : ExecutionStatus::Failed->value;
        $accounting['rate_interpretation'] = 'finite released cohort through final required ACK; not sustained fixed-window capacity';

        return $accounting;
    }

    /** @param array<string, Control> $controls
     * @param array<string, ChildProcess>        $processes
     * @param array<string, array<string, bool>> $expected
     */
    private function collect(array $controls, array $processes, array $expected, int $deadline, int $completions = 0): void
    {
        while ([] !== $expected || $completions > 0) {
            $remaining = ($deadline - hrtime(true)) / 1e9;
            if ($remaining <= 0) {
                throw new \RuntimeException('Concurrent cohort deadline expired.');
            }
            Control::waitAny(array_values($controls), min(1.0, $remaining));
            foreach ($controls as $actor => $control) {
                while ($control->hasPacket()) {
                    $id = $control->receive();
                    if (isset($expected[$actor][$id])) {
                        unset($expected[$actor][$id]);
                        if ([] === $expected[$actor]) {
                            unset($expected[$actor]);
                        }
                    } elseif ($completions > 0 && str_starts_with($actor, 'consumer-') && str_starts_with($id, Phase::Measure->value.':')) {
                        --$completions;
                    } else {
                        throw new \RuntimeException('Unexpected concurrent packet from '.$actor.': '.$id);
                    }
                }
                $processes[$actor]->checkTimeout();
                if (!$processes[$actor]->isRunning()) {
                    throw new \RuntimeException('Concurrent actor exited before cohort finished: '.$actor);
                }
            }
        }
    }
}
