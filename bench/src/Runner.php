<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportNamesStamp;
use Symfony\Component\Process\Process as ChildProcess;

/** Tiny characterization foundation only. No capacity or full measurement acceptance claims. */
final class Runner
{
    public const WARMUP_MESSAGES = 2;
    public const MEASURED_MESSAGES = 4;
    public const SETTLING_SECONDS = 0.1;
    public const WAKEUP_MESSAGES = 2;

    /** @var \Closure(string, string, Backend): array<string, mixed> */
    private readonly \Closure $executeBackend;

    /** @param \Closure(string): void $report
     * @param ?\Closure(string, string, Backend): array<string, mixed> $executeBackend isolated slot fixture seam; null selects the real backend runtime
     */
    public function __construct(private readonly \Closure $report, private readonly RunOptionsDTO $options, ?\Closure $executeBackend = null)
    {
        $this->executeBackend = $executeBackend ?? (Scenario::Concurrent === $options->scenario ? (new ConcurrentRun($options))->run(...) : $this->runBackend(...));
    }

    public function run(): int
    {
        $root = \dirname(__DIR__, 2);
        $directory = $root.'/var/bench/native-'.gmdate('Ymd-His').'-'.bin2hex(random_bytes(4));
        (new Filesystem())->mkdir($directory, 0700);
        Manifest::save($root, $directory, $this->options);
        $schedule = $this->options->schedule();
        $results = [];
        // Persist every planned slot before executing any backend, including future failures.
        foreach ($schedule as $entry) {
            (new Filesystem())->mkdir($directory.'/'.$entry['id'], 0700);
            Runtime::saveJson($directory.'/'.$entry['id'].'/result.json', $entry + ['execution_status' => ExecutionStatus::Scheduled->value, 'failure' => null]);
        }
        foreach ($schedule as $entry) {
            try {
                $result = ($this->executeBackend)($root, $directory.'/'.$entry['id'], Backend::from($entry['backend']));
            } catch (\Throwable $error) {
                $result = ['execution_status' => ExecutionStatus::Failed->value, 'integrity_status' => IntegrityStatus::Unknown->value, 'accounting_status' => AccountingStatus::Partial->value, 'failure' => $error::class.': '.$error->getMessage()];
            }
            $results[$entry['id']] = $entry + $result;
            Runtime::saveJson($directory.'/'.$entry['id'].'/result.json', $results[$entry['id']]);
            Manifest::recordDurability($directory, $entry['id'], $result['owning_connection_durability'] ?? []);
            if (false === ($result['cleanup']['complete'] ?? false)) {
                break;
            }
        }
        $coverage = match ($this->options->scenario) {
            Scenario::MultiQueue => 'two queues selected in native order by one consumer; isolated arrivals; broker WAIT_ANY, Doctrine polling; exact WAIT counters unavailable',
            Scenario::Concurrent => 'three publishers, two native consumers on one queue; fixed finite cohort; phase boundaries; exact WAIT counters and lower-driver diagnostics unavailable',
            Scenario::Application => 'one execution queue and one result queue, each with a native consumer; bounded workflows; phase-boundary resources; exact WAIT counters and lower-driver diagnostics unavailable',
            default => 'one consumed queue; bounded outstanding messages for application, one in flight otherwise; phase-boundary resources; exact WAIT counters and lower-driver diagnostics unavailable',
        };
        Runtime::saveJson($directory.'/summary.json', ['method' => Config::METHOD_REVISION, 'configuration' => $this->options->configuration(), 'schedule' => $schedule, 'results' => $results, 'coverage' => $coverage]);
        $topology = match ($this->options->scenario) {
            Scenario::MultiQueue => 'One publisher and one stock messenger:consume async results worker. Broker pickup mode: notification-wait-any. Doctrine pickup mode: polling. This topology differs from earlier single-queue polling captures.',
            Scenario::Concurrent => 'Three publisher processes and two native consumers on one queue. Finite cohort release through final required ACK, not sustained capacity.',
            Scenario::Application => 'One publisher, one execution consumer and one result/control consumer.',
            default => 'One publisher and one native consumer.',
        };
        $text = '# Native Messenger '.$this->options->scenario->value."\n\nMode: ".$this->options->configuration()['mode'].'. WAL synchronous: '.$this->options->synchronous->value.'. '.$topology." Same workers for warmup and measurement.\n\n| Run | Execution | Integrity | Accounting | Unique completions |\n| --- | --- | --- | --- | ---: |\n";
        foreach ($results as $id => $result) {
            $text .= '| '.$id.' | '.$result['execution_status'].' | '.$result['integrity_status'].' | '.$result['accounting_status'].' | '.($result['unique_completions'] ?? 'unknown')." |\n";
        }
        if (\in_array(Role::Persistence->value, $results['broker']['resources']['coverage_details']['not_present_roles'] ?? [], true)) {
            $text .= "\nBroker storage runs in-process. The persistence process is not present; broker CPU and memory include local storage.\n";
        }
        if ($this->options->scenario->isPickup()) {
            $text .= "\nThe declared no-publication interval and isolated-arrival pickup cohort are separate. Idle CPU and IO use boundary snapshots, not periodic peaks. Empty receives use fixed per-phase count and active-duration histograms; these are actor-phase counts, not exact parent-window events. Exact WAIT registration and wake counts are unavailable.\n";
        }
        if (Scenario::Application === $this->options->scenario) {
            $text .= "\nApplication publishes at a fixed target of five workflows per second with 100ms handler work. Stalled sends are not followed by catch-up bursts. Actual public attempt and completion counts remain reported.\n";
        }

        if (Scenario::Application === $this->options->scenario) {
            $text .= "\nSynthetic application-shaped workflow, not production or keepalive-failure reproduction. The execution handler synchronously waits for the declared handler milliseconds, dispatches a correlated result through Messenger, and the separate result/control worker handles and ACKs it. Workflow counts require both messages to settle cleanly; total message ACKs are separate. No LLM or network request runs.\n";
        }
        if (Scenario::Retention === $this->options->scenario) {
            $text .= "\nRetention keeps the same registered processes and publisher/consumer connections across all declared cycles. Each memory point follows complete ACK drain, settling and an empty-inventory audit. Raw cycle and resource series stream to disk. Smoke uses two cycles of two messages and is only a control-path check. Non-smoke endpoint deltas and observed ranges are characterization, not a leak-free verdict. Broker PHP memory and live-state gauges remain unavailable.\n";
        }
        $text .= "\nProduct-total resource accounting is partial. Publisher and observer share a process, and exited-process accounting is unavailable. Phase-boundary per-role samples are not complete product totals or continuous peaks.\n";
        $text .= "\nRead summary.json for the frozen schedule, phase-specific rates, finite cohort timing, latency coverage. Failures are retained without retries.\n";
        Runtime::saveText($directory.'/report.md', $text);
        ($this->report)(json_encode(['capture' => $directory, 'method' => Config::METHOD_REVISION, 'mode' => $this->options->configuration()['mode']], \JSON_THROW_ON_ERROR));
        foreach ($results as $result) {
            if (ExecutionStatus::Complete->value !== $result['execution_status'] || IntegrityStatus::Pass->value !== $result['integrity_status'] || AccountingStatus::Complete->value !== $result['accounting_status']) {
                return 1;
            }
        }

        return 0;
    }

    /** @return array<string, mixed> */
    private function runBackend(string $root, string $directory, Backend $backend): array
    {
        $filesystem = new Filesystem();
        $filesystem->mkdir($directory, 0700);
        $filesystem->mirror(__DIR__.'/config', $directory.'/config');
        $short = '/tmp/sqbench-'.bin2hex(random_bytes(6));
        $filesystem->mkdir($short, 0700);
        $database = $directory.'/queue.sqlite';
        $server = stream_socket_server('unix://'.$short.'/control.sock', $code, $error);
        if (false === $server) {
            $filesystem->remove($short);
            throw new \RuntimeException('Control bind failed: '.$error);
        }
        $units = new ChildProcess(['getconf', 'CLK_TCK'], timeout: Config::STARTUP_TIMEOUT_S);
        $units->mustRun();
        $ticks = (int) trim($units->getOutput());
        $units = new ChildProcess(['getconf', 'PAGESIZE'], timeout: Config::STARTUP_TIMEOUT_S);
        $units->mustRun();
        $pageBytes = (int) trim($units->getOutput());
        $resources = new Resources(Clock::system(), static fn (string $path): string|false => @file_get_contents($path), static fn (string $bytes): bool => file_put_contents($directory.'/resources.jsonl', $bytes, \FILE_APPEND) === \strlen($bytes), $ticks, $pageBytes);
        $resources->register(Role::ObserverPublisher, (int) getmypid());
        $phases = [];
        $boundary = static function (Phase $phase) use (&$phases, $resources): int {
            $timestamp = $phases[$phase->value] = hrtime(true);
            $resources->capture($phase->value);

            return $timestamp;
        };
        $boundary(Phase::Boot);
        $broker = new ChildProcess([\PHP_BINARY, $root.'/bin/sqlite-queue', 'broker', '--database='.$database, '--endpoint='.$short.'/broker.sock', '--redeliver-timeout='.Config::REDELIVER_TIMEOUT_S, '--synchronous='.$this->options->synchronous->value, '--no-ansi'], env: Process::environment(), timeout: $this->options->processTimeoutSeconds());
        $receivers = Scenario::MultiQueue === $this->options->scenario ? Scenario::MULTI_QUEUE_RECEIVERS : ['async'];
        $consumerArguments = [\PHP_BINARY, __DIR__.'/console.php', 'messenger:consume', ...$receivers, '--no-ansi', '--no-interaction'];
        if (Backend::Doctrine === $backend) {
            $consumerArguments[] = '--sleep='.($this->options->doctrinePollingMilliseconds / 1000);
        }
        $consumer = new ChildProcess($consumerArguments, timeout: $this->options->processTimeoutSeconds());
        $kernel = new Kernel($directory);
        $execution = ExecutionStatus::Failed;
        $failure = '';
        $accounting = [];
        $durability = []; // No owning-connection evidence exists if acquisition fails.
        $release = [];
        $previousEnvironment = [];
        $resultActors = [];
        $analysisBounds = [];
        try {
            if (Backend::Broker === $backend) {
                $broker->setTimeout(Config::STARTUP_TIMEOUT_S);
                $broker->start();
                $ready = $broker->waitUntil(static fn (string $type, string $data): bool => str_contains($data, '"event":"ready"'));
                if (!$ready) {
                    throw new \RuntimeException('Broker failed readiness: '.$broker->getErrorOutput());
                }
                $broker->setTimeout($this->options->processTimeoutSeconds());
                $readyData = null;
                foreach (explode("\n", $broker->getOutput()) as $line) {
                    $candidate = json_decode($line, true);
                    if (\is_array($candidate) && 'ready' === ($candidate['event'] ?? null)) {
                        $readyData = $candidate;
                        break;
                    }
                }
                if (!\is_array($readyData)) {
                    throw new \RuntimeException('Broker readiness lacks resource identities.');
                }
                if (($readyData['synchronous_effective'] ?? null) !== $this->options->synchronous->value) {
                    throw new \RuntimeException('Broker owning connection synchronous readback does not match the selected mode.');
                }
                $resources->registerBroker($readyData);
                Runtime::saveJson($directory.'/broker.ready.json', $readyData);
            }
            $environment = ['BENCH_PROJECT' => $directory, 'BENCH_DATABASE' => $database, 'BENCH_DSN' => Backend::Broker === $backend ? 'sqlite-queue://async?endpoint='.$short.'/broker.sock' : 'doctrine-benchmark://async', 'BENCH_CONTROL' => $short.'/control.sock', 'BENCH_RUN' => basename(\dirname($directory)), 'BENCH_ROLE' => 'consumer', 'BENCH_TELEMETRY' => $directory.'/consumer.operations.jsonl'];
            $environment['BENCH_SYNCHRONOUS'] = $this->options->synchronous->value;
            $environment['BENCH_RECEIVER'] = 'async';
            $environment['BENCH_RESULT_DSN'] = Backend::Broker === $backend ? 'sqlite-queue://results?endpoint='.$short.'/broker.sock' : 'doctrine-benchmark://results';
            $consumer->setEnv(Process::environment($environment));
            $consumer->start();
            $stream = stream_socket_accept($server, Config::STARTUP_TIMEOUT_S);
            if (false === $stream) {
                throw new \RuntimeException('Consumer control acquisition failed: '.$consumer->getErrorOutput());
            }
            $control = new Control($stream);
            $release[] = $control->close(...);
            if ('ready' !== $control->receive()) {
                throw new \RuntimeException('Consumer readiness missing.');
            }
            $pid = $consumer->getPid();
            if (!\is_int($pid)) {
                throw new \RuntimeException('Consumer PID unavailable.');
            }
            $resources->register(Role::Consumer, $pid);
            $executionControl = $control;
            if (Scenario::Application === $this->options->scenario) {
                [$resultProcess, $control] = $this->startResultConsumer($environment, $short, $directory, $backend);
                $resultActors[] = $resultProcess;
                $release[] = $control->close(...);
                $resultPid = $resultProcess->getPid();
                if (!\is_int($resultPid)) {
                    throw new \RuntimeException('Result consumer PID unavailable.');
                }
                $resources->register(Role::ResultsConsumer, $resultPid);
            }
            foreach ($environment as $key => $value) {
                $previousEnvironment[$key] = getenv($key);
                putenv($key.'='.$value);
            }
            putenv('BENCH_ROLE=publisher');
            putenv('BENCH_TELEMETRY='.$directory.'/publisher.operations.jsonl');
            $kernel->boot();
            $bus = $kernel->getContainer()->get('messenger.default_bus');
            $recorder = $kernel->getContainer()->get(Recorder::class);
            if (!$bus instanceof MessageBusInterface || !$recorder instanceof Recorder) {
                throw new \LogicException('Native services unavailable.');
            }
            $transport = $kernel->getContainer()->get(ObservedTransport::class);
            if (!$transport instanceof ObservedTransport) {
                throw new \LogicException('Publisher transport unavailable.');
            }
            // Acquire the lazy operations socket or Doctrine connection before the baseline.
            foreach ($transport->get() as $_) {
                throw new \RuntimeException('Unexpected baseline inventory.');
            }
            $actorControls = [Role::Consumer->value => $executionControl];
            if (Scenario::Application === $this->options->scenario) {
                $actorControls[Role::ResultsConsumer->value] = $control;
            }
            $snapshot = static function (Phase $phase, array $context = []) use ($actorControls, $resources): void {
                $actors = [];
                foreach ($actorControls as $role => $actorControl) {
                    $id = 'snapshot:'.$phase->value;
                    $actorControl->send($id);
                    $actor = $actorControl->receivePacket();
                    if ($actor['id'] !== $id) {
                        throw new \RuntimeException('Unexpected actor snapshot.');
                    }
                    $actors[$role] = $actor;
                }
                $resources->capture($phase->value, $actors, $context);
            };
            $boundary(Phase::Connected);
            $snapshot(Phase::Connected);
            $warmupStart = $boundary(Phase::Warmup);
            $analysisBounds = ['phase' => Phase::Warmup, 'start' => $warmupStart];
            $warmupJournal = CohortJournal::create($directory.'/warmup-expected-ids.txt');
            $release[] = $warmupJournal->close(...);
            $warmupIds = $this->cohort($bus, $control, Phase::Warmup, self::WARMUP_MESSAGES, $warmupJournal);
            $control->setTimeoutSeconds(Config::STARTUP_TIMEOUT_S);
            $recorder->flush();
            $warmupEnd = $boundary(Phase::Reset);
            $this->phaseBarrier($actorControls, Phase::Reset, hrtime(true) + Config::STARTUP_TIMEOUT_S * 1000000000);
            $durability = $this->owningDurability($directory, $backend, $resources);
            Runtime::saveJson($directory.'/durability.json', $durability);

            Manifest::recordDurability(\dirname($directory), basename($directory), $durability);
            $snapshot(Phase::Reset);
            $journal = CohortJournal::create($directory.'/expected-ids.txt');
            $release[] = $journal->close(...);
            $measurementPhase = $this->options->scenario->isPickup() ? Phase::Pickup : Phase::Measure;
            $idleWindow = [];
            $pickupProofs = [];
            if ($this->options->scenario->isPickup()) {
                $control->send(Phase::Idle->value);
                if (Phase::Idle->value !== $control->receive()) {
                    throw new \RuntimeException('Idle phase barrier failed.');
                }
                $initialIdle = $this->idleBarrier($control, 'interval');
                $idleStart = $boundary(Phase::Idle);
                $idleEnd = $idleStart + (int) ($this->options->idleSeconds() * 1e9);
                $remainingIdle = ($idleEnd - hrtime(true)) / 1e9;
                if ($remainingIdle > 0) {
                    \Amp\delay($remainingIdle);
                }
                $actualIdleEnd = $boundary(Phase::IdleEnd);
                $idleWindow = ['start' => $idleStart, 'end' => $idleEnd, 'actual_end' => $actualIdleEnd, 'readiness' => $initialIdle];
                $control->send(Phase::Pickup->value);
                if (Phase::Pickup->value !== $control->receive()) {
                    throw new \RuntimeException('Pickup phase barrier failed.');
                }
            }
            $start = $boundary($measurementPhase);
            $windowEnd = $start + (int) ((Scenario::Application === $this->options->scenario ? $this->options->applicationSeconds() : $this->options->durationSeconds) * 1e9);
            $analysisBounds = ['phase' => $measurementPhase, 'start' => $start];
            if (Scenario::Application === $this->options->scenario || (!$this->options->smoke && Scenario::Retention !== $this->options->scenario && !$this->options->scenario->isPickup())) {
                $analysisBounds['end'] = $windowEnd;
            }
            $index = 0;
            // Empty means no outstanding delivery. A nonempty ID survives the fixed window into drain.
            $pending = '';
            $finiteCohort = $this->options->smoke || $this->options->scenario->isPickup();
            $cohortCount = $this->options->finiteCohortMessages();

            $fixedPending = [];

            $retention = [];
            if (Scenario::Retention === $this->options->scenario) {
                $warmupEvents = static function () use ($directory): \Generator {
                    foreach (['publisher', 'consumer'] as $role) {
                        yield from Recorder::read($directory.'/'.$role.'.operations.jsonl');
                    }
                };
                $baselineProof = Analysis::build($directory.'/retention-baseline-analysis.sqlite', $warmupEvents(), $warmupIds, $warmupStart, $warmupEnd, Phase::Warmup);
                if (IntegrityStatus::Pass->value !== $baselineProof['integrity_status']) {
                    throw new \RuntimeException('Retention warmup must settle cleanly before matched-state observations.');
                }
                $analysisBounds = ['phase' => Phase::Warmup, 'start' => $warmupStart, 'end' => $warmupEnd];
                $retention = $this->retentionCycles($root, $directory, $database, $backend, $bus, $control, $actorControls, $journal, $recorder, $snapshot, $boundary, $analysisBounds);
                $start = $retention['start_ns'];
            }
            if (Scenario::Application === $this->options->scenario) {
                $fixedPending = $this->application($bus, $control, $journal, $start, $windowEnd);
            }
            while (Scenario::Retention !== $this->options->scenario && Scenario::Application !== $this->options->scenario && (null !== $cohortCount ? $index < $cohortCount : hrtime(true) < $windowEnd)) {
                $id = $measurementPhase->value.':'.$index;
                if ($this->options->scenario->isPickup()) {
                    $pickupProofs[$id] = $this->idleBarrier($control, $id);
                }
                $journal->append($id);
                $this->dispatch($bus, $id, $measurementPhase, $index);
                ++$index;
                $completion = $finiteCohort ? $control->receive() : $control->receiveBefore($windowEnd);
                if (null === $completion) {
                    $pending = $id;
                    break;
                }
                if ($completion !== $id) {
                    throw new \RuntimeException('Unexpected measured completion.');
                }
            }
            $end = Scenario::Retention === $this->options->scenario ? $retention['end_ns'] : (Scenario::Application === $this->options->scenario ? $windowEnd : ($finiteCohort ? hrtime(true) : $windowEnd));
            $drainStarted = $boundary(Phase::Drain);
            $drainDeadline = $drainStarted + (int) ($this->options->drainTimeoutSeconds() * 1e9);
            $control->setDeadline($drainDeadline);
            while ([] !== $fixedPending) {
                $control->setDeadline($drainDeadline);
                $id = $this->completionId($control->receive());
                if (!isset($fixedPending[$id])) {
                    throw new \RuntimeException('Unexpected application drain completion.');
                }
                unset($fixedPending[$id]);
            }
            if ('' !== $pending && $control->receive() !== $pending) {
                throw new \RuntimeException('Outstanding cohort completion missing.');
            }
            $control->setDeadline($drainDeadline);
            $this->phaseBarrier($actorControls, Phase::Drain, $drainDeadline);
            $drainEnded = hrtime(true);
            $control->setTimeoutSeconds(Config::STARTUP_TIMEOUT_S);
            $journal->flush();
            $boundary(Phase::Settle);
            // Observation delay is outside measure and drain, not a retention experiment.
            \Amp\delay(self::SETTLING_SECONDS);
            $snapshot(Phase::Settle);
            $recorder->flush();
            Runtime::saveText($directory.'/publisher.operations.jsonl.counters.json', json_encode($recorder->counters(), \JSON_THROW_ON_ERROR));
            $boundary(Phase::Audit);
            // A separate observer opens SQL only after measure and drain. Publisher and consumer never do.
            $audit = new ChildProcess([\PHP_BINARY, $root.'/bin/benchmark', 'audit', $database, $backend->value, '--no-ansi', '--no-interaction'], env: Process::environment(), timeout: Config::STARTUP_TIMEOUT_S);
            $audit->mustRun();
            Runtime::saveText($directory.'/audit.json', $audit->getOutput());
            $inventory = json_decode($audit->getOutput(), true, flags: \JSON_THROW_ON_ERROR);
            if (!\is_array($inventory) || !\is_int($inventory['remaining'] ?? null)) {
                throw new \RuntimeException('Audit returned invalid inventory.');
            }
            $remaining = $inventory['remaining'];
            $telemetryRoles = Scenario::Application === $this->options->scenario ? ['publisher', 'consumer', 'results'] : ['publisher', 'consumer'];
            $events = static function () use ($directory, $telemetryRoles): \Generator {
                foreach ($telemetryRoles as $role) {
                    yield from Recorder::read($directory.'/'.$role.'.operations.jsonl');
                }
            };
            $accounting = Analysis::build($directory.'/analysis.sqlite', $events(), $this->expectedMessages($journal->ids()), $start, $end, $measurementPhase);
            $warmup = Analysis::build($directory.'/warmup-analysis.sqlite', $events(), $this->expectedMessages($warmupIds), $warmupStart, $warmupEnd, Phase::Warmup);

            if (Scenario::Application === $this->options->scenario) {
                $accounting = WorkflowAnalysis::apply($directory.'/analysis.sqlite', $accounting, $start, $end);
                $warmup = WorkflowAnalysis::apply($directory.'/warmup-analysis.sqlite', $warmup, $warmupStart, $warmupEnd);
                $accounting['application'] = $this->options->configuration();
            }
            if (Scenario::Retention === $this->options->scenario) {
                $accounting['retention'] = RetentionAnalysis::summarize(Recorder::read($directory.'/resources.jsonl'));
                $accounting['retention']['cycle_execution'] = $retention;
                $accounting['retention']['configuration'] = $this->options->configuration();
            }
            $accounting['measurement_phase'] = $measurementPhase->value;
            $accounting['rate_interpretation'] = Scenario::Retention === $this->options->scenario ? 'cycle sequence wall time includes settling, audits and observation; not capacity evidence' : ($this->options->scenario->isPickup() ? 'finite isolated-arrival pickup cohort, separate from empty interval' : 'measured cohort; fixed-window goodput only outside smoke');
            if ($this->options->scenario->isPickup()) {
                $accounting['idle'] = IdleMetrics::summarize($events(), $idleWindow['start'], $idleWindow['end']);
                $accounting['idle']['declared_seconds'] = $this->options->idleSeconds();
                $accounting['idle']['actual_resource_boundary_end_ns'] = $idleWindow['actual_end'];
                $accounting['idle']['readiness'] = $idleWindow['readiness'];
                $accounting['idle']['observer_coverage'] = 'fixed per-phase empty receive aggregation; full failures and actual work records; idle-specific observer perturbation has not been isolated';
                $accounting['pickup'] = ['count' => self::WAKEUP_MESSAGES, 'readiness' => $pickupProofs, 'coverage' => 'isolated arrivals after prior ACK and worker idle; no backlog latency or idle-interval goodput claim'];
                if (Scenario::MultiQueue === $this->options->scenario) {
                    $accounting['pickup']['mode'] = Backend::Broker === $backend ? 'notification-wait-any' : 'polling';
                    $accounting['pickup']['queues'] = Scenario::MULTI_QUEUE_RECEIVERS;
                }
                if (IntegrityStatus::Pass->value !== $accounting['idle']['integrity_status']) {
                    $accounting['integrity_status'] = IntegrityStatus::Fail->value;
                }
            }

            $accounting['drain_seconds'] = ($drainEnded - $drainStarted) / 1e9;
            $accounting['fixed_window_end_ns'] = $end;
            $accounting['measurement_started_ns'] = $start;
            $accounting['actual_drain_started_ns'] = $drainStarted;
            $accounting['warmup_integrity_status'] = $warmup['integrity_status'];
            if (IntegrityStatus::Pass->value !== $warmup['integrity_status']) {
                $accounting['integrity_status'] = IntegrityStatus::Fail->value;
            }
            $worker = json_decode((string) file_get_contents($directory.'/consumer.operations.jsonl.worker.json'), true, flags: \JSON_THROW_ON_ERROR);
            if (Backend::Broker === $backend && \is_array($worker) && isset($worker['idle_timeout_microseconds']) && 0 !== $worker['idle_timeout_microseconds']) {
                throw new \RuntimeException('Native broker WAIT integration did not suppress polling sleep.');
            }
            $accounting['native_worker'] = $worker;
            if (Scenario::Application === $this->options->scenario) {
                $resultWorker = json_decode((string) file_get_contents($directory.'/results.operations.jsonl.worker.json'), true, flags: \JSON_THROW_ON_ERROR);
                if (Backend::Broker === $backend && (!\is_array($resultWorker) || 0 !== ($resultWorker['idle_timeout_microseconds'] ?? null))) {
                    throw new \RuntimeException('Result consumer native WAIT integration unavailable.');
                }
                $accounting['native_result_worker'] = $resultWorker;
            }
            $accounting['audit_remaining'] = $remaining;
            $accounting['integrity_status'] = 0 === $remaining ? $accounting['integrity_status'] : IntegrityStatus::Fail->value;
            $accounting['accounting_status'] = AccountingStatus::Complete->value;
            if (0 !== $accounting['observer_errors']) {
                $accounting['accounting_status'] = AccountingStatus::Partial->value;
            }
            $accounting['telemetry_footers'] = [];
            foreach (Scenario::Application === $this->options->scenario ? ['publisher', 'consumer', 'results'] : ['publisher', 'consumer'] as $role) {
                $counters = json_decode((string) file_get_contents($directory.'/'.$role.'.operations.jsonl.counters.json'), true, flags: \JSON_THROW_ON_ERROR);
                $accounting['telemetry_footers'][$role] = $counters;
                if (!\is_array($counters) || 0 !== ($counters['lost_records'] ?? -1) || 0 !== ($counters['write_failures'] ?? -1)) {
                    $accounting['accounting_status'] = AccountingStatus::Partial->value;
                }
            }
            $execution = ExecutionStatus::Complete;
            $accounting['empty_receive_latency_coverage'] = 'detailed operation spans retained';

            if (0 !== $accounting['public_errors'] || 0 !== $accounting['observer_errors'] || 0 !== $warmup['public_errors'] || 0 !== $warmup['observer_errors']) {
                $execution = ExecutionStatus::Failed;
                $failure = 'Recorded public-operation or observer errors.';
            }
        } catch (\Throwable $e) {
            $failure = $e::class.': '.$e->getMessage();
        } finally {
            $boundary(Phase::Shutdown);
            if (isset($recorder)) {
                try {
                    $recorder->flush();
                    Runtime::saveJson($directory.'/publisher.operations.jsonl.counters.json', $recorder->counters() + ['finalized' => true, 'pid' => getmypid(), 'finalized_ns' => hrtime(true)]);
                } catch (\Throwable $error) {
                    $execution = ExecutionStatus::Failed;
                    $failure .= ' Publisher telemetry finalization: '.$error::class.': '.$error->getMessage();
                }
            }
            $shutdownDeadline = hrtime(true) + (int) (Config::KILL_GRACE_S * 1e9);
            foreach ([$consumer->stop(...), ...array_map(static fn (ChildProcess $process): \Closure => $process->stop(...), $resultActors), static fn () => $kernel->shutdown(), $broker->stop(...)] as $stop) {
                try {
                    $remaining = ($shutdownDeadline - hrtime(true)) / 1e9;
                    $stop($remaining > 0 ? $remaining : 0);
                } catch (\Throwable $error) {
                    $execution = ExecutionStatus::Failed;
                    $failure .= ' Shutdown: '.$error::class.': '.$error->getMessage();
                }
            }
            if ($consumer->isStarted() && 0 !== $consumer->getExitCode()) {
                $execution = ExecutionStatus::Failed;
                $failure .= ' Consumer shutdown failed.';
            }
            if (Backend::Broker === $backend && $broker->isStarted() && 0 !== $broker->getExitCode()) {
                $execution = ExecutionStatus::Failed;
                $failure .= ' Broker shutdown failed.';
            }
            foreach ($release as $close) {
                try {
                    $close();
                } catch (\Throwable $error) {
                    $execution = ExecutionStatus::Failed;
                    $failure .= ' Cleanup: '.$error::class.': '.$error->getMessage();
                }
            }
            foreach ($resultActors as $process) {
                if (0 !== $process->getExitCode()) {
                    $execution = ExecutionStatus::Failed;
                    $failure .= ' Result consumer shutdown failed.';
                }
                try {
                    Runtime::saveText($directory.'/results.stderr.log', $process->getErrorOutput());
                } catch (\Throwable $error) {
                    $execution = ExecutionStatus::Failed;
                    $failure .= ' Result stderr preservation: '.$error::class.': '.$error->getMessage();
                }
            }
            fclose($server);
            $resultWorkersStopped = true;
            foreach ($resultActors as $process) {
                if ($process->isRunning()) {
                    $resultWorkersStopped = false;
                }
            }
            $cleanup = $resources->cleanup(static fn (int $pid): bool => posix_kill($pid, \SIGKILL), Config::KILL_GRACE_S);
            if (!$cleanup['complete']) {
                $execution = ExecutionStatus::Failed;
                $failure .= ' Owned process cleanup incomplete; endpoint retained and subsequent runs blocked.';
            }
            if ($cleanup['complete'] && !$consumer->isRunning() && !$broker->isRunning() && $resultWorkersStopped) {
                try {
                    $filesystem->remove($short);
                } catch (\Throwable $error) {
                    $execution = ExecutionStatus::Failed;
                    $failure .= ' Endpoint cleanup: '.$error::class.': '.$error->getMessage();
                }
            }
            foreach ($previousEnvironment as $name => $value) {
                putenv(false === $value ? $name : $name.'='.$value);
            }
            $phases['shutdown_complete'] = hrtime(true);
            $artifacts = ['consumer.stderr.log' => $consumer->isStarted() ? $consumer->getErrorOutput() : '', 'broker.stderr.log' => $broker->isStarted() ? $broker->getErrorOutput() : '', 'phases.json' => json_encode($phases, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR)];
            foreach ($artifacts as $path => $content) {
                try {
                    Runtime::saveText($directory.'/'.$path, $content);
                } catch (\Throwable $error) {
                    $execution = ExecutionStatus::Failed;
                    $failure .= ' Artifact '.$path.': '.$error::class.': '.$error->getMessage();
                }
            }
        }
        $roles = Scenario::Application === $this->options->scenario ? ['publisher', 'consumer', 'results'] : ['publisher', 'consumer'];
        $terminal = FailureFinalization::terminalFooters($directory, $roles);
        $accounting['terminal_telemetry'] = $terminal;
        $accounting['telemetry_footers'] = $terminal['footers'];
        if ([] !== $terminal['issues']) {
            $execution = ExecutionStatus::Failed;
            $failure .= ' Terminal telemetry is incomplete.';
            $accounting['accounting_status'] = AccountingStatus::Partial->value;
        }
        if ('' !== $failure || ExecutionStatus::Failed === $execution) {
            $phase = $analysisBounds['phase'] ?? Phase::Measure;
            $startBound = $analysisBounds['start'] ?? 0;
            $endBound = $analysisBounds['end'] ?? ($phases['shutdown'] ?? 0);
            $recovered = FailureFinalization::analyze($directory, $roles, $phase, $startBound, $endBound, Scenario::Application === $this->options->scenario);
            $accounting = array_replace($accounting, $recovered);
        }
        try {
            $accounting['resources'] = Resources::summarize(Recorder::read($directory.'/resources.jsonl'));
            if (Scenario::Retention === $this->options->scenario && is_file($directory.'/resources.jsonl')) {
                $accounting['retention'] = ($accounting['retention'] ?? []) + RetentionAnalysis::summarize(Recorder::read($directory.'/resources.jsonl'));
                $accounting['retention']['configuration'] = $this->options->configuration();
            }
        } catch (\Throwable $error) {
            $accounting['resources'] = ['coverage' => 'unavailable: '.$error::class.': '.$error->getMessage()];
            $accounting['accounting_status'] = AccountingStatus::Partial->value;
        }
        if (isset($accounting['idle'])) {
            $accounting['idle']['resources_by_role'] = $accounting['resources']['phase_roles'][Phase::Idle->value] ?? [];
            $accounting['idle']['resource_coverage'] = 'CPU and IO differences between idle and idle-end role snapshots; boundaries may bracket the declared clock window imperfectly; no periodic memory peak';
        }
        $emptyPhases = [];
        foreach ($terminal['footers'] as $footer) {
            foreach ($footer['empty_receives_by_phase'] ?? [] as $phase => $aggregate) {
                $emptyPhases[$phase] = ($emptyPhases[$phase] ?? 0) + $aggregate['count'];
            }
        }
        $accounting['aggregated_empty_receives_by_phase'] = $emptyPhases;
        $accounting['receive_counter_scope'] = 'actor phase counts; empty phase sampled at receive entry; error and nonempty records retain boundaries; not exact parent clock-window membership';
        $accounting['receive_empty'] = ($accounting['receive_empty'] ?? 0) + ($emptyPhases[Phase::Measure->value] ?? 0);
        $accounting['receive_attempts'] = ($accounting['receive_attempts'] ?? 0) + ($emptyPhases[Phase::Measure->value] ?? 0);
        if (isset($accounting['idle'])) {
            $accounting['idle']['phase_empty_receive_aggregates_by_role'] = [];
            foreach ($terminal['footers'] as $role => $footer) {
                $accounting['idle']['phase_empty_receive_aggregates_by_role'][$role] = $footer['empty_receives_by_phase'][Phase::Idle->value] ?? null;
            }
            $accounting['idle']['phase_empty_receives'] = $emptyPhases[Phase::Idle->value] ?? 0;
            $accounting['idle']['receive_empty'] = null;
            $accounting['idle']['receive_attempts'] = null;
            $accounting['idle']['receive_counter_coverage'] = 'empty counts are exact for actor idle phase, not parent [start,end); detailed errors retain their return boundaries; footer includes first/last receive boundaries and active duration histogram';
        }
        $accounting['resources']['coverage_details'] = $resources->coverage();
        $accounting['resources']['sampling_policy'] = 'always-on phase boundaries only';
        $accounting['resources']['actor_php_policy'] = 'consumer connected, post-warmup and post-drain snapshots; coordinator current at each boundary; broker and persistence unavailable';
        if (0 !== $resources->coverage()['lost_samples']) {
            $accounting['accounting_status'] = AccountingStatus::Partial->value;
        }
        $accounting['settling_seconds'] = Scenario::Retention === $this->options->scenario ? RunOptionsDTO::SETTLING_SECONDS : self::SETTLING_SECONDS;
        $accounting['settling_coverage'] = Scenario::Retention === $this->options->scenario ? ($this->options->smoke ? 'tiny smoke control-path check only; not retention characterization' : 'audited-empty cycle series, same persistent identities, no forced GC; characterization, not leak-free proof') : 'single bounded post-drain observation, not retention proof';
        $result = $accounting + ['integrity_status' => IntegrityStatus::Unknown->value, 'accounting_status' => AccountingStatus::Partial->value];
        $result['synchronous_desired'] = $this->options->synchronous->value;
        $result['owning_connection_durability'] = $durability;
        $result['configuration'] = $this->options->configuration();
        $result['execution_status'] = $execution->value;
        $result['cleanup'] = $cleanup;
        $result['failure'] = $failure;
        $result['failure_stage'] = '' === $failure ? null : 'unknown';
        try {
            Runtime::saveText($directory.'/result.json', json_encode($result, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR));
        } catch (\Throwable $error) {
            throw new \RuntimeException($failure.' Result preservation failed: '.$error::class.': '.$error->getMessage(), previous: $error);
        }

        return $result;
    }

    /** @param array<string, Control> $actorControls
     * @param \Closure(Phase, array<string, mixed>): void  $snapshot
     * @param \Closure(Phase): int                         $boundary
     * @param array{phase?: Phase, start?: int, end?: int} $analysisBounds actual observed measurement bounds, updated even on failure
     *
     * @return array<string, mixed>
     */
    private function retentionCycles(string $root, string $directory, string $database, Backend $backend, MessageBusInterface $bus, Control $control, array $actorControls, CohortJournal $journal, Recorder $recorder, \Closure $snapshot, \Closure $boundary, array &$analysisBounds): array
    {
        $stream = fopen($directory.'/cycles.jsonl', 'xb');
        if (false === $stream) {
            throw new \RuntimeException('Cannot acquire cycle ledger.');
        }
        $historical = 0;
        $measureSeconds = 0.0;
        $drainSeconds = 0.0;
        $measureBounds = [];
        $clock = Clock::system();
        try {
            for ($cycle = 0; $cycle <= $this->options->effectiveCycles(); ++$cycle) {
                $measureStart = $clock->now();
                $measureEnd = $measureStart;
                $drainStart = $measureStart;
                $drainEnd = $measureStart;
                if ($cycle > 0) {
                    $this->phaseBarrier($actorControls, Phase::Measure, hrtime(true) + Config::STARTUP_TIMEOUT_S * 1000000000);
                    $measureStart = $boundary(Phase::Measure);
                    if (1 === $cycle) {
                        $measureBounds['start'] = $measureStart;
                    }
                    try {
                        for ($index = 0; $index < $this->options->effectiveCycleMessages(); ++$index) {
                            $control->setTimeoutSeconds(Config::STARTUP_TIMEOUT_S);
                            $id = 'cycle:'.$cycle.':'.$index;
                            $journal->append($id);
                            $this->dispatch($bus, $id, Phase::Measure, $index);
                            if ($control->receive() !== $id) {
                                throw new \RuntimeException('Retention completion mismatch.');
                            }
                        }
                    } finally {
                        $measureEnd = $measureBounds['end'] = $clock->now();
                        $analysisBounds = ['phase' => Phase::Measure, 'start' => $measureBounds['start'], 'end' => $measureEnd];
                    }
                    $drainStart = $boundary(Phase::Drain);
                    $this->phaseBarrier($actorControls, Phase::Drain, $drainStart + (int) ($this->options->drainTimeoutSeconds() * 1e9));
                    $drainEnd = hrtime(true);
                    $recorder->flush();
                    $journal->flush();
                    // Exact ACK correlations control the cycle; one offline pass verifies all cohorts.
                    $historical += $this->options->effectiveCycleMessages();
                    $measureSeconds += ($measureEnd - $measureStart) / 1e9;
                    $drainSeconds += ($drainEnd - $drainStart) / 1e9;
                }
                $settleStart = $boundary(Phase::Settle);
                \Amp\delay(RunOptionsDTO::SETTLING_SECONDS);
                $auditStart = $boundary(Phase::Audit);
                $inventory = $this->inventory($root, $database, $backend);
                $equivalent = 0 === $inventory['remaining'] && 0 === $inventory['ready'] && 0 === $inventory['inflight'];
                $context = ['retention_cycle' => $cycle, 'historical_completions' => $historical, 'warmup_completed_messages' => self::WARMUP_MESSAGES, 'lifetime_completed_messages' => self::WARMUP_MESSAGES + $historical, 'historical_scope' => 'measured cycles; excludes fixed warmup', 'equivalent_empty_point' => $equivalent, 'inventory' => $inventory, 'topology' => 'same publisher, consumer and registered storage process identities'];
                $snapshot(Phase::Settle, $context);
                $record = $context + ['measure_start_ns' => $measureStart, 'measure_end_ns' => $measureEnd, 'drain_start_ns' => $drainStart, 'drain_end_ns' => $drainEnd, 'settle_start_ns' => $settleStart, 'audit_start_ns' => $auditStart, 'observation_ns' => hrtime(true), 'declared_messages' => 0 === $cycle ? 0 : $this->options->effectiveCycleMessages()];
                $line = json_encode($record, \JSON_THROW_ON_ERROR)."\n";
                if (fwrite($stream, $line) !== \strlen($line)) {
                    throw new \RuntimeException('Cycle ledger write failed.');
                }
                if (!$equivalent) {
                    throw new \RuntimeException('Retention point is not a clean audited-empty state.');
                }
            }
        } finally {
            fclose($stream);
        }

        return ['completed_cycles' => $this->options->effectiveCycles(), 'historical_completions' => $historical, 'start_ns' => $measureBounds['start'], 'end_ns' => $measureBounds['end'], 'active_measure_seconds' => $measureSeconds, 'active_drain_seconds' => $drainSeconds, 'cycle_series' => 'cycles.jsonl', 'observation_series' => 'resources.jsonl', 'observer_analysis' => 'one streaming offline pass after all declared cycles; per-cycle state uses exact ACK correlations and post-drain inventory'];
    }

    /** @return array<string, mixed> */
    private function inventory(string $root, string $database, Backend $backend): array
    {
        $audit = new ChildProcess([\PHP_BINARY, $root.'/bin/benchmark', 'audit', $database, $backend->value, '--no-ansi', '--no-interaction'], env: Process::environment(), timeout: Config::STARTUP_TIMEOUT_S);
        $audit->mustRun();
        $inventory = json_decode($audit->getOutput(), true, flags: \JSON_THROW_ON_ERROR);
        if (!\is_array($inventory)) {
            throw new \RuntimeException('Inventory audit must return an object.');
        }
        foreach (['remaining', 'ready', 'inflight'] as $field) {
            if (!\is_int($inventory[$field] ?? null)) {
                throw new \RuntimeException('Inventory lacks integer '.$field.'.');
            }
        }

        return $inventory;
    }

    /** @param array<string, string> $environment
     * @return array{ChildProcess, Control}
     */
    private function startResultConsumer(array $environment, string $short, string $directory, Backend $backend): array
    {
        $server = stream_socket_server('unix://'.$short.'/results-control.sock', $code, $error);
        if (false === $server) {
            throw new \RuntimeException('Result control bind failed: '.$error);
        }
        $environment['BENCH_CONTROL'] = $short.'/results-control.sock';
        $environment['BENCH_RECEIVER'] = 'results';
        $environment['BENCH_ROLE'] = Role::ResultsConsumer->value;
        $environment['BENCH_TELEMETRY'] = $directory.'/results.operations.jsonl';
        $arguments = [\PHP_BINARY, __DIR__.'/console.php', 'messenger:consume', 'results', '--no-ansi', '--no-interaction'];
        if (Backend::Doctrine === $backend) {
            $arguments[] = '--sleep='.($this->options->doctrinePollingMilliseconds / 1000);
        }
        $process = new ChildProcess($arguments, env: Process::environment($environment), timeout: $this->options->processTimeoutSeconds());
        try {
            $process->start();
            $stream = stream_socket_accept($server, Config::STARTUP_TIMEOUT_S);
            if (false === $stream) {
                throw new \RuntimeException('Result consumer acquisition failed: '.$process->getErrorOutput());
            }
            $control = new Control($stream);
            try {
                if ('ready' !== $control->receive()) {
                    throw new \RuntimeException('Result consumer readiness missing.');
                }
            } catch (\Throwable $error) {
                $control->close();
                throw $error;
            }

            return [$process, $control];
        } catch (\Throwable $error) {
            $process->stop(Config::KILL_GRACE_S);
            throw $error;
        } finally {
            fclose($server);
        }
    }

    /** @param array<string, Control> $controls */
    private function phaseBarrier(array $controls, Phase $phase, int $deadline): void
    {
        foreach ($controls as $control) {
            $control->setDeadline($deadline);
            $control->send($phase->value);
            if ($phase->value !== $control->receive()) {
                throw new \RuntimeException('Worker phase barrier failed: '.$phase->value);
            }
        }
    }

    private function completionId(string $id): string
    {
        if (Scenario::Application !== $this->options->scenario) {
            return $id;
        }
        if (!str_ends_with($id, WorkflowAnalysis::RESULT_SUFFIX)) {
            throw new \RuntimeException('Application completion must identify a result.');
        }

        return substr($id, 0, -\strlen(WorkflowAnalysis::RESULT_SUFFIX));
    }

    /** @param iterable<string> $roots
     * @return \Generator<int, string>
     */
    private function expectedMessages(iterable $roots): \Generator
    {
        foreach ($roots as $root) {
            yield $root;
            if (Scenario::Application === $this->options->scenario) {
                yield $root.WorkflowAnalysis::RESULT_SUFFIX;
            }
        }
    }

    /** @return list<string> */
    private function cohort(MessageBusInterface $bus, Control $control, Phase $phase, int $count, CohortJournal $journal): array
    {
        $ids = [];
        $deadline = hrtime(true) + (int) ($this->options->warmupTimeoutSeconds() * 1e9);
        for ($i = 0; $i < $count; ++$i) {
            $control->setDeadline($deadline);
            $id = $phase->value.':'.$i;
            $ids[] = $id;
            $journal->append($id);
            $this->dispatch($bus, $id, $phase, $i);
            $control->setDeadline($deadline);
            if ($this->completionId($control->receive()) !== $id) {
                throw new \RuntimeException('Unexpected completion correlation.');
            }
        }

        return $ids;
    }

    /** @return array<string, true> */
    private function application(MessageBusInterface $bus, Control $control, CohortJournal $journal, int $start, int $end): array
    {
        $next = $start;
        $index = 0;
        $pending = [];
        $interval = (int) (1e9 / RunOptionsDTO::APPLICATION_RATE);
        while (hrtime(true) < $end) {
            while ($control->hasPacket()) {
                $id = $this->completionId($control->receive());
                if (!isset($pending[$id])) {
                    throw new \RuntimeException('Unexpected application completion.');
                }
                unset($pending[$id]);
            }
            if (hrtime(true) >= $next && \count($pending) < RunOptionsDTO::APPLICATION_CAPACITY) {
                $id = 'measure:'.$index;
                $journal->append($id);
                $pending[$id] = true;
                $this->dispatch($bus, $id, Phase::Measure, $index++);
                // Never catch up with a burst after a stalled synchronous send.
                $next = hrtime(true) + $interval;
            }
            $wait = (min($end, $next) - hrtime(true)) / 1e9;
            if ($wait > 0) {
                $control->wait($wait);
            } elseif (\count($pending) >= RunOptionsDTO::APPLICATION_CAPACITY) {
                $control->wait(min(0.1, ($end - hrtime(true)) / 1e9));
            }
        }

        return $pending;
    }

    private function dispatch(MessageBusInterface $bus, string $id, Phase $phase, int $index): void
    {
        $payload = Payload::generate($id);
        $bus->dispatch(Scenario::Application === $this->options->scenario
            ? new ApplicationMessage($id, $phase, $payload, true, workMilliseconds: RunOptionsDTO::HANDLER_MILLISECONDS)
            : new ProbeMessage($id, $phase, $payload, false),
            Scenario::MultiQueue === $this->options->scenario ? [new TransportNamesStamp([Scenario::MULTI_QUEUE_RECEIVERS[$index % \count(Scenario::MULTI_QUEUE_RECEIVERS)]])] : [],
        );
    }

    /** @return array<string, mixed> */
    private function idleBarrier(Control $control, string $correlation): array
    {
        $id = 'arm-idle:'.$correlation;
        $control->send($id);
        $packet = $control->receivePacket();
        if ($packet['id'] !== $id || true !== ($packet['worker_idle'] ?? null)) {
            throw new \RuntimeException('Consumer did not establish idle readiness.');
        }

        return $packet;
    }

    /** @return array<string, mixed> */
    private function owningDurability(string $directory, Backend $backend, Resources $resources): array
    {
        $desired = $this->options->synchronous->value;
        if (Backend::Broker === $backend) {
            $ready = json_decode((string) file_get_contents($directory.'/broker.ready.json'), true, flags: \JSON_THROW_ON_ERROR);
            if (($ready['synchronous_effective'] ?? null) !== $desired) {
                throw new \RuntimeException('Broker owning connection readback mismatch.');
            }

            $execution = $ready['storage_execution'] ?? null;
            $authority = match ($execution) {
                'fabpot' => 'driver connection configuration readback before readiness',
                'in_process' => 'broker in-process configuration readback before readiness',
                null => 'broker worker configuration readback before readiness',
                default => throw new \RuntimeException('Unknown broker storage execution topology.'),
            };

            return ['desired' => $desired, 'effective' => $desired, 'authority' => $authority, 'pid' => $resources->registeredPid('in_process' === $execution ? Role::Broker : Role::Persistence)];
        }
        $expected = ['publisher.operations.jsonl.async.durability.json', 'consumer.operations.jsonl.async.durability.json'];
        if (Scenario::Application === $this->options->scenario) {
            $expected[] = 'results.operations.jsonl.results.durability.json';
        }
        foreach ($expected as $file) {
            if (!is_file($directory.'/'.$file)) {
                throw new \RuntimeException('Missing owning Doctrine connection readback: '.$file);
            }
        }
        $connections = [];
        $files = glob($directory.'/*.durability.json');
        if (false === $files) {
            throw new \RuntimeException('Cannot enumerate owning connection readbacks.');
        }
        foreach ($files as $file) {
            $readback = json_decode((string) file_get_contents($file), true, flags: \JSON_THROW_ON_ERROR);
            if (($readback['desired'] ?? null) !== $desired || !Baseline::isDurabilityEquivalent($readback['effective'], $this->options->synchronous)) {
                throw new \RuntimeException('Doctrine owning connection readback mismatch: '.basename($file));
            }
            $connections[basename($file)] = $readback;
        }

        return ['desired' => $desired, 'effective' => $desired, 'connections' => $connections, 'authority' => 'actual Doctrine transport owning connections before measurement'];
    }
}
