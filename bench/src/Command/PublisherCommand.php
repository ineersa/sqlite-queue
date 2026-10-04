<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Ineersa\SqliteQueue\Bench\ConcurrentCohort;
use Ineersa\SqliteQueue\Bench\Control;
use Ineersa\SqliteQueue\Bench\ObservedTransport;
use Ineersa\SqliteQueue\Bench\Payload;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\ProbeMessage;
use Ineersa\SqliteQueue\Bench\Recorder;
use Ineersa\SqliteQueue\Bench\Runtime;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Messenger\MessageBusInterface;

final class PublisherCommand extends Command
{
    public function __construct(private readonly MessageBusInterface $bus, private readonly Recorder $recorder, private readonly Control $control, private readonly ObservedTransport $transport)
    {
        parent::__construct('concurrent:publish');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $actor = Runtime::environment('BENCH_ROLE');
        $count = filter_var(Runtime::environment('BENCH_COHORT_MESSAGES'), \FILTER_VALIDATE_INT);
        if (false === $count || !\in_array($count, [ConcurrentCohort::SMOKE_PER_PUBLISHER, ConcurrentCohort::MESSAGES_PER_PUBLISHER], true)) {
            throw new \RuntimeException('Invalid declared publisher cohort.');
        }
        try {
            // Resolve the decorated owning transport before readiness (and its Doctrine readback).
            $this->transport->setPhase(Phase::Warmup);
            foreach ($this->transport->get() as $_) {
                throw new \RuntimeException('Unexpected concurrent baseline inventory.');
            }
            $this->control->send('ready');
            foreach ([0, 1] as $round) {
                if ($this->control->receive() !== 'warmup:'.$round) {
                    throw new \RuntimeException('Publisher warmup release missing.');
                }
                $this->publish(ConcurrentCohort::identity($actor, Phase::Warmup, $round), Phase::Warmup);
                $this->recorder->flush();
                $this->control->send('published:warmup:'.$round);
            }
            if ('release' !== $this->control->receive()) {
                throw new \RuntimeException('Publisher measure release missing.');
            }
            for ($index = 0; $index < $count; ++$index) {
                $this->publish(ConcurrentCohort::identity($actor, Phase::Measure, $index), Phase::Measure);
            }
            $this->recorder->flush();
            $this->control->send('published:measure');
            $this->control->setTimeoutSeconds(ConcurrentCohort::COHORT_TIMEOUT_SECONDS);
            if ('finish' !== $this->control->receive()) {
                throw new \RuntimeException('Publisher finalization release missing.');
            }

            return Command::SUCCESS;
        } finally {
            $this->recorder->flush();
            Runtime::saveJson(Runtime::environment('BENCH_TELEMETRY').'.counters.json', $this->recorder->counters() + ['finalized' => true, 'pid' => getmypid(), 'finalized_ns' => hrtime(true)]);
        }
    }

    private function publish(string $id, Phase $phase): void
    {
        $this->bus->dispatch(new ProbeMessage($id, $phase, Payload::generate($id), false));
    }
}
