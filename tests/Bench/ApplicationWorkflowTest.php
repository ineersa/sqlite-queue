<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Analysis;
use Ineersa\SqliteQueue\Bench\ApplicationMessage;
use Ineersa\SqliteQueue\Bench\Clock;
use Ineersa\SqliteQueue\Bench\Command\RunCommand;
use Ineersa\SqliteQueue\Bench\Handler;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\ProbeMessage;
use Ineersa\SqliteQueue\Bench\Recorder;
use Ineersa\SqliteQueue\Bench\ResultMessage;
use Ineersa\SqliteQueue\Bench\WorkflowAnalysis;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;

final class ApplicationWorkflowTest extends TestCase
{
    public function testExecutionDispatchesCorrelatedResultThroughBusAndRoutesAreSiblings(): void
    {
        $bus = $this->createMock(MessageBusInterface::class);
        $bus->expects($this->once())->method('dispatch')->with($this->callback(function (object $result): bool {
            $this->assertInstanceOf(ResultMessage::class, $result);
            $this->assertSame('warmup:0:result', $result->id);
            $this->assertSame(Phase::Warmup, $result->phase);
            $this->assertSame(\Ineersa\SqliteQueue\Bench\Payload::generate('warmup:0'), $result->payload);
            $this->assertFalse($result->followUp);
            $this->assertNotInstanceOf(ProbeMessage::class, $result);

            return true;
        }))->willReturn(new Envelope(new \stdClass()));
        $handler = new Handler(new Recorder(static fn (string $bytes): bool => true, 8192, 'run', 'execution'), $bus, Clock::system());
        $handler(new ApplicationMessage('warmup:0', Phase::Warmup, \Ineersa\SqliteQueue\Bench\Payload::generate('warmup:0'), true));
    }

    public function testRootAndResultHaveDistinctAckAndWorkflowCounters(): void
    {
        $summary = $this->analyze($this->events());
        $this->assertSame(1, $summary['unique_completions']);
        $this->assertSame(2, $summary['total_message_ack_completions']);
        $this->assertSame(1, $summary['window_unique_completions']);
        $this->assertSame(400.0, $summary['workflow_latency_ms']['mean']);
        $this->assertSame('pass', $summary['integrity_status']);
        $this->assertSame(1.0, $summary['workflow_window_confirmations_per_second']);
    }

    #[DataProvider('invalidWorkflows')]
    public function testInvalidWorkflowNeverCountsClean(string $fault): void
    {
        $events = $this->events();
        if ('missing' === $fault) {
            $events = array_values(array_filter($events, static fn (array $event): bool => 'root:result' !== $event['correlation']));
        } elseif ('duplicate' === $fault) {
            $events[] = $events[7];
        } elseif ('duplicate-send' === $fault) {
            $events[] = $events[4];
        } elseif ('orphan' === $fault) {
            foreach ($events as &$event) {
                if ('root:result' === $event['correlation']) {
                    $event['correlation'] = 'orphan:result';
                }
            }
            unset($event);
        } else {
            $index = match ($fault) {
                'intermediate-ack' => 3,
                'handler' => 2,
                'unknown-send' => 4,
            };
            $events[$index]['outcome'] = 'error';
            $events[$index]['unknown_commit'] = 'unknown-send' === $fault;
        }
        $summary = $this->analyze($events);
        $this->assertSame(0, $summary['unique_completions']);
        $this->assertSame('fail', $summary['integrity_status']);
        $this->assertSame(1, $summary['unfinished']);
    }

    public static function invalidWorkflows(): iterable
    {
        foreach (['missing', 'duplicate', 'duplicate-send', 'orphan', 'intermediate-ack', 'handler', 'unknown-send'] as $fault) {
            yield $fault => [$fault];
        }
    }

    public function testWindowWaitsForIntermediateAckEvenWhenResultFinishedEarlier(): void
    {
        $events = $this->events();
        $events[3]['ended_ns'] = 2000000000;
        $summary = $this->analyze($events);
        $this->assertSame(1, $summary['unique_completions']);
        $this->assertSame(0, $summary['window_unique_completions']);
        $this->assertSame(1.0, $summary['cohort_seconds']);
        $this->assertSame(400.0, $summary['workflow_latency_ms']['mean']);
    }

    public function testApplicationOptionsDeclareSyntheticWorkAndBothConsumers(): void
    {
        $command = new RunCommand();
        $options = RunCommand::options(new ArrayInput(['--workload' => 'application', '--smoke' => true], $command->getDefinition()));
        $this->assertSame(100, $options->configuration()['handler_milliseconds']);
        $this->assertSame(5, $options->configuration()['rate']);
    }

    private function events(): array
    {
        $events = [];
        foreach (['root', 'root:result'] as $id) {
            foreach (['send', 'delivery', 'handler', 'ack'] as $operation) {
                $events[] = ['phase' => 'measure', 'operation' => $operation, 'outcome' => 'success', 'correlation' => $id, 'started_ns' => 1000000000, 'ended_ns' => 'root:result' === $id ? 1400000000 : 1200000000, 'payload_bytes' => 256];
            }
        }

        foreach (['root', 'root:result'] as $id) {
            $events[] = ['phase' => 'measure', 'operation' => 'handler', 'outcome' => 'enter', 'correlation' => $id, 'started_ns' => 1000000000, 'ended_ns' => 1000000000, 'payload_bytes' => 256];
        }

        return $events;
    }

    private function analyze(array $events): array
    {
        $path = sys_get_temp_dir().'/sq-workflow-'.bin2hex(random_bytes(6));
        try {
            $messages = Analysis::build($path, $events, ['root', 'root:result'], 1000000000, 2000000000, Phase::Measure);

            return WorkflowAnalysis::apply($path, $messages, 1000000000, 2000000000);
        } finally {
            unlink($path);
        }
    }
}
