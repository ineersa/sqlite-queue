<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Backend;
use Ineersa\SqliteQueue\Bench\CohortJournal;
use Ineersa\SqliteQueue\Bench\Command\AuditCommand;
use Ineersa\SqliteQueue\Bench\Control;
use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Ineersa\SqliteQueue\Bench\IdleMetrics;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\Recorder;
use Ineersa\SqliteQueue\Bench\Runner;
use Ineersa\SqliteQueue\Bench\Scenario;
use Ineersa\SqliteQueue\Bench\TelemetryLevel;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Messenger\MessageBusInterface;

final class MeasurementRepairTest extends TestCase
{
    public function testEssentialIdlePhaseAggregateIsNotReportedAsExactWindowZero(): void
    {
        $summary = IdleMetrics::summarize([], 100, 200);
        $essential = IdleMetrics::reconcileFooter($summary, ['aggregated_empty_attempts_by_phase' => ['idle' => 63]], TelemetryLevel::Essential);
        $this->assertSame(63, $essential['phase_aggregated_empty_attempts']);
        $this->assertNull($essential['receive_empty']);
        $this->assertNull($essential['receive_attempts']);
        $this->assertStringContainsString('unavailable for the exact window', $essential['receive_counter_coverage']);
        $missing = IdleMetrics::reconcileFooter($summary, [], TelemetryLevel::Essential);
        $this->assertNull($missing['phase_aggregated_empty_attempts']);
        $detailed = IdleMetrics::reconcileFooter($summary, ['aggregated_empty_attempts_by_phase' => ['idle' => 63]], TelemetryLevel::Detailed);
        $this->assertSame(0, $detailed['receive_empty']);
        $this->assertSame(0, $detailed['receive_attempts']);
        $this->assertSame($summary['integrity_status'], $detailed['integrity_status']);
    }

    public function testAuditAcquisitionIsStructurallyReadOnlyWithUriCharacters(): void
    {
        $directory = sys_get_temp_dir().'/sq-audit-uri-'.bin2hex(random_bytes(6));
        (new Filesystem())->mkdir($directory);
        $path = $directory.'/queue ?#%.sqlite';
        try {
            $writer = new \PDO('sqlite:'.$path);
            $writer->exec('CREATE TABLE queue_messages (available_at INTEGER, reserved_until INTEGER)');
            $writer->exec('INSERT INTO queue_messages VALUES (0, NULL)');
            $method = new \ReflectionMethod(AuditCommand::class, 'openReadOnly');
            $reader = $method->invoke(null, $path);
            $this->assertSame(1, (int) $reader->query('SELECT COUNT(*) FROM queue_messages')->fetchColumn());
            try {
                $reader->exec('INSERT INTO queue_messages VALUES (0, NULL)');
                $this->fail('An audit connection must reject writes.');
            } catch (\PDOException $error) {
                $this->assertSame(8, $error->errorInfo[1]);
            }
            $command = new CommandTester(new AuditCommand());
            $this->assertSame(0, $command->execute(['database' => $path, 'backend' => 'broker']));
            $inventory = json_decode($command->getDisplay(), true, flags: \JSON_THROW_ON_ERROR);
            $this->assertSame(1, $inventory['remaining']);
            $this->assertSame(1, $inventory['ready']);
            $this->assertSame(0, $inventory['inflight']);
            try {
                $method->invoke(null, $path.'-missing');
                $this->fail('Read-only acquisition cannot create missing storage.');
            } catch (\InvalidArgumentException) {
                $this->assertFileDoesNotExist($path.'-missing');
            }
        } finally {
            (new Filesystem())->remove($directory);
        }
    }

    public function testFailedRetentionSendUpdatesActualMeasurementBoundsBeforeCleanup(): void
    {
        $root = \dirname(__DIR__, 2);
        $directory = sys_get_temp_dir().'/sq-retention-bounds-'.bin2hex(random_bytes(6));
        (new Filesystem())->mkdir($directory);
        $path = $directory.'/queue.sqlite';
        $writer = new \PDO('sqlite:'.$path);
        $writer->exec('CREATE TABLE queue_messages (available_at INTEGER, reserved_until INTEGER)');
        $journal = CohortJournal::create($directory.'/expected-ids.txt');
        $pair = stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        fwrite($pair[1], "{\"id\":\"measure\"}\n");
        $control = new Control($pair[0]);
        $bus = $this->createMock(MessageBusInterface::class);
        $bus->expects($this->once())->method('dispatch')->willThrowException(new \RuntimeException('controlled publish failure'));
        $runner = new Runner(static function (string $line): void {}, new RunOptionsDTO(Scenario::Retention, true, false, 60, 5, cycles: 20, cycleMessages: 1, settlingSeconds: 0.001));
        $bounds = ['phase' => Phase::Warmup, 'start' => 1, 'end' => 2];
        $snapshot = static function (Phase $phase, array $context): void {};
        $boundary = static fn (Phase $phase): int => match ($phase) {
            Phase::Settle => 100,
            Phase::Audit => 110,
            Phase::Measure => 120,
            default => throw new \LogicException('Unexpected boundary.'),
        };
        $recorder = new Recorder(static fn (string $bytes): bool => true, 8192, 'run', 'publisher');
        try {
            $method = new \ReflectionMethod(Runner::class, 'retentionCycles');
            $arguments = [$root, $directory, $path, Backend::Broker, $bus, $control, ['consumer' => $control], $journal, $recorder, $snapshot, $boundary, &$bounds];
            try {
                $method->invokeArgs($runner, $arguments);
                $this->fail('Controlled publication must fail.');
            } catch (\RuntimeException $error) {
                $this->assertSame('controlled publish failure', $error->getMessage());
            }
            $this->assertSame(Phase::Measure, $bounds['phase']);
            $this->assertSame(120, $bounds['start']);
            $this->assertIsInt($bounds['end']);
            $this->assertNotSame(2, $bounds['end']);
        } finally {
            $journal->close();
            $control->close();
            fclose($pair[1]);
            (new Filesystem())->remove($directory);
        }
    }
}
