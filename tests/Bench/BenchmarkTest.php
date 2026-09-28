<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\{Baseline, Config, Process, Report, SampleStore, Stats};
use PHPUnit\Framework\TestCase;

final class BenchmarkTest extends TestCase
{
    private string $directory;

    protected function setUp(): void
    {
        $this->directory = dirname(__DIR__, 2) . '/var/tests/bench-' . bin2hex(random_bytes(8));
        mkdir($this->directory, 0700, true);
    }

    protected function tearDown(): void
    {
        $files = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($this->directory, \FilesystemIterator::SKIP_DOTS), \RecursiveIteratorIterator::CHILD_FIRST);
        foreach ($files as $file) {
            $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
        }
        rmdir($this->directory);
    }

    public function testPercentilesKeepOutliersAndNegativeValues(): void
    {
        self::assertNull(Stats::percentiles([])['p99']);
        $values = Stats::percentiles([-5, 1, 2, 10000]);
        self::assertSame(1.0, $values['p50']);
        self::assertSame(-5.0, $values['min']);
        self::assertSame(10000.0, $values['p99']);
        self::assertSame('inconclusive', $values['tail']);
        self::assertSame('ok', Stats::percentiles(range(1, Config::MIN_TAIL_SAMPLES))['tail']);
    }

    public function testCorrelatedMetricsDoNotClipDeliveryBeforeConfirmation(): void
    {
        $result = $this->analyze($this->records());
        self::assertSame('complete', $result['status']);
        self::assertSame(3.0, $result['metrics']['send_ms']['p50']);
        self::assertSame(-1.0, $result['metrics']['confirmation_to_handler_ms']['p50']);
        self::assertSame(5.0, $result['metrics']['full_cycle_ms']['p50']);
        self::assertSame(1, $result['integrity']['acks']);
        self::assertSame(0, $result['integrity']['unfinished']);
    }

    public function testMissingDuplicateCorruptAndFailedSamplesCannotPass(): void
    {
        $records = $this->records();
        $withoutDelivery = array_values(array_filter($records, static fn (array $r): bool => $r['kind'] !== 'deliver'));
        self::assertSame(1, $this->analyze($withoutDelivery)['integrity']['unfinished']);
        $records[] = $records[3];
        $records[] = $records[2];
        $result = $this->analyze($records);
        self::assertSame('incomplete', $result['status']);
        self::assertSame(1, $result['integrity']['duplicate_deliveries']);
        self::assertContains('duplicate send correlation IDs', $result['incomplete_reasons']);
        self::assertSame('incomplete', $this->analyze($this->records(), ['exit_code' => 1])['status']);
        self::assertSame('incomplete', $this->analyze($this->records(), ['timed_out' => true])['status']);
        self::assertSame('incomplete', $this->analyze($this->records(), ['exit_code' => null])['status']);
        file_put_contents($this->directory . '/broken.jsonl', "{oops\n");
        self::assertSame(1, SampleStore::read($this->directory . '/broken.jsonl')['corrupt']);
        self::assertTrue(SampleStore::read($this->directory . '/missing.jsonl')['missing']);
    }

    public function testProbeIsAccountedButExcludedFromMeasuredDistributions(): void
    {
        $records = $this->records();
        $records[2]['measured'] = false;
        $result = $this->analyze($records);
        self::assertSame('complete', $result['status']);
        self::assertSame(1, $result['integrity']['acks']);
        self::assertSame(0, $result['metrics']['send_ms']['count']);
    }

    public function testClockDurabilityAndLifecycleEvidenceAreRequired(): void
    {
        $records = $this->records();
        $records[0]['durability']['synchronous'] = 1;
        self::assertFalse($this->analyze($records)['durability']['verified']);
        self::assertFalse(Baseline::isDurabilityEquivalent(['journal_mode' => 'memory', 'synchronous' => 2, 'file_backed' => false]));
        $records = $this->records();
        $records[1]['valid'] = false;
        $result = $this->analyze($records);
        self::assertFalse($result['clock_comparable']);
        self::assertSame(0, $result['metrics']['full_cycle_ms']['count']);
        self::assertNull($result['throughput']);
        array_pop($records);
        self::assertContains('missing lifecycle or clock evidence', $this->analyze($records)['incomplete_reasons']);
    }

    public function testDelayedLatenessUsesStoredDeadlineAndRetainsQuantization(): void
    {
        $records = $this->records();
        $records[2] += ['delay_ms' => 2000, 'requested_deadline_wall' => 12.5];
        $records[3] += ['handler_wall' => 12.1, 'stored_deadline_wall' => 12.0, 'lateness_ms' => 100.0];
        $result = $this->analyze($records, [], ['prefill' => ['per_queue' => 1, 'delay_ms' => 2000]]);
        self::assertSame(100.0, $result['metrics']['delayed_lateness_ms']['p50']);
        self::assertEqualsWithDelta(-400.0, $result['metrics']['requested_deadline_lateness_ms']['p50'], 0.0001);
        self::assertSame(-500.0, $result['metrics']['deadline_quantization_ms']['p50']);
        $records[2]['measured'] = false;
        $result = $this->analyze($records, [], ['prefill' => ['per_queue' => 1, 'delay_ms' => 2000]]);
        self::assertSame(0, $result['metrics']['delayed_lateness_ms']['count']);
        self::assertCount(1, $result['metrics']['subsecond_probes']);
    }

    public function testReportPreservesFailedRunsAndCannotInventComparison(): void
    {
        $run = $this->analyze($this->records());
        $run['workload'] = ['name' => 'test'];
        $run['warmup'] = false;
        $failed = $run;
        $failed['status'] = 'incomplete';
        $failed['repetition'] = 'failed';
        $report = Report::build([$run, $failed], ['scheduled_runs' => 3]);
        self::assertSame('incomplete_baseline_only', $report['comparison']['status']);
        self::assertNull($report['comparison']['candidate']);
        self::assertSame('incomplete', $report['baseline_status']);
        self::assertSame(1, $report['unexecuted_runs']);
        self::assertCount(2, $report['per_run_variation']['test']['send_ms']);
        Report::write($this->directory, $report);
        self::assertEquals($report, json_decode(file_get_contents($this->directory . '/summary.json'), true, 512, JSON_THROW_ON_ERROR));
        self::assertStringContainsString('candidate has not been measured', file_get_contents($this->directory . '/report.md'));
        self::assertSame('incomplete', Report::build([], [])['baseline_status']);
    }

    public function testProcessReadinessAndTimeoutReapOnlyOwnedProcess(): void
    {
        if (PHP_OS_FAMILY !== 'Linux') {
            self::markTestSkipped('Linux process evidence');
        }
        $script = $this->directory . '/fixture.php';
        file_put_contents($script, '<?php file_put_contents($argv[1], json_encode(["event"=>"ready", "pid"=>getmypid()])); while (true) { usleep(10000); }');
        $process = Process::spawn('fixture', '0', [PHP_BINARY, $script, $this->directory . '/ready/fixture-0.ready'], ['PATH' => '/usr/bin:/bin'], $this->directory);
        try {
            self::assertSame($process->pid(), $process->waitForReady(5)['pid']);
            $snapshot = \Ineersa\SqliteQueue\Bench\ProcessTree::snapshot();
            self::assertSame(getmypid(), $snapshot[$process->pid()]['ppid']);
            self::assertContains($process->pid(), \Ineersa\SqliteQueue\Bench\ProcessTree::descendants(getmypid(), $snapshot));
            self::assertGreaterThan(0, $snapshot[$process->pid()]['rss_kb']);
            $process->wait(0.01);
            self::assertTrue($process->timedOut());
            self::assertFalse($process->isRunning());
            self::assertSame([], $process->survivors());
            self::assertNotSame(0, $process->exitCode());
        } finally {
            if ($process->isRunning()) {
                $process->killTree();
                $process->wait(1);
            }
        }
    }

    public function testDoctrineConfirmsCommittedSendAndAckWithBinaryPayload(): void
    {
        $connection = Baseline::connect($this->directory . '/queue.sqlite');
        $observer = null;
        try {
            $transport = Baseline::transport($connection, 'test');
            $transport->setup();
            $observer = Baseline::connect($this->directory . '/queue.sqlite');
            self::assertTrue(Baseline::isDurabilityEquivalent(Baseline::durability($connection)));
            self::assertSame(1000, Baseline::durability($observer)['wal_autocheckpoint']);
            $message = \Ineersa\SqliteQueue\Bench\BenchMessage::generate('id', 'test', 256);
            $sent = $transport->send(new \Symfony\Component\Messenger\Envelope($message));
            $id = $sent->last(\Symfony\Component\Messenger\Stamp\TransportMessageIdStamp::class)->getId();
            self::assertFalse($connection->isTransactionActive());
            self::assertTrue(Baseline::rowExists($observer, $id));
            $received = iterator_to_array($transport->get());
            self::assertCount(1, $received);
            self::assertTrue($received[0]->getMessage()->matchesRegeneratedPayload());
            $transport->ack($received[0]);
            self::assertFalse($connection->isTransactionActive());
            self::assertFalse(Baseline::rowExists($observer, $id));
        } finally {
            $observer?->close();
            $connection->close();
        }
    }

    private function records(): array
    {
        return [
            ['kind' => 'header', 'durability' => ['journal_mode' => 'wal', 'synchronous' => 2, 'file_backed' => true]],
            ['kind' => 'clock_check', 'valid' => true, 'before_ready_ns' => 1, 'parent_go_ns' => 2, 'after_go_ns' => 3],
            ['kind' => 'send', 'msg' => 'a', 'ok' => true, 'size' => 256, 'duration_ms' => 3, 't_invoke_ns' => 1000000, 't_confirm_ns' => 4000000],
            ['kind' => 'deliver', 'msg' => 'a', 'outcome' => 'ack', 'payload_ok' => true, 't_handler_ns' => 3000000, 't_ack_confirm_ns' => 6000000, 'ack_duration_ms' => 1, 'handling_ms' => 3],
            ['kind' => 'foot'],
        ];
    }

    private function analyze(array $records, array $sourceOverrides = [], array $workloadOverrides = []): array
    {
        $path = $this->directory . '/samples.jsonl';
        file_put_contents($path, implode("\n", array_map(static fn (array $r): string => json_encode($r, JSON_THROW_ON_ERROR), $records)) . "\n");
        $source = $sourceOverrides + ['role' => 'fixture', 'argument' => '0', 'path' => $path, 'exit_code' => 0, 'timed_out' => false, 'killed' => false, 'stderr' => ''];
        return Stats::analyze($workloadOverrides + ['publishers' => [['count' => 1]], 'queues' => ['q']], 'test', [$source], ['inventory' => [], 'clock_probe' => ['verified' => true]]);
    }
}
