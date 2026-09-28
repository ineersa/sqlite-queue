<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Baseline;
use Ineersa\SqliteQueue\Bench\Config;
use Ineersa\SqliteQueue\Bench\Process;
use Ineersa\SqliteQueue\Bench\Report;
use Ineersa\SqliteQueue\Bench\SampleStore;
use Ineersa\SqliteQueue\Bench\Stats;
use PHPUnit\Framework\TestCase;

final class BenchmarkTest extends TestCase
{
    private string $directory;

    protected function setUp(): void
    {
        $this->directory = \dirname(__DIR__, 2).'/var/tests/bench-'.bin2hex(random_bytes(8));
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
        $this->assertNull(Stats::percentiles([])['p99']);
        $values = Stats::percentiles([-5, 1, 2, 10000]);
        $this->assertSame(1.0, $values['p50']);
        $this->assertSame(-5.0, $values['min']);
        $this->assertSame(10000.0, $values['p99']);
        $this->assertSame('inconclusive', $values['tail']);
        $this->assertSame('ok', Stats::percentiles(range(1, Config::MIN_TAIL_SAMPLES))['tail']);
    }

    public function testCorrelatedMetricsDoNotClipDeliveryBeforeConfirmation(): void
    {
        $result = $this->analyze($this->records());
        $this->assertSame('complete', $result['status']);
        $this->assertSame(3.0, $result['metrics']['send_ms']['p50']);
        $this->assertSame(-1.0, $result['metrics']['confirmation_to_handler_ms']['p50']);
        $this->assertSame(5.0, $result['metrics']['full_cycle_ms']['p50']);
        $this->assertSame(1, $result['integrity']['acks']);
        $this->assertSame(0, $result['integrity']['unfinished']);
    }

    public function testMissingDuplicateCorruptAndFailedSamplesCannotPass(): void
    {
        $records = $this->records();
        $withoutDelivery = array_values(array_filter($records, static fn (array $r): bool => 'deliver' !== $r['kind']));
        $this->assertSame(1, $this->analyze($withoutDelivery)['integrity']['unfinished']);
        $records[] = $records[3];
        $records[] = $records[2];
        $result = $this->analyze($records);
        $this->assertSame('incomplete', $result['status']);
        $this->assertSame(1, $result['integrity']['duplicate_deliveries']);
        $this->assertContains('duplicate send correlation IDs', $result['incomplete_reasons']);
        $this->assertSame('incomplete', $this->analyze($this->records(), ['exit_code' => 1])['status']);
        $this->assertSame('incomplete', $this->analyze($this->records(), ['timed_out' => true])['status']);
        $this->assertSame('incomplete', $this->analyze($this->records(), ['exit_code' => null])['status']);
        file_put_contents($this->directory.'/broken.jsonl', "{oops\n");
        $this->assertSame(1, SampleStore::read($this->directory.'/broken.jsonl')['corrupt']);
        $this->assertTrue(SampleStore::read($this->directory.'/missing.jsonl')['missing']);
    }

    public function testProbeIsAccountedButExcludedFromMeasuredDistributions(): void
    {
        $records = $this->records();
        $records[2]['measured'] = false;
        $result = $this->analyze($records);
        $this->assertSame('complete', $result['status']);
        $this->assertSame(1, $result['integrity']['acks']);
        $this->assertSame(0, $result['metrics']['send_ms']['count']);
    }

    public function testClockDurabilityAndLifecycleEvidenceAreRequired(): void
    {
        $records = $this->records();
        $records[0]['durability']['synchronous'] = 1;
        $this->assertFalse($this->analyze($records)['durability']['verified']);
        $this->assertFalse(Baseline::isDurabilityEquivalent(['journal_mode' => 'memory', 'synchronous' => 2, 'file_backed' => false]));
        $records = $this->records();
        $records[1]['valid'] = false;
        $result = $this->analyze($records);
        $this->assertFalse($result['clock_comparable']);
        $this->assertSame(0, $result['metrics']['full_cycle_ms']['count']);
        $this->assertNull($result['throughput']);
        array_pop($records);
        $this->assertContains('missing lifecycle or clock evidence', $this->analyze($records)['incomplete_reasons']);
    }

    public function testDelayedLatenessUsesStoredDeadlineAndRetainsQuantization(): void
    {
        $records = $this->records();
        $records[2] += ['delay_ms' => 2000, 'requested_deadline_wall' => 12.5];
        $records[3] += ['handler_wall' => 12.1, 'stored_deadline_wall' => 12.0, 'lateness_ms' => 100.0];
        $result = $this->analyze($records, [], ['prefill' => ['per_queue' => 1, 'delay_ms' => 2000]]);
        $this->assertSame(100.0, $result['metrics']['delayed_lateness_ms']['p50']);
        $this->assertEqualsWithDelta(-400.0, $result['metrics']['requested_deadline_lateness_ms']['p50'], 0.0001);
        $this->assertSame(-500.0, $result['metrics']['deadline_quantization_ms']['p50']);
        $records[2]['measured'] = false;
        $result = $this->analyze($records, [], ['prefill' => ['per_queue' => 1, 'delay_ms' => 2000]]);
        $this->assertSame(0, $result['metrics']['delayed_lateness_ms']['count']);
        $this->assertCount(1, $result['metrics']['subsecond_probes']);
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
        $this->assertSame('incomplete_baseline_only', $report['comparison']['status']);
        $this->assertNull($report['comparison']['candidate']);
        $this->assertSame('incomplete', $report['baseline_status']);
        $this->assertSame(1, $report['unexecuted_runs']);
        $this->assertCount(2, $report['per_run_variation']['test']['send_ms']);
        Report::write($this->directory, $report);
        $this->assertEquals($report, json_decode(file_get_contents($this->directory.'/summary.json'), true, 512, \JSON_THROW_ON_ERROR));
        $this->assertStringContainsString('candidate has not been measured', file_get_contents($this->directory.'/report.md'));
        $this->assertSame('incomplete', Report::build([], [])['baseline_status']);
    }

    public function testProcessReadinessAndTimeoutReapOnlyOwnedProcess(): void
    {
        if (\PHP_OS_FAMILY !== 'Linux') {
            $this->markTestSkipped('Linux process evidence');
        }
        $script = $this->directory.'/fixture.php';
        file_put_contents($script, '<?php file_put_contents($argv[1], json_encode(["event"=>"ready", "pid"=>getmypid()])); while (true) { usleep(10000); }');
        $process = Process::spawn('fixture', '0', [\PHP_BINARY, $script, $this->directory.'/ready/fixture-0.ready'], ['PATH' => '/usr/bin:/bin'], $this->directory);
        try {
            $this->assertSame($process->pid(), $process->waitForReady(5)['pid']);
            $snapshot = \Ineersa\SqliteQueue\Bench\ProcessTree::snapshot();
            $this->assertSame(getmypid(), $snapshot[$process->pid()]['ppid']);
            $this->assertContains($process->pid(), \Ineersa\SqliteQueue\Bench\ProcessTree::descendants(getmypid(), $snapshot));
            $this->assertGreaterThan(0, $snapshot[$process->pid()]['rss_kb']);
            $process->wait(0.01);
            $this->assertTrue($process->timedOut());
            $this->assertFalse($process->isRunning());
            $this->assertSame([], $process->survivors());
            $this->assertNotSame(0, $process->exitCode());
        } finally {
            if ($process->isRunning()) {
                $process->killTree();
                $process->wait(1);
            }
        }
    }

    public function testDoctrineConfirmsCommittedSendAndAckWithBinaryPayload(): void
    {
        $connection = Baseline::connect($this->directory.'/queue.sqlite');
        $observer = null;
        try {
            $transport = Baseline::transport($connection, 'test');
            $transport->setup();
            $observer = Baseline::connect($this->directory.'/queue.sqlite');
            $this->assertTrue(Baseline::isDurabilityEquivalent(Baseline::durability($connection)));
            $this->assertSame(1000, Baseline::durability($observer)['wal_autocheckpoint']);
            $message = \Ineersa\SqliteQueue\Bench\BenchMessage::generate('id', 'test', 256);
            $sent = $transport->send(new \Symfony\Component\Messenger\Envelope($message));
            $id = $sent->last(\Symfony\Component\Messenger\Stamp\TransportMessageIdStamp::class)->getId();
            $this->assertFalse($connection->isTransactionActive());
            $this->assertTrue(Baseline::rowExists($observer, $id));
            $received = iterator_to_array($transport->get());
            $this->assertCount(1, $received);
            $this->assertTrue($received[0]->getMessage()->matchesRegeneratedPayload());
            $transport->ack($received[0]);
            $this->assertFalse($connection->isTransactionActive());
            $this->assertFalse(Baseline::rowExists($observer, $id));
        } finally {
            $observer?->close();
            $connection->close();
        }
    }

    /**
     * @return list<array<string, mixed>>
     */
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

    /**
     * @param list<array<string, mixed>> $records
     * @param array<string, mixed>       $sourceOverrides
     * @param array<string, mixed>       $workloadOverrides
     *
     * @return array<string, mixed>
     */
    private function analyze(array $records, array $sourceOverrides = [], array $workloadOverrides = []): array
    {
        $path = $this->directory.'/samples.jsonl';
        file_put_contents($path, implode("\n", array_map(static fn (array $r): string => json_encode($r, \JSON_THROW_ON_ERROR), $records))."\n");
        $source = $sourceOverrides + ['role' => 'fixture', 'argument' => '0', 'path' => $path, 'exit_code' => 0, 'timed_out' => false, 'killed' => false, 'stderr' => ''];

        return Stats::analyze($workloadOverrides + ['publishers' => [['count' => 1]], 'queues' => ['q']], 'test', [$source], ['inventory' => [], 'clock_probe' => ['verified' => true]]);
    }
}
