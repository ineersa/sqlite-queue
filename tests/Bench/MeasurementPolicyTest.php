<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Backend;
use Ineersa\SqliteQueue\Bench\DTO\RunOptionsDTO;
use Ineersa\SqliteQueue\Bench\Operation;
use Ineersa\SqliteQueue\Bench\Outcome;
use Ineersa\SqliteQueue\Bench\Phase;
use Ineersa\SqliteQueue\Bench\Recorder;
use Ineersa\SqliteQueue\Bench\Runner;
use Ineersa\SqliteQueue\Bench\Scenario;
use PHPUnit\Framework\TestCase;

final class MeasurementPolicyTest extends TestCase
{
    public function testEmptyAttemptsAreBoundedAndNeverWriteUntilFooter(): void
    {
        $writes = 0;
        $bytes = '';
        $recorder = new Recorder(static function (string $data) use (&$writes, &$bytes): bool {
            ++$writes;
            $bytes .= $data;

            return true;
        }, 65536, 'run', 'consumer');
        for ($i = 0; $i < 100000; ++$i) {
            $recorder->record(Operation::Receive, Outcome::Empty, Phase::Idle, 'empty', '', $i * 10000, $i * 10000 + 9000, '', activeNanoseconds: 1000);
        }
        $this->assertSame(0, $writes);
        $this->assertSame(0, $recorder->counters()['buffered_bytes']);
        $row = $recorder->counters()['empty_receives_by_phase']['idle'];
        $this->assertSame(100000, $row['count']);
        $this->assertSame(100000000, $row['sum_active_ns']);
        $this->assertSame(1000, $row['max_active_ns']);
        $this->assertSame(100000, $row['histogram'][0]);
        $this->assertSame(0, $row['first_started_ns']);
        $this->assertSame(999999000, $row['last_ended_ns']);
        $this->assertLessThan(2000, \strlen(json_encode($recorder->counters(), \JSON_THROW_ON_ERROR)));
        $recorder->record(Operation::Receive, Outcome::Error, Phase::Idle, 'error', '', 1, 2, 'receive failed', activeNanoseconds: 1);
        $recorder->flush();
        $this->assertSame(1, $writes);
        $this->assertSame('error', json_decode(trim($bytes), true, flags: \JSON_THROW_ON_ERROR)['outcome']);
    }

    public function testTerminalFooterReconcilesHistogramAndFlush(): void
    {
        $fixture = new \Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase();
        try {
            $recorder = new Recorder(static fn (string $bytes): bool => true, 65536, 'run', 'consumer');
            $recorder->record(Operation::Receive, Outcome::Empty, Phase::Idle, 'empty', '', 10, 1011, '', activeNanoseconds: 1001);
            $footer = $recorder->counters() + ['finalized' => true];
            $path = $fixture->path('consumer.operations.jsonl.counters.json');
            file_put_contents($path, json_encode($footer, \JSON_THROW_ON_ERROR));
            $this->assertSame([], \Ineersa\SqliteQueue\Bench\FailureFinalization::terminalFooters($fixture->directory(), ['consumer'])['issues']);
            $this->assertSame(1, $footer['empty_receives_by_phase']['idle']['histogram'][1]);
            $footer['empty_receives_by_phase']['idle']['count'] = 2;
            file_put_contents($path, json_encode($footer, \JSON_THROW_ON_ERROR));
            $this->assertArrayHasKey('footer_error', \Ineersa\SqliteQueue\Bench\FailureFinalization::terminalFooters($fixture->directory(), ['consumer'])['issues']['consumer']);
        } finally {
            $fixture->remove();
        }
    }

    public function testIncompleteCleanupStopsRunnerScheduleAndPreservesEvidence(): void
    {
        $calls = [];
        $capture = '';
        $runner = new Runner(static function (string $line) use (&$capture): void { $capture = json_decode($line, true, flags: \JSON_THROW_ON_ERROR)['capture']; }, new RunOptionsDTO(Scenario::Roundtrip, true, 1, \Ineersa\SqliteQueue\Sqlite\SqliteSynchronousMode::Normal), static function (string $root, string $directory, Backend $backend) use (&$calls): array {
            $calls[] = $backend;
            file_put_contents($directory.'/endpoint-retained.sock', 'owned endpoint fixture');
            file_put_contents($directory.'/evidence.json', '{"survivor":true}');

            return ['execution_status' => 'failed', 'integrity_status' => 'unknown', 'accounting_status' => 'partial', 'cleanup' => ['complete' => false], 'failure' => 'owned survivor'];
        });
        $this->assertSame(1, $runner->run());
        $this->assertSame([Backend::Doctrine], $calls);
        $this->assertFileExists($capture.'/doctrine/endpoint-retained.sock');
        $this->assertFileExists($capture.'/doctrine/evidence.json');
        $this->assertSame('scheduled', json_decode(file_get_contents($capture.'/broker/result.json'), true, flags: \JSON_THROW_ON_ERROR)['execution_status']);
    }
}
