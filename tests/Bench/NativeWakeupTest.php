<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Recorder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;

final class NativeWakeupTest extends TestCase
{
    #[DataProvider('scenarios')]
    public function testNativeSingleQueueWakeupSmoke(string $scenario): void
    {
        $root = \dirname(__DIR__, 2);
        $process = new Process([\PHP_BINARY, $root.'/bin/benchmark', 'run', '--smoke', '--workload='.$scenario], $root, timeout: 90);
        $process->mustRun();
        $output = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
        $directory = $output['capture'];
        $summary = json_decode(file_get_contents($directory.'/summary.json'), true, flags: \JSON_THROW_ON_ERROR);
        $manifest = json_decode(file_get_contents($directory.'/manifest.json'), true, flags: \JSON_THROW_ON_ERROR);
        $this->assertCount(2, $manifest['results']);
        $this->assertSame($scenario, $manifest['settings']['workload']);
        foreach (['doctrine', 'broker'] as $backend) {
            $run = $summary['results'][$backend];
            $this->assertSame('complete', $run['execution_status']);
            $this->assertSame('pass', $run['integrity_status']);
            $this->assertSame('complete', $run['accounting_status']);
            $this->assertSame('pass', $run['warmup_integrity_status']);
            $this->assertSame(2, $run['unique_completions']);
            $this->assertSame(0, $run['audit_remaining']);
            $this->assertSame(['async'], $run['native_worker']['receivers']);
            if ('broker' === $backend) {
                $this->assertSame(0, $run['native_worker']['idle_timeout_microseconds']);
            }
            $pids = [];
            foreach (Recorder::read($directory.'/'.$backend.'/consumer.operations.jsonl') as $event) {
                $pids[$event['pid']] = true;
            }
            $this->assertCount(1, $pids);
            if ('idle' === $scenario) {
                $this->assertSame('pickup', $run['measurement_phase']);
                $this->assertSame(0.1, $run['idle']['window_seconds']);
                $this->assertSame(0, $run['idle']['publication_attempts']);
                $this->assertSame(0, $run['idle']['unexpected_work_records']);
                $this->assertNull($run['idle']['wait_registrations']);
                $this->assertNull($run['idle']['wait_wakes']);
                $this->assertArrayHasKey('receive_empty', $run['idle']);
                $this->assertCount(2, $run['pickup']['readiness']);
                foreach ($run['pickup']['readiness'] as $packet) {
                    $this->assertTrue($packet['worker_idle']);
                    $this->assertSame($run['native_worker']['pid'], $packet['pid']);
                    $this->assertNull($packet['wait_registration']);
                }
                $delta = $run['idle']['resources_by_role']['consumer']['until_next_boundary'];
                $this->assertArrayHasKey('user_seconds', $delta);
                $this->assertArrayHasKey('system_seconds', $delta);
                $this->assertArrayHasKey('elapsed_seconds', $delta);
                $this->assertArrayHasKey('io', $delta);
            }
        }
    }

    public static function scenarios(): iterable
    {
        yield 'idle' => ['idle'];
    }
}
