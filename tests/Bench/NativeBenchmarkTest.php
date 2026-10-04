<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Recorder;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process;

final class NativeBenchmarkTest extends TestCase
{
    public function testNativeCommandsKeepWarmupAndMeasureInOneWorker(): void
    {
        $root = \dirname(__DIR__, 2);
        $process = new Process([\PHP_BINARY, $root.'/bin/benchmark', 'run', '--smoke', '--workload=roundtrip', '--no-ansi', '--no-interaction'], $root, timeout: 60);
        $process->run();
        $this->assertSame(0, $process->getExitCode(), $process->getErrorOutput().' '.$process->getOutput());
        $output = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
        $directory = $output['capture'];
        $summary = json_decode(file_get_contents($directory.'/summary.json'), true, flags: \JSON_THROW_ON_ERROR);
        foreach (['doctrine', 'broker'] as $backend) {
            $result = $summary['results'][$backend];
            $this->assertSame('complete', $result['execution_status']);
            $this->assertSame('pass', $result['integrity_status']);
            $this->assertSame('complete', $result['accounting_status']);
            $this->assertSame('pass', $result['warmup_integrity_status']);
            $this->assertSame(4, $result['unique_completions']);
            $this->assertSame(0, $result['audit_remaining']);
            $this->assertSame([], $result['terminal_telemetry']['issues']);
            $this->assertTrue($result['telemetry_footers']['publisher']['finalized']);
            $this->assertTrue($result['telemetry_footers']['consumer']['finalized']);
            foreach (['send_ms', 'delivery_ms', 'handler_ms', 'ack_ms', 'full_cycle_ms', 'confirmation_gap_ms'] as $metric) {
                $this->assertSame(4, $result['latencies'][$metric]['count']);
                $this->assertSame('available-boundaries-only', $result['latencies'][$metric]['coverage']);
                $this->assertSame(2, $result['payload_latencies'][256][$metric]['count']);
                $this->assertSame(2, $result['payload_latencies'][16384][$metric]['count']);
            }
            $roles = $result['resources']['coverage_details']['registry'];
            $this->assertArrayHasKey('observer-publisher-shared', $roles);
            $this->assertSame($result['native_worker']['pid'], $roles['consumer']['pid']);
            $this->assertIsInt($roles['consumer']['start_ticks']);
            if ('broker' === $backend) {
                $this->assertArrayHasKey('broker', $roles);
                $this->assertArrayHasKey('persistence', $roles);
                $this->assertNotSame($roles['broker']['pid'], $roles['persistence']['pid']);
                $ready = json_decode(file_get_contents($directory.'/'.$backend.'/broker.ready.json'), true, flags: \JSON_THROW_ON_ERROR);
                $this->assertSame($ready['pid'], $roles['broker']['pid']);
                $this->assertSame($ready['persistence_pid'], $roles['persistence']['pid']);
                $this->assertNull($result['resources']['phase_roles']['connected']['persistence']['php_used_bytes']);
            }
            $connected = $result['resources']['phase_roles']['connected'];
            $this->assertIsInt($connected['consumer']['php_used_bytes']);
            $this->assertArrayHasKey('pss_bytes', $connected['consumer']);
            $this->assertArrayHasKey('io', $connected['consumer']);
            $this->assertArrayHasKey('until_next_boundary', $result['resources']['phase_roles']['measure']['consumer']);
            $this->assertSame(0, $result['telemetry_footers']['consumer']['lost_records']);
            $this->assertFileExists($directory.'/'.$backend.'/resources.jsonl');
            $this->assertFileExists($directory.'/'.$backend.'/analysis.sqlite');
            $pids = [];
            $phases = [];
            foreach (Recorder::read($directory.'/'.$backend.'/consumer.operations.jsonl') as $event) {
                $pids[$event['pid']] = true;
                $phases[$event['phase']] = true;
            }
            $this->assertCount(1, $pids);
            $this->assertArrayHasKey('warmup', $phases);
            $this->assertArrayHasKey('measure', $phases);
            $boundaries = json_decode(file_get_contents($directory.'/'.$backend.'/phases.json'), true, flags: \JSON_THROW_ON_ERROR);
            $this->assertSame(['boot', 'connected', 'warmup', 'reset', 'measure', 'drain', 'settle', 'audit', 'shutdown', 'shutdown_complete'], array_keys($boundaries));
            $values = array_values($boundaries);
            $sorted = $values;
            sort($sorted);
            $this->assertSame($sorted, $values);
        }
        $this->assertSame(0, $summary['results']['broker']['native_worker']['idle_timeout_microseconds']);
        $this->assertSame(1000, $summary['results']['doctrine']['native_worker']['idle_timeout_microseconds']);
        $this->assertFileExists($directory.'/report.md');
        $this->assertStringContainsString('Product-total resource accounting is partial', file_get_contents($directory.'/report.md'));
        $this->assertFileExists($directory.'/manifest.json');
        $this->assertFileExists($directory.'/source/bench/src/Kernel.php');
    }

    public function testApplicationUsesTwoNativeWorkersAndCorrelatedResultAcknowledgement(): void
    {
        $root = \dirname(__DIR__, 2);
        $process = new Process([\PHP_BINARY, $root.'/bin/benchmark', 'run', '--smoke', '--workload=application', '--handler-ms=1', '--rate=10', '--capacity=4'], $root, timeout: 90);
        $process->mustRun();
        $output = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
        $summary = json_decode(file_get_contents($output['capture'].'/summary.json'), true, flags: \JSON_THROW_ON_ERROR);
        foreach ($summary['results'] as $backend => $run) {
            $this->assertSame('complete', $run['execution_status']);
            $this->assertSame('pass', $run['integrity_status']);
            $this->assertSame('pass', $run['warmup_integrity_status']);
            $this->assertSame('complete', $run['accounting_status']);
            $this->assertSame('offered-load-met', $run['generator']['status']);
            $this->assertSame($run['expected'], $run['unique_completions']);
            $this->assertSame(2 * $run['unique_completions'], $run['total_message_ack_completions']);
            $this->assertSame($run['unique_completions'], $run['workflow_latency_ms']['count']);
            $this->assertGreaterThan(0, $run['workflow_payload_latency_ms'][256]['count']);
            $this->assertGreaterThan(0, $run['workflow_payload_latency_ms'][16384]['count']);
            $this->assertSame(0, $run['audit_remaining']);
            $registry = $run['resources']['coverage_details']['registry'];
            $this->assertSame($run['native_worker']['pid'], $registry['consumer']['pid']);
            $this->assertSame($run['native_result_worker']['pid'], $registry['consumer-results']['pid']);
            $this->assertNotSame($registry['consumer']['pid'], $registry['consumer-results']['pid']);
            if ('broker' === $backend) {
                $this->assertSame(0, $run['native_worker']['idle_timeout_microseconds']);
                $this->assertSame(0, $run['native_result_worker']['idle_timeout_microseconds']);
            }
            foreach (['consumer', 'results'] as $role) {
                $pids = [];
                $phases = [];
                foreach (Recorder::read($output['capture'].'/'.$backend.'/'.$role.'.operations.jsonl') as $event) {
                    $pids[$event['pid']] = true;
                    $phases[$event['phase']] = true;
                }
                $this->assertCount(1, $pids);
                $this->assertArrayHasKey('warmup', $phases);
                $this->assertArrayHasKey('measure', $phases);
            }
            $this->assertIsInt($run['resources']['phase_roles']['connected']['consumer-results']['php_used_bytes']);
            $this->assertIsInt($run['resources']['phase_roles']['settle']['consumer-results']['php_used_bytes']);
        }
    }

    public function testRetentionKeepsProcessIdentitiesAcrossTwentyAuditedEmptyCycles(): void
    {
        $root = \dirname(__DIR__, 2);
        $process = new Process([\PHP_BINARY, $root.'/bin/benchmark', 'run', '--smoke', '--workload=retention', '--cycles=20', '--cycle-messages=2', '--settling=0.001'], $root, timeout: 180);
        $process->mustRun();
        $output = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
        $summary = json_decode(file_get_contents($output['capture'].'/summary.json'), true, flags: \JSON_THROW_ON_ERROR);
        foreach ($summary['results'] as $backend => $run) {
            $this->assertSame('complete', $run['execution_status']);
            $this->assertSame('pass', $run['integrity_status']);
            $this->assertSame('complete', $run['accounting_status']);
            $this->assertSame(40, $run['unique_completions']);
            $this->assertSame(20, $run['retention']['cycle_execution']['completed_cycles']);
            $this->assertSame(40, $run['retention']['cycle_execution']['historical_completions']);
            $this->assertSame(0, $run['audit_remaining']);
            $cycles = 0;
            foreach (Recorder::read($output['capture'].'/'.$backend.'/cycles.jsonl') as $cycle) {
                $this->assertTrue($cycle['equivalent_empty_point']);
                $this->assertSame(0, $cycle['inventory']['remaining']);
                $this->assertSame(0, $cycle['inventory']['ready']);
                $this->assertSame(0, $cycle['inventory']['inflight']);
                $this->assertSame(2 * $cycle['retention_cycle'], $cycle['historical_completions']);
                ++$cycles;
            }
            $this->assertSame(21, $cycles);
            $registry = $run['resources']['coverage_details']['registry'];
            foreach ($run['retention']['roles'] as $role => $series) {
                $this->assertSame($registry[$role]['pid'], $series['pid']);
                $this->assertSame($registry[$role]['start_ticks'], $series['start_ticks']);
                $this->assertSame(21, $series['matched_points']);
                $this->assertSame(0, $series['excluded_points']);
                $this->assertSame(0, $series['identity_changes']);
                $this->assertSame(40, $series['historical_completions']);
                if (\in_array($role, ['broker', 'persistence'], true)) {
                    $this->assertSame('unavailable', $series['metrics']['php_used_bytes']['coverage']);
                }
            }
            $this->assertSame(21, $run['retention']['roles']['consumer']['metrics']['php_used_bytes']['count']);
            $this->assertFalse($run['retention']['forced_gc']);
            $this->assertFalse($run['retention']['process_restarts']);
            if ('broker' === $backend) {
                $this->assertArrayHasKey('broker', $run['retention']['roles']);
                $this->assertArrayHasKey('persistence', $run['retention']['roles']);
            }
        }
    }

    public function testTinyPilotRecordsFixedWindowAndFrozenSource(): void
    {
        $root = \dirname(__DIR__, 2);
        $process = new Process([\PHP_BINARY, $root.'/bin/benchmark', 'run', '--pilot', '--duration=0.1', '--repetitions=1', '--workload=roundtrip'], $root, timeout: 90);
        $process->mustRun();
        $output = json_decode(trim($process->getOutput()), true, flags: \JSON_THROW_ON_ERROR);
        $directory = $output['capture'];
        $manifest = json_decode(file_get_contents($directory.'/manifest.json'), true, flags: \JSON_THROW_ON_ERROR);
        $this->assertSame('pilot', $manifest['mode']);
        $this->assertCount(2, $manifest['schedule']);
        $this->assertSame(64, \strlen($manifest['source_sha256']));
        $this->assertFileExists($directory.'/git-status.txt');
        $summary = json_decode(file_get_contents($directory.'/summary.json'), true, flags: \JSON_THROW_ON_ERROR);
        foreach ($summary['results'] as $id => $run) {
            $this->assertSame('complete', $run['execution_status']);
            $this->assertSame('pass', $run['integrity_status']);
            $this->assertSame(0.1, $run['window_seconds']);
            $this->assertSame($run['expected'], $run['unique_completions']);
            $this->assertFileExists($directory.'/'.$id.'/expected-ids.txt');
            $this->assertFileExists($directory.'/'.$id.'/config.json');
        }
    }
}
