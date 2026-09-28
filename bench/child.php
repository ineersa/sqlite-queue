<?php

declare(strict_types=1);

require __DIR__ . '/bootstrap.php';

use Ineersa\SqliteQueue\Bench\{Baseline, BenchMessage, Clock, Config};
use Ineersa\SqliteQueue\Bench\Child\{Recorder, Receiver};
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\{Envelope, MessageBus, Worker};
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Stamp\{DelayStamp, TransportMessageIdStamp};

date_default_timezone_set('UTC');
$config = json_decode(file_get_contents($argv[1]), true, 512, JSON_THROW_ON_ERROR);
$role = $argv[2];
$index = (int) $argv[3];
$directory = $config['directory'];
$workload = $config['workload'];
$label = $role . '-' . $index;
$recorder = new Recorder($directory . '/samples/' . $label . '.jsonl', $role, (string) $index);
$connection = null;
$receiver = null;
$exit = 0;
$wait = static function (string $path, float $seconds) use ($directory): void {
    $deadline = hrtime(true) + (int) ($seconds * 1e9);
    while (!is_file($path)) {
        if (hrtime(true) > $deadline || is_file($directory . '/stop')) {
            throw new RuntimeException('Barrier timeout: ' . basename($path));
        }
        usleep(1000);
    }
};
try {
    $connection = Baseline::connect($config['database']);
    $durability = Baseline::durability($connection);
    if (!Baseline::isDurabilityEquivalent($durability)) {
        throw new RuntimeException('Durability mismatch');
    }
    $recorder->header(['durability' => $durability]);
    $beforeReady = hrtime(true);
    file_put_contents($directory . '/ready/' . $label . '.ready', json_encode(['event' => 'ready', 'pid' => getmypid(), 'clock_ns' => $beforeReady]));
    $wait($directory . '/go/' . $label, Config::STARTUP_TIMEOUT_S + $workload['timeout_s']);
    // A parent timestamp written before this read must be <= this timestamp on the same clock.
    $parentNs = (int) file_get_contents($directory . '/go/' . $label);
    $start = hrtime(true);
    $recorder->sample(['kind' => 'clock_check', 'before_ready_ns' => $beforeReady, 'parent_go_ns' => $parentNs, 'after_go_ns' => $start, 'valid' => $beforeReady <= $parentNs && $parentNs <= $start]);
    if ($beforeReady > $parentNs || $parentNs > $start) {
        throw new RuntimeException('Cross-process monotonic clock ordering failed');
    }
    if ($role === 'consumer') {
        $receiver = new Receiver($connection, $workload['queues'], $recorder, $directory, $index, $workload['name'] === 'roundtrip');
        $bus = new MessageBus([new HandleMessageMiddleware(new HandlersLocator([BenchMessage::class => [$receiver->enter(...)]]))]);
        $events = new EventDispatcher();
        $events->addListener(WorkerRunningEvent::class, static function (WorkerRunningEvent $event) use ($directory, $receiver): void {
            if (is_file($directory . '/stop') || $receiver->failed) {
                $event->getWorker()->stop();
            }
        });
        $worker = new Worker(['baseline' => $receiver], $bus, $events);
        $worker->run(['sleep' => Config::POLL_SLEEP_US, 'time_limit' => $workload['timeout_s']]);
        if ($receiver->failed) {
            throw new RuntimeException('Consumer delivery failed');
        }
    } else {
        $jobs = [];
        if ($role === 'publisher') {
            for ($i = 0; $i < $workload['publishers'][$index]['count']; ++$i) {
                $jobs[] = [$workload['queues'][$i % count($workload['queues'])], 0, true];
            }
        } else {
            foreach ($workload['prefill']['queues'] ?? $workload['queues'] as $queue) {
                for ($i = 0; $i < $workload['prefill']['per_queue']; ++$i) {
                    $jobs[] = [$queue, $workload['prefill']['delay_ms'] ?? 0, true];
                }
            }
            foreach ($workload['probes'] ?? [] as $probe) {
                $jobs[] = [$probe['queue'], $probe['delay_ms'], false];
            }
        }
        $transports = [];
        $pacing = $role === 'publisher' ? $workload['pacing_us'] : ($workload['prefill']['stagger_us'] ?? 0);
        foreach ($jobs as $i => [$queue, $delay, $measured]) {
            if (is_file($directory . '/stop')) { throw new RuntimeException('Run stopped'); }
            $target = $start + $i * $pacing * 1000;
            while (hrtime(true) < $target) {
                usleep((int) min(1000, max(1, ($target - hrtime(true)) / 1000)));
            }
            if (hrtime(true) - $start > $workload['timeout_s'] * 1e9) {
                throw new RuntimeException('Publisher exceeded run bound');
            }
            $size = Config::payloadSize($i);
            $msg = BenchMessage::generate($role . '-' . $index . '-' . $i, $queue, $size);
            $transport = $transports[$queue] ??= Baseline::transport($connection, $queue);
            $invoke = hrtime(true);
            $wall = microtime(true);
            $ok = false;
            $error = null;
            $rowId = null;
            try {
                $result = $transport->send(new Envelope($msg, [new DelayStamp($delay)]));
                $confirm = hrtime(true);
                $ok = true;
                $rowId = $result->last(TransportMessageIdStamp::class)?->getId();
            } catch (Throwable $e) {
                $confirm = hrtime(true);
                $error = $e::class . ': ' . $e->getMessage();
                $exit = 1;
            }
            $recorder->sample([
                'kind' => 'send', 'msg' => $msg->corrId, 'queue' => $queue, 'transport_id' => $rowId,
                'ok' => $ok, 'error' => $error, 'size' => $size, 'measured' => $measured,
                't_invoke_ns' => $invoke, 't_confirm_ns' => $confirm, 'duration_ms' => ($confirm - $invoke) / 1e6,
                'schedule_lag_ms' => $pacing ? ($invoke - $target) / 1e6 : null,
                'requested_deadline_wall' => $wall + $delay / 1000, 'delay_ms' => $delay,
            ]);
            if ($ok && $workload['name'] === 'roundtrip') {
                $wait($directory . '/acks/' . $msg->corrId, max(0.01, $workload['timeout_s'] - (hrtime(true) - $start) / 1e9));
            }
        }
    }
} catch (Throwable $e) {
    $recorder->error($role, $e);
    fwrite(STDERR, $e::class . ': ' . $e->getMessage() . "\n");
    $exit = 1;
} finally {
    $connection?->close();
    $recorder->foot(['poll_totals' => $receiver?->polls ?? [], 'php_peak_bytes' => memory_get_peak_usage(true), 'exit_code' => $exit]);
}
exit($exit);
