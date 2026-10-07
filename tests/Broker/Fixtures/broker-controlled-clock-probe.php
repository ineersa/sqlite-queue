<?php

declare(strict_types=1);

/*
 * Child probe for BrokerProcessTest WAIT lifecycle evidence that needs a controlled clock.
 *
 * It constructs the real Broker through BrokerFactory with an injected millisecond clock and
 * serves the ordinary Unix-socket protocol. A second control socket accepts JSON line commands
 * so the parent can observe notifier waiters, advance the clock, and fire the deadline timer
 * without production test APIs. Arguments: <database> <endpoint> <control> <now_ms>
 */

use Ineersa\SqliteQueue\Broker\Broker;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Broker\QueueNotifier;
use Ineersa\SqliteQueue\Sqlite\SqliteWorkerContextFactory;
use Revolt\EventLoop;
use Revolt\EventLoop\Internal\TimerCallback;

use function Amp\async;
use function Amp\Socket\listen;

require dirname(__DIR__, 3).'/vendor/autoload.php';

$database = $argv[1] ?? '';
$endpoint = $argv[2] ?? '';
$control = $argv[3] ?? '';
$now = (int) ($argv[4] ?? 0);
if ('' === $database || '' === $endpoint || '' === $control || $now < 0) {
    fwrite(\STDERR, "usage: broker-controlled-clock-probe.php <database> <endpoint> <control> <now_ms>\n");
    exit(1);
}

$clockFile = $database.'.clock';
file_put_contents($clockFile, (string) $now);
$workers = new SqliteWorkerContextFactory([
    dirname(__DIR__, 2).'/Sqlite/Fixtures/worker-controlled-clock.php',
    (string) $now,
    $clockFile,
]);
$broker = (new BrokerFactory(
    $database,
    $endpoint,
    clock: static function () use (&$now, $clockFile): int {
        file_put_contents($clockFile, (string) $now);

        return $now;
    },
    workers: $workers,
))->create();

$controlServer = listen('unix://'.$control);
async(static function () use ($controlServer, $broker, &$now, $clockFile): void {
    while (null !== ($socket = $controlServer->accept())) {
        async(static function () use ($socket, $broker, &$now, $clockFile): void {
            $buffer = '';
            try {
                while (null !== ($chunk = $socket->read())) {
                    $buffer .= $chunk;
                    while (false !== ($pos = strpos($buffer, "\n"))) {
                        $line = substr($buffer, 0, $pos);
                        $buffer = substr($buffer, $pos + 1);
                        $command = json_decode($line, true, 8, \JSON_THROW_ON_ERROR);
                        if (!is_array($command)) {
                            throw new RuntimeException('Control command must be a JSON object.');
                        }
                        $socket->write(json_encode(controlReply($broker, $now, $clockFile, $command), \JSON_THROW_ON_ERROR)."\n");
                    }
                }
            } finally {
                $socket->close();
            }
        });
    }
});

$code = $broker->run(static function (array $event): void {
    fwrite(\STDOUT, json_encode($event, \JSON_THROW_ON_ERROR)."\n");
});
$controlServer->close();

exit($code);

/**
 * @param array<string, mixed> $command
 *
 * @return array<string, mixed>
 */
function controlReply(Broker $broker, int &$now, string $clockFile, array $command): array
{
    return match ($command['op'] ?? null) {
        'get_now' => ['now' => $now],
        'set_now' => setNow($now, $clockFile, expectInt($command, 'now')),
        'waiter_count' => ['count' => waiterCount($broker)],
        'deadline' => deadline($broker, expectInt($command, 'ready_at')),
        'fire' => fire(expectString($command, 'timer_id')),
        'stop' => tap($broker->stop(), ['ok' => true]),
        default => throw new RuntimeException('Unknown control operation.'),
    };
}

/**
 * @return array{now: int}
 */
function setNow(int &$now, string $clockFile, int $value): array
{
    $now = $value;
    file_put_contents($clockFile, (string) $now);

    return ['now' => $now];
}

function waiterCount(Broker $broker): int
{
    $notifier = (new ReflectionProperty(Broker::class, 'notifier'))->getValue($broker);
    $waiters = (new ReflectionProperty(QueueNotifier::class, 'waiters'))->getValue($notifier);
    $total = 0;
    foreach ($waiters as $list) {
        $total += count($list);
    }

    return $total;
}

/**
 * @return array{timer_id: ?string, ready_at: int, enabled: bool}
 */
function deadline(Broker $broker, int $readyAt): array
{
    $notifier = (new ReflectionProperty(Broker::class, 'notifier'))->getValue($broker);
    $watches = (new ReflectionProperty(QueueNotifier::class, 'watches'))->getValue($notifier);
    foreach ($watches as $watch) {
        if (!$watch->querying && !$watch->dirty && $watch->timerReadyAt === $readyAt && null !== $watch->timerId) {
            $timerId = $watch->timerId;
            // Freeze before the reply leaves this process so real elapsed time cannot replace the ID.
            EventLoop::disable($timerId);

            return ['timer_id' => $timerId, 'ready_at' => $readyAt, 'enabled' => EventLoop::isEnabled($timerId)];
        }
    }

    return ['timer_id' => null, 'ready_at' => $readyAt, 'enabled' => false];
}

/**
 * @return array{ok: bool}
 */
function fire(string $timerId): array
{
    $driver = EventLoop::getDriver();
    $callbacks = null;
    $reflection = new ReflectionObject($driver);
    while (null !== $reflection) {
        if ($reflection->hasProperty('callbacks')) {
            $callbacks = $reflection->getProperty('callbacks')->getValue($driver);
            break;
        }
        $reflection = $reflection->getParentClass() ?: null;
    }
    $callback = is_array($callbacks) ? ($callbacks[$timerId] ?? null) : null;
    if (!$callback instanceof TimerCallback) {
        return ['ok' => false];
    }
    EventLoop::cancel($timerId);
    ($callback->closure)($timerId);

    return ['ok' => true];
}

/**
 * @param array<string, mixed> $command
 */
function expectInt(array $command, string $field): int
{
    $value = $command[$field] ?? null;
    if (!is_int($value)) {
        throw new RuntimeException('Control field '.$field.' must be an integer.');
    }

    return $value;
}

/**
 * @param array<string, mixed> $command
 */
function expectString(array $command, string $field): string
{
    $value = $command[$field] ?? null;
    if (!is_string($value) || '' === $value) {
        throw new RuntimeException('Control field '.$field.' must be a non-empty string.');
    }

    return $value;
}

/**
 * @param array<string, mixed> $reply
 *
 * @return array<string, mixed>
 */
function tap(mixed $ignored, array $reply): array
{
    return $reply;
}
