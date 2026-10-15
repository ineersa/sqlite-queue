<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue {
    use function array_key_first as selectPendingKey;
    use function array_splice as detachPendingKeepalives;

    /** The child alone intercepts the dequeue boundary; production Client is unchanged. */
    function injectKeepaliveBoundary(string $boundary): void
    {
        $callback = $GLOBALS['keepalive_boundary'] ?? false;
        if ($callback instanceof \Closure) {
            unset($GLOBALS['keepalive_boundary']);
            $callback($boundary);
        }
    }

    function array_key_first(array $array): int|string|null
    {
        $key = selectPendingKey($array);
        // Pre-fix: the key is selected, but the caller has not retrieved its entry yet.
        injectKeepaliveBoundary('array_key_first');

        return $key;
    }

    function array_splice(array &$array, int $offset): array
    {
        $snapshot = detachPendingKeepalives($array, $offset);
        // Post-fix: the atomic detach has happened, before iteration can start.
        injectKeepaliveBoundary('array_splice');

        return $snapshot;
    }
}

namespace {
    use Amp\ByteStream\BufferedReader;
    use Amp\Process\Process;
    use Amp\TimeoutCancellation;
    use Ineersa\SqliteQueue\Client;
    use Ineersa\SqliteQueue\Protocol\Operation;
    use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;

    use function Amp\async;
    use function Amp\ByteStream\buffer;

    require dirname(__DIR__, 3).'/vendor/autoload.php';
    $source = getenv('KEEPALIVE_REVIEW_CLIENT_SOURCE');
    if (false !== $source) {
        require $source;
    }
    $operation = Operation::from($argv[1]);
    $directory = new IsolatedDatabase();
    $endpoint = $directory->path('boundary.sock');
    $peer = Process::start([\PHP_BINARY, __DIR__.'/keepalive-peer.php', $endpoint, $operation->value, 'malformed']);
    $stderr = async(static fn (): string => buffer($peer->getStderr()));
    $reader = new BufferedReader($peer->getStdout());
    if ('ready' !== $reader->readUntil("\n", new TimeoutCancellation(5))) {
        throw new RuntimeException('Boundary peer did not start.');
    }
    $client = Client::connect($endpoint);
    $reader->readUntil("\n", new TimeoutCancellation(5));
    $report = ['warnings' => [], 'errors' => [], 'signal_errors' => []];
    set_error_handler(static function (int $severity, string $message) use (&$report): bool {
        $report['warnings'][] = $message;

        return true;
    });
    $observer = static function (Throwable $error) use (&$report): void {
        $report['errors'][] = ['class' => $error::class, 'message' => $error->getMessage()];
    };
    pcntl_async_signals(true);
    $minimum = 5000;
    pcntl_signal(\SIGALRM, static function () use ($client, $observer, &$minimum, &$report): void {
        try {
            $report['signal_result'] = $client->keepaliveSignalSafe('active-sibling', $minimum, $observer);
        } catch (Throwable $error) {
            $report['signal_errors'][] = $error::class;
        }
    });
    try {
        $original = async(static function () use ($client, $operation): ?int {
            if (Operation::Send === $operation) {
                return $client->send('jobs', 'payload');
            }
            $client->acknowledge('settled-receipt');

            return null;
        });
        $reader->readUntil("\n", new TimeoutCancellation(5));
        posix_kill(getmypid(), \SIGALRM);
        $GLOBALS['keepalive_boundary'] = static function (string $boundary) use ($client, &$minimum, &$report): void {
            $report['boundary'] = $boundary;
            $report['busy_at_boundary'] = (new ReflectionProperty(Client::class, 'busy'))->getValue($client);
            $report['flushing_at_boundary'] = (new ReflectionProperty(Client::class, 'flushingKeepalives'))->getValue($client);
            $minimum = 30_000;
            posix_kill(getmypid(), \SIGALRM);
        };
        $peer->getStdin()->write("release\n");
        try {
            $report['original_result'] = $original->await(new TimeoutCancellation(5));
            $report['original_confirmed'] = true;
        } catch (Throwable $error) {
            $report['original_confirmed'] = false;
            $report['original_error'] = $error::class.': '.$error->getMessage();
        }
    } finally {
        unset($GLOBALS['keepalive_boundary']);
        pcntl_signal(\SIGALRM, \SIG_DFL);
        restore_error_handler();
        $client->close();
        $report['peer_exit'] = $peer->join(new TimeoutCancellation(5));
        $report['peer_stderr'] = $stderr->await(new TimeoutCancellation(5));
        $directory->remove();
    }
    echo json_encode($report, \JSON_THROW_ON_ERROR)."\n";
}
