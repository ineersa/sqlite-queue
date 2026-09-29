<?php

declare(strict_types=1);

/*
 * Test-only entry point for shutdown-boundary diagnostics.
 *
 * It runs the real Broker Command through Symfony Console the same way bin/sqlite-queue does.
 * It does not register signals, open the broker, or create an event-loop driver itself. The only
 * extra behaviour is a ConsoleOutput wrapper that records the SIGTERM handler identity when the
 * real readiness JSON line is written. Reflection stays in this fixture.
 *
 * Arguments: <database> <endpoint> <trace-file> <handler-probe-file>
 */

use Ineersa\SqliteQueue\Broker\Command;
use Symfony\Component\Console\Application;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\ConsoleOutput;

require dirname(__DIR__, 3).'/vendor/autoload.php';

$database = $argv[1] ?? '';
$endpoint = $argv[2] ?? '';
$tracePath = $argv[3] ?? '';
$handlerProbePath = $argv[4] ?? '';
if ('' === $database || '' === $endpoint || '' === $tracePath || '' === $handlerProbePath) {
    fwrite(\STDERR, json_encode(['event' => 'failed', 'error_type' => 'InvalidArgumentException'], \JSON_THROW_ON_ERROR)."\n");
    exit(1);
}

/**
 * Observes the real readiness JSON line, records the SIGTERM handler once, then delegates normally.
 */
final class ReadyHandlerProbeOutput extends ConsoleOutput
{
    private bool $recorded = false;

    public function __construct(
        private readonly string $handlerProbePath,
    ) {
        parent::__construct();
    }

    public function writeln(string|iterable $messages, int $options = self::OUTPUT_NORMAL): void
    {
        // The command emits readiness as one JSON string. Leave iterable output untouched.
        if (is_string($messages)) {
            $this->observeReady($messages);
        }
        parent::writeln($messages, $options);
    }

    private function observeReady(string $message): void
    {
        if ($this->recorded) {
            return;
        }
        $line = trim($message);
        if ('' === $line || !str_starts_with($line, '{')) {
            return;
        }
        try {
            $event = json_decode($line, true, 16, \JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return;
        }
        if (!is_array($event) || ($event['event'] ?? null) !== 'ready') {
            return;
        }

        $handler = pcntl_signal_get_handler(\SIGTERM);
        $probe = [
            'event' => 'sigterm-handler',
            'pid' => (int) getmypid(),
            'type' => get_debug_type($handler),
        ];
        if ($handler instanceof Closure) {
            $reflection = new ReflectionFunction($handler);
            $probe['scope'] = $reflection->getClosureScopeClass()?->getName();
            $probe['name'] = $reflection->getName();
            $probe['file'] = $reflection->getFileName() ?: null;
            $probe['start_line'] = $reflection->getStartLine() ?: null;
        }
        @file_put_contents($this->handlerProbePath, json_encode($probe, \JSON_THROW_ON_ERROR)."\n");
        $this->recorded = true;
    }
}

$fail = static function (string $errorType, string $message): int {
    fwrite(\STDERR, json_encode(['event' => 'failed', 'error_type' => $errorType, 'message' => $message], \JSON_THROW_ON_ERROR)."\n");

    return 1;
};

if (!class_exists(Application::class)) {
    exit($fail('RuntimeException', 'The broker command line requires symfony/console.'));
}

$output = new ReadyHandlerProbeOutput($handlerProbePath);
$output->setDecorated(false);
$application = new Application('SQLite queue broker');
$application->setCatchExceptions(false);
$application->setAutoExit(false);
$application->addCommand(new Command());

try {
    $input = new ArrayInput([
        'command' => 'broker',
        '--database' => $database,
        '--endpoint' => $endpoint,
        '--trace-file' => $tracePath,
    ]);
    exit($application->run($input, $output));
} catch (Throwable $error) {
    exit($fail($error::class, 'The broker command line failed before the broker could report.'));
}
