<?php

declare(strict_types=1);

use Amp\ByteStream\StreamChannel;
use Amp\Sync\Channel;
use Ineersa\SqliteQueue\Tests\Support\WorkerFaultAction;

use function Amp\Socket\connect;

final class QueueFaultBarrier
{
    private bool $malformed = false;

    public function __construct(private readonly Channel $control)
    {
    }

    public function at(string $stage): void
    {
        $this->control->send($stage);
        $action = $this->control->receive();
        if (!is_string($action)) {
            throw new RuntimeException('Fault barrier action must be a string.');
        }
        match (WorkerFaultAction::from($action)) {
            WorkerFaultAction::Continue => null,
            WorkerFaultAction::ExitBeforeCommit => exit(1),
            WorkerFaultAction::LostReply => exit(0),
            WorkerFaultAction::MalformedReply => $this->malformed = true,
        };
    }

    /** @param array<string, mixed> $value
     * @return array<string, mixed>
     */
    public function reply(array $value): array
    {
        return $this->malformed ? ['outcome' => 'commit', 'data' => 'invalid'] : $value;
    }
}

// Exact-anchor hooks exist only in temporary source copies, never in the dependency.
require $argv[1];
$socket = connect($argv[3]);
$GLOBALS['queueFaultBarrier'] = new QueueFaultBarrier(new StreamChannel($socket, $socket));

return require $argv[2];
