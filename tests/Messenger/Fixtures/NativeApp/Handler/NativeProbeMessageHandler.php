<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Handler;

use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\Message\NativeProbeMessage;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
final class NativeProbeMessageHandler
{
    public const string MODE_ACK = 'ack';
    public const string MODE_FAIL_THEN_SUCCEED = 'fail_then_succeed';
    public const string MODE_ALWAYS_FAIL = 'always_fail';

    /** @var list<string> */
    public static array $handled = [];
    public static int $attempts = 0;
    public static string $mode = self::MODE_ACK;

    public function __construct(private readonly string $recordPath)
    {
    }

    public function __invoke(NativeProbeMessage $message): void
    {
        ++self::$attempts;
        match (self::$mode) {
            self::MODE_FAIL_THEN_SUCCEED => $this->failThenSucceed($message),
            self::MODE_ALWAYS_FAIL => throw new \RuntimeException('always fails: '.$message->body),
            default => $this->record($message),
        };
    }

    public static function reset(): void
    {
        self::$handled = [];
        self::$attempts = 0;
        self::$mode = self::MODE_ACK;
    }

    private function failThenSucceed(NativeProbeMessage $message): void
    {
        if (1 === self::$attempts) {
            throw new \RuntimeException('first delivery fails');
        }
        $this->record($message);
    }

    private function record(NativeProbeMessage $message): void
    {
        self::$handled[] = $message->body;
        if (false === file_put_contents($this->recordPath, json_encode(['body' => $message->body], \JSON_THROW_ON_ERROR)."\n", \FILE_APPEND)) {
            throw new \RuntimeException('Could not record the test handler effect.');
        }
    }
}
