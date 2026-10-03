<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

final class BrokenDecodeSerializer implements SerializerInterface
{
    public function decode(array $encodedEnvelope): Envelope
    {
        if (\is_callable([MessageDecodingFailedException::class, 'wrap'])) {
            return MessageDecodingFailedException::wrap($encodedEnvelope, 'native decode failure');
        }

        throw new MessageDecodingFailedException('native decode failure');
    }

    public function encode(Envelope $envelope): array
    {
        return ['body' => 'x', 'headers' => ['type' => 'probe']];
    }
}
