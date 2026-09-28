<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Ineersa\SqliteQueue\ProtocolException;
use Ineersa\SqliteQueue\Queue;

/** Translates protocol v1 request frames to typed models and back. Raw control arrays stay here. */
final class RequestCodec
{
    public static function encode(Request $request): Frame
    {
        return match (true) {
            $request instanceof HelloRequest => new Frame([
                'v' => Frame::VERSION, 'id' => $request->id, 'op' => Operation::Hello->value,
            ]),
            $request instanceof SendRequest => new Frame([
                'v' => Frame::VERSION, 'id' => $request->id, 'op' => Operation::Send->value,
                'queue' => $request->queue, 'delay' => $request->delay,
            ], $request->body, $request->headers),
            $request instanceof ReceiveRequest => new Frame([
                'v' => Frame::VERSION, 'id' => $request->id, 'op' => Operation::Receive->value,
                'queue' => $request->queue,
            ]),
            $request instanceof AcknowledgeRequest => new Frame([
                'v' => Frame::VERSION, 'id' => $request->id, 'op' => Operation::Acknowledge->value,
                'receipt' => $request->receipt,
            ]),
            $request instanceof RejectRequest => new Frame([
                'v' => Frame::VERSION, 'id' => $request->id, 'op' => Operation::Reject->value,
                'receipt' => $request->receipt,
            ]),
            default => throw new \LogicException('Unsupported request type.'),
        };
    }

    public static function decode(Frame $frame, int $expected): Request
    {
        $control = $frame->control;
        self::version($control);
        $id = self::sequenceId($control, $expected);
        $operation = self::operation($control);
        self::allowedFields($control, $operation);
        self::handshake($operation, $expected, $frame);
        self::payload($operation, $frame);

        return match ($operation) {
            Operation::Hello => new HelloRequest($id),
            Operation::Send => new SendRequest($id, self::queue($control), self::delay($control), $frame->body, $frame->headers),
            Operation::Receive => new ReceiveRequest($id, self::queue($control)),
            Operation::Acknowledge => new AcknowledgeRequest($id, self::receipt($control)),
            Operation::Reject => new RejectRequest($id, self::receipt($control)),
        };
    }

    /** @param array<string, mixed> $control */
    private static function version(array $control): void
    {
        if (($control['v'] ?? null) !== Frame::VERSION) {
            throw new ProtocolException(ErrorCode::UnsupportedProtocolVersion, 'Unsupported protocol version.');
        }
    }

    /** @param array<string, mixed> $control */
    private static function sequenceId(array $control, int $expected): int
    {
        $id = $control['id'] ?? null;
        if (!\is_int($id)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid request sequence or operation.');
        }
        if ($id !== $expected) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid request sequence or operation.');
        }

        return $id;
    }

    /** @param array<string, mixed> $control */
    private static function operation(array $control): Operation
    {
        $name = $control['op'] ?? null;
        if (!\is_string($name)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid request sequence or operation.');
        }
        $operation = Operation::tryFrom($name);
        if (null === $operation) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unsupported operation.');
        }

        return $operation;
    }

    /** @param array<string, mixed> $control */
    private static function allowedFields(array $control, Operation $operation): void
    {
        $fields = match ($operation) {
            Operation::Hello => [],
            Operation::Send => ['queue', 'delay'],
            Operation::Receive => ['queue'],
            Operation::Acknowledge, Operation::Reject => ['receipt'],
        };
        $extra = array_diff(array_keys($control), ['v', 'id', 'op', 'body_length', 'headers_length', ...$fields]);
        if ([] !== $extra) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unsupported control field.');
        }
    }

    private static function handshake(Operation $operation, int $expected, Frame $frame): void
    {
        if (0 === $expected && Operation::Hello !== $operation) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Handshake required.');
        }
        if (0 !== $expected && Operation::Hello === $operation) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Handshake required.');
        }
        if (Operation::Hello === $operation && ('' !== $frame->body || '' !== $frame->headers)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Handshake required.');
        }
    }

    private static function payload(Operation $operation, Frame $frame): void
    {
        if (Operation::Send === $operation) {
            return;
        }
        if ('' !== $frame->body || '' !== $frame->headers) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unexpected application payload.');
        }
    }

    /** @param array<string, mixed> $control */
    private static function queue(array $control): string
    {
        $queue = $control['queue'] ?? null;
        if (!\is_string($queue)) {
            throw new ProtocolException(ErrorCode::InvalidQueueName, 'Invalid queue name.');
        }
        if (1 !== preg_match(Queue::NAME_PATTERN, $queue)) {
            throw new ProtocolException(ErrorCode::InvalidQueueName, 'Invalid queue name.');
        }

        return $queue;
    }

    /** @param array<string, mixed> $control */
    private static function delay(array $control): int
    {
        $delay = $control['delay'] ?? null;
        if (!\is_int($delay)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delay.');
        }
        if ($delay < 0) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delay.');
        }

        return $delay;
    }

    /** @param array<string, mixed> $control */
    private static function receipt(array $control): string
    {
        $receipt = $control['receipt'] ?? null;
        if (!\is_string($receipt)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Missing receipt.');
        }

        return $receipt;
    }
}
