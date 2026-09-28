<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Protocol;

use Ineersa\SqliteQueue\Delivery;
use Ineersa\SqliteQueue\ProtocolException;

/** Translates protocol v1 response frames to typed models and back. Raw control arrays stay here. */
final class ResponseCodec
{
    public static function encode(Response $response): Frame
    {
        return match (true) {
            $response instanceof HelloResponse => new Frame([
                'v' => Frame::VERSION, 'id' => $response->id, 'ok' => true,
                'result' => ['max_payload' => $response->maxPayload],
            ]),
            $response instanceof SentResponse => new Frame([
                'v' => Frame::VERSION, 'id' => $response->id, 'ok' => true,
                'result' => $response->messageId,
            ]),
            $response instanceof ReceivedResponse => new Frame([
                'v' => Frame::VERSION, 'id' => $response->id, 'ok' => true,
                'result' => [
                    'id' => $response->delivery->id, 'queue' => $response->delivery->queue,
                    'receipt' => $response->delivery->receipt, 'available_at' => $response->delivery->availableAt,
                    'reserved_until' => $response->delivery->reservedUntil,
                ],
            ], $response->delivery->body, $response->delivery->headers),
            $response instanceof EmptyReceiveResponse => new Frame([
                'v' => Frame::VERSION, 'id' => $response->id, 'ok' => true, 'result' => null,
            ]),
            $response instanceof SettledResponse => new Frame([
                'v' => Frame::VERSION, 'id' => $response->id, 'ok' => true, 'result' => null,
            ]),
            $response instanceof FailedResponse => new Frame([
                'v' => Frame::VERSION, 'id' => $response->id, 'ok' => false,
                'error' => ['code' => $response->code->value],
            ]),
            default => throw new \LogicException('Unsupported response type.'),
        };
    }

    public static function decode(Frame $frame, Request $request): Response
    {
        $control = $frame->control;
        self::version($control);
        self::correlation($control, $request);
        if (!self::success($control)) {
            return self::failure($control, $frame, $request);
        }
        self::resultPresent($control);

        return match (true) {
            $request instanceof HelloRequest => self::hello($control, $frame, $request),
            $request instanceof SendRequest => self::sent($control, $frame, $request),
            $request instanceof ReceiveRequest => self::received($control, $frame, $request),
            $request instanceof AcknowledgeRequest => self::settled($control, $frame, $request),
            $request instanceof RejectRequest => self::settled($control, $frame, $request),
            default => throw new \LogicException('Unsupported request type.'),
        };
    }

    /** @param array<string, mixed> $control */
    private static function version(array $control): void
    {
        if (($control['v'] ?? null) !== Frame::VERSION) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
    }

    /** @param array<string, mixed> $control */
    private static function correlation(array $control, Request $request): void
    {
        $id = $control['id'] ?? null;
        if (!\is_int($id)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
        if ($id !== $request->id()) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
    }

    /** @param array<string, mixed> $control */
    private static function success(array $control): bool
    {
        $ok = $control['ok'] ?? null;
        if (!\is_bool($ok)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }

        return $ok;
    }

    /** @param array<string, mixed> $control */
    private static function resultPresent(array $control): void
    {
        if (!\array_key_exists('result', $control)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid or uncorrelated broker response.');
        }
    }

    /** @param array<string, mixed> $control */
    private static function failure(array $control, Frame $frame, Request $request): FailedResponse
    {
        self::emptyPayload($frame, 'Failure must not carry a payload.');
        if (\array_key_exists('result', $control)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Failure must not carry a result.');
        }
        $error = $control['error'] ?? null;
        if (!\is_array($error)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unknown broker error.');
        }
        $name = $error['code'] ?? null;
        if (!\is_string($name)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unknown broker error.');
        }
        $code = ErrorCode::tryFrom($name);
        if (null === $code) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Unknown broker error.');
        }

        return new FailedResponse($request->id(), $code);
    }

    /** @param array<string, mixed> $control */
    private static function hello(array $control, Frame $frame, HelloRequest $request): HelloResponse
    {
        self::emptyPayload($frame, 'Hello must not carry a payload.');
        $result = $control['result'];
        if (!\is_array($result)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid hello payload.');
        }
        $maxPayload = $result['max_payload'] ?? null;
        if (Limits::MAX_PAYLOAD !== $maxPayload) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid hello payload.');
        }

        return new HelloResponse($request->id(), $maxPayload);
    }

    /** @param array<string, mixed> $control */
    private static function sent(array $control, Frame $frame, SendRequest $request): SentResponse
    {
        self::emptyPayload($frame, 'Send confirmation must not carry a payload.');
        $messageId = $control['result'];
        if (!\is_int($messageId)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid send confirmation.');
        }
        if ($messageId < 1) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid send confirmation.');
        }

        return new SentResponse($request->id(), $messageId);
    }

    /** @param array<string, mixed> $control */
    private static function received(array $control, Frame $frame, ReceiveRequest $request): Response
    {
        $result = $control['result'];
        if (null === $result) {
            self::emptyPayload($frame, 'Empty delivery must not carry a payload.');

            return new EmptyReceiveResponse($request->id());
        }
        if (!\is_array($result)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delivery.');
        }
        $id = $result['id'] ?? null;
        if (!\is_int($id)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delivery.');
        }
        if ($id < 1) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delivery.');
        }
        if (($result['queue'] ?? null) !== $request->queue) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delivery.');
        }
        $receipt = $result['receipt'] ?? null;
        if (!\is_string($receipt)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delivery.');
        }
        $availableAt = $result['available_at'] ?? null;
        if (!\is_int($availableAt)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delivery.');
        }
        $reservedUntil = $result['reserved_until'] ?? null;
        if (!\is_int($reservedUntil)) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Invalid delivery.');
        }

        return new ReceivedResponse($request->id(), new Delivery(
            $id, $request->queue, $frame->body, $frame->headers, $receipt, $availableAt, $reservedUntil,
        ));
    }

    /** @param array<string, mixed> $control */
    private static function settled(array $control, Frame $frame, Request $request): SettledResponse
    {
        if (null !== $control['result']) {
            throw new ProtocolException(ErrorCode::InvalidRequest, 'Settlement must not carry a result.');
        }
        self::emptyPayload($frame, 'Settlement must not carry a payload.');

        return new SettledResponse($request->id());
    }

    private static function emptyPayload(Frame $frame, string $message): void
    {
        if ('' !== $frame->body || '' !== $frame->headers) {
            throw new ProtocolException(ErrorCode::InvalidRequest, $message);
        }
    }
}
