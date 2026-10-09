<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Messenger;

use Amp\Cancellation;
use Amp\CancelledException;
use Ineersa\SqliteQueue\DTO\DeliveryDTO;
use Ineersa\SqliteQueue\Exception\InvalidReceiptException;
use Ineersa\SqliteQueue\Exception\TransportException as ClientTransportException;
use Ineersa\SqliteQueue\Messenger\Stamp\DeliveryReceiptStamp;
use Ineersa\SqliteQueue\Protocol\ProtocolException;
use Ineersa\SqliteQueue\ValueObject\QueueName;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\CloseableTransportInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Symfony Messenger adapter over separate operation and notification connections for one queue.
 *
 * Serialization stays at this boundary. The broker stores opaque body and headers
 * strings. Headers crossing this adapter are a JSON object of UTF-8 string values;
 * NUL is preserved, invalid UTF-8 is rejected. get() claims at most one committed
 * delivery. ACK and reject settle the original receipt only and never retry.
 */
final class Transport implements TransportInterface, CloseableTransportInterface
{
    /** Failure-envelope header that preserves opaque broker header bytes when they are not a JSON object. */
    public const string RAW_HEADERS_HEADER = 'x-sqlite-queue-raw-headers';
    private const int JSON_DEPTH = 32;

    /**
     * Owners must acquire distinct broker Clients. Cancelling notification WAIT closes that
     * connection; the operation connection must remain live for deferred reservation ACKs.
     * Both owners belong to this transport and are closed with it. brokerEndpoint identifies the
     * broker socket for shared multi-queue notification waits across transports.
     */
    public function __construct(
        private readonly BrokerConnection $operations,
        private readonly QueueName $queue,
        private readonly SerializerInterface $serializer,
        private readonly BrokerConnection $notifications,
        private readonly string $brokerEndpoint,
    ) {
        if ($operations === $notifications) {
            throw new \InvalidArgumentException('Operation and notification connection owners must be distinct.');
        }
        if ('' === $brokerEndpoint || !str_starts_with($brokerEndpoint, '/')) {
            throw new \InvalidArgumentException('The broker endpoint must be a non-empty absolute filesystem path.');
        }
    }

    private function __clone(): void
    {
    }

    /**
     * Nonblocking receive of at most one committed claim.
     *
     * `$fetchSize` defaults to 1 and is accepted for Messenger 8.1+ callers. Values
     * greater than 1 are ignored because batch receive stays deferred.
     *
     * @return iterable<int, Envelope>
     */
    public function get(int $fetchSize = 1): iterable
    {
        if ($fetchSize < 1) {
            throw new \InvalidArgumentException('fetchSize must be a positive integer.');
        }

        try {
            $delivery = $this->operations->client()->receive($this->queue->value);
        } catch (ClientTransportException|ProtocolException $error) {
            throw new TransportException('Could not receive from the queue broker.', 0, $error);
        }

        if (null === $delivery) {
            return [];
        }

        return [$this->createEnvelopeFromDelivery($delivery)];
    }

    public function ack(Envelope $envelope): void
    {
        $this->settle($envelope, acknowledge: true);
    }

    public function reject(Envelope $envelope): void
    {
        $this->settle($envelope, acknowledge: false);
    }

    public function send(Envelope $envelope): Envelope
    {
        $envelope = $envelope
            ->withoutAll(DeliveryReceiptStamp::class)
            ->withoutAll(TransportMessageIdStamp::class);

        $encoded = $this->serializer->encode($envelope);
        if (!\array_key_exists('body', $encoded) || !\is_string($encoded['body'])) {
            throw new \InvalidArgumentException('Serializer encode() must return a string body.');
        }
        $headers = \array_key_exists('headers', $encoded)
            ? $this->encodeHeaders($encoded['headers'])
            : '{}';
        $delay = $this->delayMilliseconds($envelope);

        try {
            $id = $this->operations->client()->send($this->queue->value, $encoded['body'], $headers, $delay);
        } catch (ClientTransportException|ProtocolException $error) {
            throw new TransportException('Could not send to the queue broker.', 0, $error);
        }

        return $envelope->with(new TransportMessageIdStamp($id));
    }

    /**
     * Bounded readiness hint for the configured queue. True means try get(); false is a
     * normal timeout or empty probe. Cancellation wraps as TransportException with the
     * CancelledException preserved; inspect the passed token for requested shutdown.
     */
    public function wait(int $timeoutMilliseconds, Cancellation $cancellation): bool
    {
        try {
            return $this->notifications->client($cancellation)->wait($this->queue->value, $timeoutMilliseconds, $cancellation);
        } catch (CancelledException $error) {
            throw new TransportException('Queue wait was cancelled.', 0, $error);
        } catch (ClientTransportException|ProtocolException $error) {
            throw new TransportException('Could not wait on the queue broker.', 0, $error);
        }
    }

    /**
     * Bounded readiness hint across several queues on this transport's notification owner.
     *
     * True means try get() on the selected receivers; false is a normal timeout or empty probe.
     *
     * @param list<string> $queues
     */
    public function waitAny(array $queues, int $timeoutMilliseconds, Cancellation $cancellation): bool
    {
        try {
            return $this->notifications->client($cancellation)->waitAny($queues, $timeoutMilliseconds, $cancellation);
        } catch (CancelledException $error) {
            throw new TransportException('Queue wait was cancelled.', 0, $error);
        } catch (ClientTransportException|ProtocolException $error) {
            throw new TransportException('Could not wait on the queue broker.', 0, $error);
        }
    }

    public function queueName(): string
    {
        return $this->queue->value;
    }

    public function brokerEndpoint(): string
    {
        return $this->brokerEndpoint;
    }

    public function close(): void
    {
        try {
            $this->notifications->close();
        } finally {
            $this->operations->close();
        }
    }

    private function settle(Envelope $envelope, bool $acknowledge): void
    {
        $stamp = $envelope->last(DeliveryReceiptStamp::class);
        if (!$stamp instanceof DeliveryReceiptStamp) {
            throw new LogicException(\sprintf('No %s found on the Envelope.', DeliveryReceiptStamp::class));
        }
        if ($stamp->transport !== $this) {
            throw new LogicException('The envelope was not received by this transport instance.');
        }

        try {
            if ($acknowledge) {
                $this->operations->client()->acknowledge($stamp->receipt);
            } else {
                $this->operations->client()->reject($stamp->receipt);
            }
        } catch (InvalidReceiptException $error) {
            throw new TransportException('The delivery receipt is no longer valid.', 0, $error);
        } catch (ClientTransportException|ProtocolException $error) {
            throw new TransportException($acknowledge ? 'Could not acknowledge the delivery.' : 'Could not reject the delivery.', 0, $error);
        }
    }

    private function createEnvelopeFromDelivery(DeliveryDTO $delivery): Envelope
    {
        $received = new DeliveryReceiptStamp($delivery->receipt, $this);
        $id = new TransportMessageIdStamp($delivery->id);

        try {
            $encoded = [
                'body' => $delivery->body,
                'headers' => $this->decodeHeaders($delivery->headers),
            ];
        } catch (TransportException $error) {
            // Keep the opaque broker header string diagnosable for failure listeners.
            $encoded = [
                'body' => $delivery->body,
                'headers' => [self::RAW_HEADERS_HEADER => $delivery->headers],
            ];

            return $this->failureEnvelope($encoded, $error->getMessage(), 0, $error, $received, $id);
        }

        try {
            $decoded = $this->serializer->decode($encoded);
        } catch (MessageDecodingFailedException $error) {
            return $this->failureEnvelope($encoded, $error->getMessage(), (int) $error->getCode(), $error, $received, $id);
        }

        return $decoded
            ->withoutAll(TransportMessageIdStamp::class)
            ->withoutAll(DeliveryReceiptStamp::class)
            ->with($received, $id);
    }

    /**
     * @param array{body: string, headers: array<string, string>} $encoded
     */
    private function failureEnvelope(
        array $encoded,
        string $message,
        int $code,
        \Throwable $previous,
        DeliveryReceiptStamp $received,
        TransportMessageIdStamp $id,
    ): Envelope {
        if (\is_callable([MessageDecodingFailedException::class, 'wrap'])) {
            return MessageDecodingFailedException::wrap($encoded, $message, $code, $previous)
                ->with($received, $id);
        }

        // Symfony Messenger 8.0 has no failure-envelope helper and deletes on decode failure.
        // Reject the committed claim before rethrowing so the delivery cannot stay invisible.
        try {
            $this->operations->client()->reject($received->receipt);
        } catch (InvalidReceiptException|ClientTransportException|ProtocolException $rejectError) {
            throw new TransportException('Could not reject a delivery after a decode failure.', 0, $rejectError);
        }

        throw new MessageDecodingFailedException($message, $code, $previous);
    }

    private function delayMilliseconds(Envelope $envelope): int
    {
        $stamp = $envelope->last(DelayStamp::class);
        if (!$stamp instanceof DelayStamp) {
            return 0;
        }
        $delay = $stamp->getDelay();
        if ($delay < 0) {
            throw new \InvalidArgumentException('DelayStamp must carry nonnegative milliseconds.');
        }

        return $delay;
    }

    private function encodeHeaders(mixed $headers): string
    {
        if (null === $headers) {
            throw new \InvalidArgumentException('Serializer headers must not be null; omit the key for empty headers.');
        }
        if (!\is_array($headers)) {
            throw new \InvalidArgumentException('Serializer headers must be an array of UTF-8 string values.');
        }
        foreach ($headers as $name => $value) {
            if (!\is_string($name) || '' === $name) {
                throw new \InvalidArgumentException('Serializer header names must be non-empty strings.');
            }
            if (!\is_string($value)) {
                throw new \InvalidArgumentException(\sprintf('Serializer header "%s" must be a string.', $name));
            }
        }

        try {
            return json_encode($headers, \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE);
        } catch (\JsonException $error) {
            throw new \InvalidArgumentException('Serializer headers must be JSON-encodable UTF-8 strings.', 0, $error);
        }
    }

    /**
     * @return array<string, string>
     */
    private function decodeHeaders(string $headers): array
    {
        if ('' === $headers) {
            return [];
        }

        try {
            $decoded = json_decode($headers, true, self::JSON_DEPTH, \JSON_THROW_ON_ERROR);
        } catch (\JsonException $error) {
            throw new TransportException('Stored delivery headers are not valid JSON.', 0, $error);
        }
        if (!\is_array($decoded)) {
            throw new TransportException('Stored delivery headers must decode to a JSON object.');
        }
        if (array_is_list($decoded) && [] !== $decoded) {
            throw new TransportException('Stored delivery headers must decode to a JSON object.');
        }

        $normalized = [];
        foreach ($decoded as $name => $value) {
            if (!\is_string($name) || '' === $name) {
                throw new TransportException('Stored delivery headers contain a non-string name.');
            }
            if (!\is_string($value)) {
                throw new TransportException(\sprintf('Stored delivery header "%s" is not a string.', $name));
            }
            $normalized[$name] = $value;
        }

        return $normalized;
    }
}
