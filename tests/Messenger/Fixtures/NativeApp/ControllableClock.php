<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp;

use Symfony\Component\Clock\ClockInterface;
use Symfony\Component\Clock\DatePoint;

/**
 * Test clock for native consume proofs.
 *
 * sleep() records calls and returns immediately. WAIT-armed proofs assert sleepCalls stays empty.
 * now() is controlled so --time-limit proofs stay deterministic.
 */
final class ControllableClock implements ClockInterface
{
    /** @var list<float|int> */
    public array $sleepCalls = [];

    public function __construct(private float $now)
    {
    }

    public function now(): DatePoint
    {
        return DatePoint::createFromFormat('U.u', \sprintf('%.6F', $this->now))
            ?: throw new \RuntimeException('Could not create DatePoint from controllable clock.');
    }

    public function sleep(float|int $seconds): void
    {
        $this->sleepCalls[] = $seconds;
    }

    public function withTimeZone(\DateTimeZone|string $timezone): static
    {
        return $this;
    }

    public function advance(float $seconds): void
    {
        $this->now += $seconds;
    }

    public function set(float $now): void
    {
        $this->now = $now;
    }
}
