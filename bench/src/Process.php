<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/**
 * One spawned benchmark process with an explicit environment, readiness handshake, hard
 * bound, and owned-tree teardown.
 *
 * Standard output and error go to files, so a chatty child cannot block the coordinator on a
 * full pipe. Readiness is a dedicated file that carries the child's pid, which makes a stale
 * file from an earlier run detectable instead of trusted.
 */
final class Process
{
    public const READY_EVENT = 'ready';

    /** Signals are passed as numbers: ext-pcntl is not a declared dependency of this package. */
    private const SIGTERM = 15;

    private const SIGKILL = 9;

    /** @var resource */
    private $handle;

    private bool $reaped = false;

    private ?int $exitCode = null;

    private bool $timedOut = false;

    private bool $killed = false;

    /**
     * @param list<string> $argv
     * @param array<string, string> $env
     */
    private function __construct(
        private string $role,
        private string $argument,
        private array $argv,
        private array $env,
        $handle,
        private int $pid,
        private string $stdoutPath,
        private string $stderrPath,
        private string $readyPath,
    ) {
        $this->handle = $handle;
    }

    /**
     * @param list<string> $argv
     * @param array<string, string> $env
     */
    public static function spawn(
        string $role,
        string $argument,
        array $argv,
        array $env,
        string $directory,
    ): self {
        foreach (['ready', 'logs'] as $sub) {
            $path = $directory . '/' . $sub;
            if (!\is_dir($path) && !\mkdir($path, 0o700, true) && !\is_dir($path)) {
                throw new \RuntimeException(\sprintf('Cannot create "%s".', $path));
            }
        }

        $id = $role . '-' . \preg_replace('/[^a-z0-9_-]+/i', '_', $argument);
        $stdoutPath = $directory . '/logs/' . $id . '.out';
        $stderrPath = $directory . '/logs/' . $id . '.err';
        $readyPath = $directory . '/ready/' . $id . '.ready';

        @\unlink($readyPath);

        $descriptors = [
            0 => ['file', '/dev/null', 'rb'],
            1 => ['file', $stdoutPath, 'ab'],
            2 => ['file', $stderrPath, 'ab'],
        ];

        $pipes = [];
        $handle = \proc_open($argv, $descriptors, $pipes, $directory, $env);

        if (!\is_resource($handle)) {
            throw new \RuntimeException(\sprintf('Cannot start the "%s" process.', $id));
        }

        $status = \proc_get_status($handle);
        $pid = (int) ($status['pid'] ?? 0);
        if ($pid <= 0) {
            \proc_terminate($handle, self::SIGKILL);
            \proc_close($handle);

            throw new \RuntimeException(\sprintf('The "%s" process has no pid.', $id));
        }

        return new self($role, $argument, $argv, $env, $handle, $pid, $stdoutPath, $stderrPath, $readyPath);
    }

    public function pid(): int
    {
        return $this->pid;
    }

    public function role(): string
    {
        return $this->role;
    }

    public function argument(): string
    {
        return $this->argument;
    }

    public function label(): string
    {
        return $this->role . ':' . $this->argument;
    }

    /**
     * @return array{argv: list<string>, env: array<string, string>}
     */
    public function invocation(): array
    {
        return ['argv' => $this->argv, 'env' => $this->env];
    }

    /**
     * Waits for the child's readiness file.
     *
     * @return array<string, mixed>
     */
    public function waitForReady(float $timeoutSeconds): array
    {
        $deadline = \microtime(true) + $timeoutSeconds;

        while (\microtime(true) < $deadline) {
            if (\is_file($this->readyPath)) {
                $contents = \file_get_contents($this->readyPath);
                if (\is_string($contents) && '' !== \trim($contents)) {
                    try {
                        /** @var mixed $decoded */
                        $decoded = \json_decode(\trim($contents), true, 32, \JSON_THROW_ON_ERROR);
                    } catch (\JsonException) {
                        $decoded = null;
                    }

                    if (\is_array($decoded) && self::READY_EVENT === ($decoded['event'] ?? null)) {
                        if ($this->pid === ($decoded['pid'] ?? null)) {
                            return $decoded;
                        }

                        throw new \RuntimeException(\sprintf(
                            'The readiness file of "%s" reports pid %s, not %d.',
                            $this->label(),
                            \var_export($decoded['pid'] ?? null, true),
                            $this->pid,
                        ));
                    }
                }
            }

            if (!$this->isRunning()) {
                $this->reap();

                throw new \RuntimeException(\sprintf(
                    'The "%s" process exited with code %s before reporting readiness. %s',
                    $this->label(),
                    \var_export($this->exitCode, true),
                    $this->errorTail(),
                ));
            }

            \usleep(2_000);
        }

        throw new \RuntimeException(\sprintf(
            'The "%s" process did not report readiness within %.1f s. %s',
            $this->label(),
            $timeoutSeconds,
            $this->errorTail(),
        ));
    }

    public function isRunning(): bool
    {
        if ($this->reaped || !\is_resource($this->handle)) {
            return false;
        }

        $status = \proc_get_status($this->handle);

        return (bool) ($status['running'] ?? false);
    }

    /**
     * Waits for the child to exit within the bound, then reaps it.
     */
    public function wait(float $timeoutSeconds): int
    {
        $deadline = \microtime(true) + $timeoutSeconds;

        while (\microtime(true) < $deadline) {
            if (!$this->isRunning()) {
                return $this->reap();
            }

            \usleep(5_000);
        }

        $this->timedOut = true;
        $this->killTree();

        return $this->reap();
    }

    /**
     * Terminates the whole owned tree: SIGTERM, then SIGKILL after a grace period.
     */
    public function killTree(): void
    {
        $this->killed = true;
        $this->signalTree(self::SIGTERM);

        $deadline = \microtime(true) + Config::KILL_GRACE_S;
        while (\microtime(true) < $deadline && $this->isRunning()) {
            \usleep(20_000);
        }

        $this->signalTree(self::SIGKILL);

        $grace = \microtime(true) + 2.0;
        while (\microtime(true) < $grace) {
            if (!$this->isRunning() && [] === $this->survivors()) {
                return;
            }
            \usleep(20_000);
        }
    }

    /**
     * @return list<int> PIDs still alive in this process's tree.
     */
    public function survivors(): array
    {
        if (!ProcessTree::available()) {
            return [];
        }

        $snapshot = ProcessTree::snapshot();

        return ProcessTree::descendants($this->pid, $snapshot);
    }

    public function exitCode(): ?int
    {
        return $this->exitCode;
    }

    public function timedOut(): bool
    {
        return $this->timedOut;
    }

    public function killed(): bool
    {
        return $this->killed;
    }

    public function stdoutTail(int $bytes = 2000): string
    {
        return self::tail($this->stdoutPath, $bytes);
    }

    public function errorTail(int $bytes = 2000): string
    {
        return self::tail($this->stderrPath, $bytes);
    }

    public function stdoutPath(): string
    {
        return $this->stdoutPath;
    }

    public function stderrPath(): string
    {
        return $this->stderrPath;
    }

    private function signalTree(int $signal): void
    {
        if (\function_exists('posix_kill')) {
            $snapshot = ProcessTree::available() ? ProcessTree::snapshot() : [];
            $targets = ProcessTree::descendants($this->pid, $snapshot);
            // Descendants first: a parent cannot respawn or reap what the signal is meant to end.
            foreach (\array_reverse($targets) as $pid) {
                @\posix_kill($pid, $signal);
            }
        }

        // The direct child is always signalled through proc_terminate, which needs no extension.

        if ($this->isRunning()) {
            @\proc_terminate($this->handle, $signal);
        }
    }

    private function reap(): int
    {
        if ($this->reaped) {
            return (int) $this->exitCode;
        }

        $status = \proc_get_status($this->handle);

        if ((bool) ($status['running'] ?? false)) {
            $this->exitCode = null;
        } else {
            $this->exitCode = (int) ($status['exitcode'] ?? -1);
        }

        \proc_close($this->handle);
        $this->reaped = true;

        return (int) $this->exitCode;
    }

    private static function tail(string $path, int $bytes): string
    {
        if (!\is_file($path)) {
            return '';
        }

        $size = (int) \filesize($path);
        $handle = \fopen($path, 'rb');
        if (false === $handle) {
            return '';
        }

        if ($size > $bytes) {
            \fseek($handle, $size - $bytes);
        }

        $contents = (string) \stream_get_contents($handle);
        \fclose($handle);

        return $contents;
    }
}
