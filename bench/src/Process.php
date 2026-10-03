<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

use Symfony\Component\Process\Process as SymfonyProcess;

/** Adds benchmark readiness, artifact logs, and owned-tree cleanup to Symfony Process. */
final class Process
{
    private bool $timedOut = false;
    private bool $killed = false;

    private function __construct(
        private readonly SymfonyProcess $process,
        private readonly string $role,
        private readonly string $argument,
        private readonly int $pid,
        private readonly string $readyPath,
        private readonly string $stderrPath,
    ) {
    }

    /**
     * @param list<string>                $argv
     * @param array<string, string|false> $env
     */
    public static function spawn(string $role, string $argument, array $argv, array $env, string $directory): self
    {
        foreach (['ready', 'logs'] as $subdirectory) {
            $path = $directory.'/'.$subdirectory;
            if (!is_dir($path) && !mkdir($path, 0700, true)) {
                throw new \RuntimeException('Cannot create process artifact directory: '.$path);
            }
        }

        $id = $role.'-'.preg_replace('/[^a-z0-9_-]+/i', '_', $argument);
        $stdoutPath = $directory.'/logs/'.$id.'.out';
        $stderrPath = $directory.'/logs/'.$id.'.err';
        $readyPath = $directory.'/ready/'.$id.'.ready';
        $process = new SymfonyProcess($argv, $directory, self::environment($env), timeout: null);
        $process->start(static function (string $type, string $data) use ($stdoutPath, $stderrPath): void {
            file_put_contents(SymfonyProcess::ERR === $type ? $stderrPath : $stdoutPath, $data, \FILE_APPEND);
        });
        $pid = $process->getPid();
        if (null === $pid) {
            throw new \RuntimeException('Process exited during startup: '.$id.'. '.$process->getErrorOutput());
        }

        return new self($process, $role, $argument, $pid, $readyPath, $stderrPath);
    }

    /**
     * Symfony inherits environment variables unless they are explicitly removed.
     *
     * @param array<string, string|false> $overrides
     *
     * @return array<string, string|false>
     */
    public static function environment(array $overrides = []): array
    {
        $inherited = array_keys(getenv() + $_ENV);

        $xdebugMode = getenv('XDEBUG_MODE');
        $runtime = false === $xdebugMode ? [] : ['XDEBUG_MODE' => $xdebugMode];

        return $overrides + $runtime + ['PATH' => '/usr/bin:/bin', 'LANG' => 'C', 'TZ' => 'UTC'] + array_fill_keys($inherited, false);
    }

    /**
     * @return array<string, mixed>
     */
    public function waitForReady(float $timeoutSeconds): array
    {
        $deadline = hrtime(true) + (int) ($timeoutSeconds * 1e9);
        while (hrtime(true) < $deadline) {
            if (is_file($this->readyPath)) {
                $ready = json_decode(file_get_contents($this->readyPath), true);
                if (\is_array($ready) && ($ready['event'] ?? null) === 'ready') {
                    if (($ready['pid'] ?? null) !== $this->pid) {
                        throw new \RuntimeException('Readiness PID mismatch for '.$this->label());
                    }

                    return $ready;
                }
            }
            if (!$this->isRunning()) {
                throw new \RuntimeException('Process exited before readiness: '.$this->label().'. '.$this->errorTail());
            }
            usleep(2000);
        }

        throw new \RuntimeException('Readiness timeout for '.$this->label());
    }

    public function wait(float $timeoutSeconds): int
    {
        $deadline = hrtime(true) + (int) ($timeoutSeconds * 1e9);
        while ($this->isRunning()) {
            if (hrtime(true) >= $deadline) {
                $this->timedOut = true;
                $this->killTree();
                break;
            }
            usleep(5000);
        }

        return $this->exitCode() ?? -1;
    }

    public function killTree(): void
    {
        $this->killed = true;
        $snapshot = ProcessTree::snapshot();
        $descendants = ProcessTree::descendants($this->pid, $snapshot);
        $owned = array_intersect_key($snapshot, array_flip($descendants));
        $this->signalDescendants($owned, \SIGTERM);
        $this->process->stop(Config::KILL_GRACE_S, \SIGKILL);
        $this->signalDescendants($owned, \SIGKILL);
    }

    public function isRunning(): bool
    {
        $running = $this->process->isRunning();
        // Output is already in artifact files. Do not retain an unbounded in-memory copy.
        $this->process->clearOutput();
        $this->process->clearErrorOutput();

        return $running;
    }

    /**
     * @return list<int>
     */
    public function survivors(): array
    {
        return ProcessTree::descendants($this->pid, ProcessTree::snapshot());
    }

    public function pid(): int
    {
        return $this->pid;
    }

    public function terminate(): void
    {
        if ($this->isRunning()) {
            $this->process->signal(\SIGTERM);
        }
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
        return $this->role.':'.$this->argument;
    }

    public function exitCode(): ?int
    {
        return $this->process->getExitCode();
    }

    public function timedOut(): bool
    {
        return $this->timedOut;
    }

    public function killed(): bool
    {
        return $this->killed;
    }

    public function errorTail(): string
    {
        if (!is_file($this->stderrPath)) {
            return '';
        }

        return file_get_contents($this->stderrPath, offset: max(0, filesize($this->stderrPath) - 2000));
    }

    /**
     * @param array<int, array{ppid: int, state: string, start_time_ticks: int, uid: int|false, cmd: string, cpu_seconds: float, rss_kb: int}> $owned
     */
    private function signalDescendants(array $owned, int $signal): void
    {
        $current = ProcessTree::snapshot();
        foreach (array_reverse(array_keys($owned)) as $pid) {
            $identity = $owned[$pid];
            if (($current[$pid]['start_time_ticks'] ?? null) === $identity['start_time_ticks']
                && $identity['uid'] === posix_geteuid() && 0 !== $identity['uid']) {
                // The process can exit between the snapshot and the signal.
                @posix_kill($pid, $signal);
            }
        }
    }
}
