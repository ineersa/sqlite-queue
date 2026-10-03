<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Records effective debug modes separately from the INI default. */
final class RuntimeProfile
{
    /** @return array<string, mixed> */
    public static function current(): array
    {
        $override = getenv('XDEBUG_MODE');
        $modes = [];
        if (\extension_loaded('xdebug')) {
            if (!\function_exists('xdebug_info')) {
                throw new \RuntimeException('Cannot observe effective Xdebug modes.');
            }
            $modes = xdebug_info('mode');
            if (!\is_array($modes)) {
                throw new \RuntimeException('Invalid effective Xdebug mode observation.');
            }
            sort($modes);
        }

        return [
            'xdebug_ini_mode' => \ini_get('xdebug.mode'),
            'xdebug_mode_override' => false === $override ? null : $override,
            'xdebug_effective_modes' => $modes,
            'mode_observation' => 'xdebug_info_mode',
        ];
    }

    /** @param array<string, mixed> $expected */
    public static function verifyCurrent(array $expected): void
    {
        $actual = self::current();
        foreach (['xdebug_mode_override', 'xdebug_effective_modes'] as $field) {
            if ($actual[$field] !== ($expected[$field] ?? null)) {
                throw new \RuntimeException('Benchmark runtime mismatch: '.$field);
            }
        }
    }

    /**
     * The vendor worker has no PHP diagnostic RPC, and cli_set_process_title makes /proc environ
     * unreliable. Probe the same unmodified Amp context inheritance inside the broker, and verify
     * the actual SQLite worker uses that interpreter. This is startup instrumentation, not SQL work.
     *
     * @param array<string, mixed> $expected
     *
     * @return array<string, mixed>
     */
    public static function worker(int $pid, array $expected): array
    {
        if (readlink('/proc/'.$pid.'/exe') !== realpath(\PHP_BINARY)) {
            throw new \RuntimeException('SQLite worker interpreter does not match the broker.');
        }
        $context = (new \Amp\Parallel\Context\ProcessContextFactory())->start(__DIR__.'/RuntimeProbe.php');
        try {
            $probe = $context->join(new \Amp\TimeoutCancellation(Config::STARTUP_TIMEOUT_S));
            if (!\is_array($probe)) {
                throw new \RuntimeException('Missing worker inheritance runtime probe.');
            }
        } finally {
            $context->close();
        }
        foreach (['xdebug_mode_override', 'xdebug_effective_modes'] as $field) {
            if (($probe[$field] ?? null) !== $expected[$field]) {
                throw new \RuntimeException('SQLite worker context inheritance mismatch: '.$field);
            }
        }

        $probe['mode_observation'] = 'inheritance_probe_xdebug_info_mode';

        return $probe + [
            'pid' => $pid,
            'worker_mode_verification' => 'same_amp_context_inheritance_and_actual_worker_interpreter',
        ];
    }
}
