<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Config;
use Ineersa\SqliteQueue\Bench\Process;
use Ineersa\SqliteQueue\Bench\RuntimeProfile;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Process as SymfonyProcess;

final class RuntimeProfileTest extends TestCase
{
    public function testOnlyRequestedDebugOverrideIsAddedToIsolatedEnvironment(): void
    {
        $original = getenv('XDEBUG_MODE');
        $secret = getenv('SQLITE_QUEUE_TEST_RUNTIME_SECRET');
        try {
            putenv('XDEBUG_MODE=off');
            putenv('SQLITE_QUEUE_TEST_RUNTIME_SECRET=not-for-children');
            $environment = Process::environment();
            $this->assertSame('off', $environment['XDEBUG_MODE']);
            $this->assertFalse($environment['SQLITE_QUEUE_TEST_RUNTIME_SECRET']);
            $this->assertSame('develop', Process::environment(['XDEBUG_MODE' => 'develop'])['XDEBUG_MODE']);
            putenv('XDEBUG_MODE');
            $this->assertFalse(Process::environment()['XDEBUG_MODE'] ?? false);
        } finally {
            false === $original ? putenv('XDEBUG_MODE') : putenv('XDEBUG_MODE='.$original);
            false === $secret ? putenv('SQLITE_QUEUE_TEST_RUNTIME_SECRET') : putenv('SQLITE_QUEUE_TEST_RUNTIME_SECRET='.$secret);
        }
    }

    public function testNativeEffectiveModeAndMachineMetadataHonorOffOverrideInsteadOfIniDefault(): void
    {
        $process = new SymfonyProcess([
            \PHP_BINARY, '-d', 'xdebug.mode=develop', '-r',
            'require $argv[1]; echo json_encode(\Ineersa\SqliteQueue\Bench\Machine::describe(sys_get_temp_dir(), \Ineersa\SqliteQueue\Bench\Config::rootDir())["php"], JSON_THROW_ON_ERROR);',
            Config::rootDir().'/vendor/autoload.php',
        ], env: Process::environment(['XDEBUG_MODE' => 'off']));
        $this->assertSame(0, $process->run());
        $php = json_decode($process->getOutput(), true, 512, \JSON_THROW_ON_ERROR);
        $this->assertSame('off', $php['xdebug_mode']);
        $this->assertSame('off', $php['xdebug_mode_override']);
        $this->assertSame([], $php['runtime_profile']['xdebug_effective_modes']);
        if (null !== $php['xdebug_version']) {
            $this->assertSame('develop', $php['xdebug_ini_mode']);
        }
    }

    public function testRuntimeMismatchFailsExplicitly(): void
    {
        $profile = RuntimeProfile::current();
        $profile['xdebug_effective_modes'] = ['not-the-current-profile'];
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Benchmark runtime mismatch: xdebug_effective_modes');
        RuntimeProfile::verifyCurrent($profile);
    }
}
