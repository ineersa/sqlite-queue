<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Tooling;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

final class TaskReportsTest extends TestCase
{
    private string $directory;
    private Filesystem $filesystem;

    protected function setUp(): void
    {
        $this->filesystem = new Filesystem();
        $this->directory = \dirname(__DIR__, 2).'/var/tests/castor-'.bin2hex(random_bytes(8));
        $this->filesystem->mkdir($this->directory.'/.castor', 0700);
        $this->filesystem->copy(\dirname(__DIR__, 2).'/.castor/reports.php', $this->directory.'/.castor/reports.php');
        $this->filesystem->dumpFile($this->directory.'/castor.php', <<<'PHP'
            <?php
            declare(strict_types=1);
            defined('CASTOR_USE_CHDIR') || define('CASTOR_USE_CHDIR', true);
            Castor\import(__DIR__.'/.castor/reports.php');
            #[Castor\Attribute\AsTask(name: 'probe')]
            function probe(): int
            {
                return SqliteQueueTasks\report('probe', [PHP_BINARY, 'probe.php'], jsonOutput: true, nativeReport: 'var/qa/probe/native.xml');
            }
            PHP);
    }

    protected function tearDown(): void
    {
        $this->filesystem->remove($this->directory);
    }

    public function testReportsKeepOutputOffConsoleAndPropagateFailure(): void
    {
        $this->filesystem->dumpFile($this->directory.'/probe.php', <<<'PHP'
            <?php
            echo json_encode(['payload' => str_repeat('x', 100_000)]);
            fwrite(STDERR, "failure detail\n");
            exit(7);
            PHP);
        $process = $this->runProbe();
        $this->assertSame(7, $process->getExitCode(), $process->getOutput().$process->getErrorOutput());
        $this->assertSame('', $process->getErrorOutput());
        $summary = json_decode($process->getOutput(), true, 512, \JSON_THROW_ON_ERROR);
        $this->assertSame('failed', $summary['status']);
        $this->assertSame(7, $summary['exit_code']);
        $this->assertLessThan(250, \strlen($process->getOutput()));
        $result = $this->readResult();
        $this->assertSame(7, $result['exit_code']);
        $this->assertSame('failed', $result['status']);
        $output = json_decode(file_get_contents($this->directory.'/'.$result['stdout']), true, 512, \JSON_THROW_ON_ERROR);
        $this->assertSame(100_000, \strlen($output['payload']));
        $this->assertSame("failure detail\n", file_get_contents($this->directory.'/'.$result['stderr']));
    }

    public function testFailedRerunCannotLeaveStalePassingReports(): void
    {
        $this->filesystem->dumpFile($this->directory.'/probe.php', <<<'PHP'
            <?php
            file_put_contents('var/qa/probe/native.xml', '<testsuites/>');
            echo '{"ok":true}';
            PHP);
        $process = $this->runProbe();
        $this->assertSame(0, $process->getExitCode(), $process->getOutput().$process->getErrorOutput());
        $this->assertSame('passed', $this->readResult()['status']);
        $this->assertSame('var/qa/probe/native.xml', $this->readResult()['native_report']);

        $this->filesystem->dumpFile($this->directory.'/probe.php', '<?php exit(2);');
        $this->assertSame(2, $this->runProbe()->getExitCode());
        $result = $this->readResult();
        $this->assertSame('failed', $result['status']);
        $this->assertNull($result['native_report']);
        $this->assertFileDoesNotExist($this->directory.'/var/qa/probe/native.xml');
        $this->assertSame('', file_get_contents($this->directory.'/'.$result['stdout']));
        $this->assertSame('', file_get_contents($this->directory.'/'.$result['stderr']));
    }

    private function runProbe(): Process
    {
        $process = new Process([\PHP_BINARY, \dirname(__DIR__, 2).'/vendor/bin/castor', 'probe', '--no-ansi', '--no-interaction'], $this->directory, [
            'CASTOR_GENERATE_STUBS' => '0',
            'CASTOR_DISABLE_VERSION_CHECK' => '1',
        ], timeout: 20);
        $process->run();

        return $process;
    }

    /** @return array<string, mixed> */
    private function readResult(): array
    {
        return json_decode(file_get_contents($this->directory.'/var/qa/probe/result.json'), true, 512, \JSON_THROW_ON_ERROR);
    }
}
