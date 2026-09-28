<?php

declare(strict_types=1);

namespace SqliteQueueTasks;

use Castor\Console\Output\VerbosityLevel;
use Castor\Context;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

use function Castor\run;

/**
 * Stream tool output to disk and preserve the exit code. Each invocation replaces
 * its own reports; an interrupted invocation leaves a running status, not a stale pass.
 *
 * @param list<string> $command
 */
function report(string $task, array $command, bool $jsonOutput = false, ?string $nativeReport = null, ?float $timeout = 300): int
{
    $root = \dirname(__DIR__);
    $directory = 'var/qa/'.str_replace(':', '-', $task);
    $filesystem = new Filesystem();
    $filesystem->mkdir($root.'/'.$directory, 0700);
    $stdout = $directory.($jsonOutput ? '/stdout.json' : '/stdout.log');
    $stderr = $directory.'/stderr.log';
    $resultPath = $directory.'/result.json';
    $filesystem->dumpFile($root.'/'.$stdout, '');
    $filesystem->dumpFile($root.'/'.$stderr, '');
    if (null !== $nativeReport) {
        $filesystem->remove($root.'/'.$nativeReport);
    }

    $result = [
        'task' => $task,
        'command' => $command,
        'started_at' => gmdate(\DATE_ATOM),
        'status' => 'running',
        'exit_code' => null,
        'stdout' => $stdout,
        'stderr' => $stderr,
        'native_report' => null,
    ];
    $filesystem->dumpFile($root.'/'.$resultPath, json_encode($result, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR)."\n");
    $start = hrtime(true);
    try {
        $process = run($command, new Context(
            environment: ['NO_COLOR' => '1', 'CLICOLOR' => '0', 'COMPOSER_NO_INTERACTION' => '1'],
            workingDirectory: $root,
            timeout: $timeout,
            quiet: true,
            allowFailure: true,
            notify: false,
            verbosityLevel: VerbosityLevel::SILENT,
        ), static function (string $type, string $data, Process $process) use ($filesystem, $root, $stdout, $stderr): void {
            $filesystem->appendToFile($root.'/'.(Process::ERR === $type ? $stderr : $stdout), $data);
            $process->clearOutput();
            $process->clearErrorOutput();
        });
        $exitCode = $process->getExitCode() ?? 1;
    } catch (\Throwable $error) {
        $filesystem->appendToFile($root.'/'.$stderr, (string) $error."\n");
        $exitCode = 1;
    }

    $result['status'] = 0 === $exitCode ? 'passed' : 'failed';
    $result['exit_code'] = $exitCode;
    $result['duration_ms'] = (hrtime(true) - $start) / 1e6;
    $result['finished_at'] = gmdate(\DATE_ATOM);
    $result['native_report'] = null !== $nativeReport && is_file($root.'/'.$nativeReport) ? $nativeReport : null;
    $filesystem->dumpFile($root.'/'.$resultPath, json_encode($result, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR)."\n");
    echo json_encode(['task' => $task, 'status' => $result['status'], 'exit_code' => $exitCode, 'result' => $resultPath], \JSON_THROW_ON_ERROR)."\n";

    return $exitCode;
}
