<?php

declare(strict_types=1);

namespace SqliteQueueTasks;

use Castor\Attribute\AsOption;
use Castor\Attribute\AsTask;
use Symfony\Component\Filesystem\Filesystem;

#[AsTask(name: 'composer:validate', namespace: '', description: 'Validate Composer metadata; save output under var/qa.')]
function composer_validate(): int
{
    return report('composer:validate', [\PHP_BINARY, 'vendor/bin/composer', 'validate', '--no-check-publish', '--no-ansi', '--no-interaction']);
}

#[AsTask(name: 'cs:fix', namespace: '', description: 'Apply PHP CS Fixer rules; save a JSON report.')]
function cs_fix(): int
{
    return report('cs:fix', [\PHP_BINARY, 'vendor/bin/php-cs-fixer', 'fix', '--no-ansi', '--no-interaction', '--show-progress=none', '--format=json'], jsonOutput: true);
}

#[AsTask(name: 'cs:check', namespace: '', description: 'Check formatting without edits; save JSON including diffs.')]
function cs_check(): int
{
    return report('cs:check', [\PHP_BINARY, 'vendor/bin/php-cs-fixer', 'check', '--diff', '--no-ansi', '--no-interaction', '--show-progress=none', '--format=json'], jsonOutput: true);
}

#[AsTask(name: 'phpstan', namespace: '', description: 'Run level-6 strict analysis; save a JSON report.')]
function phpstan(#[AsOption(description: 'Analyze this path instead of the configured paths.')] ?string $path = null): int
{
    $command = [\PHP_BINARY, 'vendor/bin/phpstan', 'analyse', '--memory-limit=1G', '--no-ansi', '--no-progress', '--error-format=json'];
    if (null !== $path) {
        $command[] = $path;
    }

    return report('phpstan', $command, jsonOutput: true);
}

#[AsTask(name: 'test', namespace: '', description: 'Run correctness tests; save JUnit XML and logs.')]
function test(#[AsOption(description: 'PHPUnit test-name filter.')] ?string $filter = null): int
{
    return run_tests('test', null, $filter);
}

#[AsTask(name: 'test:flex', namespace: '', description: 'Install into a real no-dev Flex app; requires network and saves reports.')]
function test_flex(): int
{
    return report('test:flex', ['bash', 'tests/Messenger/Fixtures/flex-install.sh'], timeout: null);
}

#[AsTask(name: 'test:driver', namespace: '', description: 'Run async-driver tests; save JUnit XML and logs.')]
function test_driver(#[AsOption(description: 'PHPUnit test-name filter.')] ?string $filter = null): int
{
    return run_tests('test:driver', 'driver', $filter);
}

#[AsTask(name: 'test:bench', namespace: '', description: 'Run benchmark correctness tests; save JUnit XML and logs.')]
function test_bench(#[AsOption(description: 'PHPUnit test-name filter.')] ?string $filter = null): int
{
    return run_tests('test:bench', 'benchmark', $filter);
}

#[AsTask(name: 'test:broker', namespace: '', description: 'Run broker, protocol, and client tests; save JUnit XML and logs.')]
function test_broker(#[AsOption(description: 'PHPUnit test-name filter.')] ?string $filter = null): int
{
    return run_tests('test:broker', 'broker', $filter);
}

#[AsTask(name: 'bench', namespace: '', description: 'Capture a benchmark; save the capture path and progress in the task log.')]
function bench(
    #[AsOption(description: 'Run smoke workloads, not a performance capture.')] bool $smoke = false,
    #[AsOption(description: 'Workload name or all.')] string $workload = 'all',
): int {
    $command = [\PHP_BINARY, 'bin/benchmark', 'run', '--no-ansi', '--no-interaction', '--workload='.$workload];
    if ($smoke) {
        $command[] = '--smoke';
    }

    return report('bench', $command, timeout: null);
}

#[AsTask(name: 'qa', namespace: '', description: 'Run validation, style, analysis, and tests; save an aggregate result.')]
function qa(): int
{
    $filesystem = new Filesystem();
    $path = \dirname(__DIR__).'/var/qa/result.json';
    $result = ['status' => 'running', 'exit_code' => null, 'started_at' => gmdate(\DATE_ATOM), 'tasks' => []];
    $filesystem->dumpFile($path, json_encode($result, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR)."\n");
    $exitCode = 0;
    foreach (['composer:validate' => composer_validate(...), 'cs:check' => cs_check(...), 'phpstan' => phpstan(...), 'test' => test(...)] as $name => $task) {
        $code = $task();
        $result['tasks'][$name] = ['exit_code' => $code, 'result' => 'var/qa/'.str_replace(':', '-', $name).'/result.json'];
        if (0 !== $code && 0 === $exitCode) {
            $exitCode = $code;
        }
        $filesystem->dumpFile($path, json_encode($result, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR)."\n");
    }
    $result['status'] = 0 === $exitCode ? 'passed' : 'failed';
    $result['exit_code'] = $exitCode;
    $result['finished_at'] = gmdate(\DATE_ATOM);
    $filesystem->dumpFile($path, json_encode($result, \JSON_PRETTY_PRINT | \JSON_THROW_ON_ERROR)."\n");
    echo json_encode(['task' => 'qa', 'status' => $result['status'], 'exit_code' => $exitCode, 'result' => 'var/qa/result.json'], \JSON_THROW_ON_ERROR)."\n";

    return $exitCode;
}

function run_tests(string $task, ?string $suite, ?string $filter): int
{
    $junit = 'var/qa/'.str_replace(':', '-', $task).'/junit.xml';
    $command = [\PHP_BINARY, 'vendor/bin/phpunit', '--colors=never', '--no-progress', '--log-junit', $junit];
    if (null !== $suite) {
        array_push($command, '--testsuite', $suite);
    }
    if (null !== $filter) {
        array_push($command, '--filter', $filter);
    }

    return report($task, $command, nativeReport: $junit);
}
