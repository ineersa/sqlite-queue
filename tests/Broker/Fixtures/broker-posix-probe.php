<?php

declare(strict_types=1);

use Ineersa\SqliteQueue\Broker\BrokerFactory;

/*
 * Reports how BrokerFactory refuses to start without ext-posix.
 *
 * The caller disables the extension for this process, for example with
 * `php -d disable_functions=posix_geteuid broker-posix-probe.php <database> <endpoint>`.
 */
require dirname(__DIR__, 3).'/vendor/autoload.php';

$database = $argv[1] ?? '';
$endpoint = $argv[2] ?? '';

try {
    (new BrokerFactory($database, $endpoint))->create();
    $report = ['outcome' => 'served'];
} catch (Throwable $error) {
    $report = ['outcome' => 'threw', 'class' => $error::class, 'message' => $error->getMessage()];
}

fwrite(\STDOUT, json_encode($report, \JSON_THROW_ON_ERROR)."\n");
