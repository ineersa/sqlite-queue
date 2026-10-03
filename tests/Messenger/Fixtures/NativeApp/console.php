<?php

declare(strict_types=1);

use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\NativeApp\NativeKernel;
use Symfony\Bundle\FrameworkBundle\Console\Application;

require dirname(__DIR__, 4).'/vendor/autoload.php';

$project = getenv('NATIVE_PROJECT_DIR');
if (!is_string($project) || '' === $project) {
    throw new RuntimeException('NATIVE_PROJECT_DIR must identify the isolated test app.');
}

// No worker construction or command replacement: use the application's native console.
(new Application(new NativeKernel($project)))->run();
