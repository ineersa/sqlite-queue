<?php

declare(strict_types=1);
use Ineersa\SqliteQueue\Bench\Kernel;
use Ineersa\SqliteQueue\Bench\Runtime;
use Symfony\Bundle\FrameworkBundle\Console\Application;

require dirname(__DIR__, 2).'/vendor/autoload.php';
(new Application(new Kernel(Runtime::environment('BENCH_PROJECT'))))->run();
