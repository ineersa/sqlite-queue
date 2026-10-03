<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Bench;

use Ineersa\SqliteQueue\Bench\Backend;
use Ineersa\SqliteQueue\Bench\Baseline;
use Ineersa\SqliteQueue\Bench\Comparison;
use Ineersa\SqliteQueue\Bench\Config;
use Ineersa\SqliteQueue\Bench\Process;
use Ineersa\SqliteQueue\Bench\ProcessTree;
use Ineersa\SqliteQueue\Bench\Schedule;
use Ineersa\SqliteQueue\Tests\Support\IsolatedDatabase;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresOperatingSystem;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

final class PairedBenchmarkTest extends TestCase
{
    private const int SQLITE_BUSY = 5;

    public function testOrderAlternatesWithoutChangingWorkload(): void
    {
        $this->assertSame([Backend::Doctrine, Backend::Broker], Schedule::order(0));
        $this->assertSame([Backend::Broker, Backend::Doctrine], Schedule::order(1));
        $this->assertSame(Schedule::order(0), Schedule::order(2));
        $this->assertSame(3000, Config::expectedSends(Config::workload('concurrent')));
        $this->expectException(\InvalidArgumentException::class);
        Schedule::order(-1);
    }

    public static function verdicts(): iterable
    {
        yield 'separated gain' => [1.0, 'win'];
        yield 'separated loss' => [20.0, 'regression'];
        yield 'overlapping ranges' => [10.0, 'neutral'];
    }

    #[DataProvider('verdicts')]
    public function testComparisonUsesPerRepetitionRangesAndRetainsFailedEvidence(float $candidate, string $verdict): void
    {
        $runs = [];
        for ($index = 1; $index <= Config::MEASURED_REPETITIONS; ++$index) {
            foreach (Schedule::order($index) as $backend) {
                $value = (Backend::Doctrine === $backend ? 10.0 : $candidate) + $index;
                $distribution = ['count' => Config::MIN_TAIL_SAMPLES, 'p95' => $value, 'p99' => $value];
                $metrics = ['publish_to_handler_ms' => $distribution, 'full_cycle_ms' => $distribution];
                $runs[] = ['status' => 'complete', 'warmup' => false, 'repetition' => 'run-'.$index, 'workload' => ['name' => 'concurrent', 'backend' => $backend->value], 'metrics' => ['by_payload' => ['small' => $metrics, 'large' => $metrics]]];
            }
        }
        $this->assertSame($verdict, Comparison::build($runs, 6)['status']);
        $this->assertSame('inconclusive', Comparison::build($runs, 7)['status']);
        $failed = $runs;
        $failed[0]['status'] = 'incomplete';
        $this->assertSame('inconclusive', Comparison::build($failed, 6)['status']);
        $runs[0]['metrics']['by_payload']['small']['full_cycle_ms']['count'] = Config::MIN_TAIL_SAMPLES - 1;
        $this->assertSame('inconclusive', Comparison::build($runs, 6)['status']);
    }

    public function testBackendReadsOriginalEligibilityWithoutReplacingMissingRows(): void
    {
        $database = new IsolatedDatabase();
        $connection = Baseline::connect($database->path());
        try {
            $connection->executeStatement('CREATE TABLE queue_messages (id INTEGER PRIMARY KEY, queue TEXT, available_at INTEGER)');
            $connection->executeStatement("INSERT INTO queue_messages VALUES (1, 'jobs', 1700000000123)");
            $this->assertSame(1700000000.123, Backend::Broker->deadline($connection, 1));
            $this->assertNull(Backend::Broker->deadline($connection, 2));
            $this->assertSame(['jobs' => 1], Backend::Broker->inventory($connection));
            $this->assertLessThanOrEqual(100, \strlen(Backend::endpoint(str_repeat('/deep', 100))));
        } finally {
            $connection->close();
            $database->remove();
        }
    }

    public function testDoctrineNativeImmediateModeReservesWriterAtBeginAndRollbackReleasesIt(): void
    {
        $database = new IsolatedDatabase();
        $first = Baseline::connect($database->path());
        $second = Baseline::connect($database->path());
        try {
            $first->executeStatement('CREATE TABLE writer_probe (id INTEGER PRIMARY KEY)');
            $native = $second->getNativeConnection();
            $this->assertInstanceOf(\Pdo\Sqlite::class, $native);
            $this->assertSame(\Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE, $native->getAttribute(\Pdo\Sqlite::ATTR_TRANSACTION_MODE));
            $settings = Baseline::durability($first);
            $this->assertSame('immediate', $settings['transaction_mode']);
            $this->assertSame(\Pdo\Sqlite::TRANSACTION_MODE_IMMEDIATE, $settings['native_transaction_mode']);
            // No timing inference: a held writer plus timeout zero makes BEGIN decisive.
            $second->executeStatement('PRAGMA busy_timeout=0');
            $first->beginTransaction();
            try {
                $native->beginTransaction();
                $this->fail('The second writer must fail at BEGIN, before any read/write upgrade.');
            } catch (\PDOException $error) {
                $this->assertSame(self::SQLITE_BUSY, $error->errorInfo[1]);
            }
            $this->assertFalse($native->inTransaction());
            $first->executeStatement('INSERT INTO writer_probe VALUES (1)');
            $this->assertSame(0, (int) $second->fetchOne('SELECT COUNT(*) FROM writer_probe'));
            $first->rollBack();
            $second->beginTransaction();
            $second->executeStatement('INSERT INTO writer_probe VALUES (2)');
            $second->rollBack();
            $this->assertSame(0, (int) $first->fetchOne('SELECT COUNT(*) FROM writer_probe'));
            $first->beginTransaction();
            $first->executeStatement('INSERT INTO writer_probe VALUES (3)');
            $first->commit();
            $this->assertSame(3, (int) $second->fetchOne('SELECT id FROM writer_probe'));
            $this->assertFalse($first->isTransactionActive());
            $this->assertFalse($second->isTransactionActive());
        } finally {
            $first->close();
            $second->close();
            $database->remove();
        }
    }

    #[RequiresOperatingSystem('Linux')]
    public function testBenchmarkBrokerReadinessIncludesRealWorkerDurabilityAndGracefulTreeCleanup(): void
    {
        $database = new IsolatedDatabase();
        $directory = \dirname($database->path());
        $socketDirectory = \dirname(Backend::endpoint($directory));
        mkdir($socketDirectory, 0700);
        $process = Process::spawn('broker', '0', [\PHP_BINARY, Config::rootDir().'/bin/benchmark', 'broker', $directory], ['XDEBUG_MODE' => 'off'], $directory);
        try {
            $ready = $process->waitForReady(Config::STARTUP_TIMEOUT_S);
            $worker = $ready['persistence_pid'];
            $this->assertContains($worker, $process->survivors());
            $profiles = json_decode(file_get_contents($directory.'/broker-runtime.json'), true, 512, \JSON_THROW_ON_ERROR);
            $this->assertSame([], $profiles['broker']['xdebug_effective_modes']);
            $this->assertSame('off', $profiles['sqlite_worker']['xdebug_mode_override']);
            $this->assertSame([], $profiles['sqlite_worker']['xdebug_effective_modes']);
            $settings = json_decode(file_get_contents($directory.'/broker-durability.json'), true, 512, \JSON_THROW_ON_ERROR);
            $this->assertTrue(Baseline::isDurabilityEquivalent($settings));
            $this->assertSame(Config::BUSY_TIMEOUT_MS, $settings['busy_timeout']);
            $this->assertSame(1000, $settings['wal_autocheckpoint']);
            $this->assertSame('immediate', $settings['transaction_mode']);
            $process->terminate();
            $this->assertSame(0, $process->wait(Config::KILL_GRACE_S + 1));
            $this->assertArrayNotHasKey($worker, ProcessTree::snapshot());
            $this->assertArrayNotHasKey($process->pid(), ProcessTree::snapshot());
            $this->assertFileDoesNotExist(Backend::endpoint($directory));
            $this->assertFalse($process->killed());
        } finally {
            if ($process->isRunning()) {
                $process->killTree();
            }
            (new Filesystem())->remove($socketDirectory);
            (new Filesystem())->remove($directory);
        }
    }
}
