<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Tests\Queue;

use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use Ineersa\SqliteQueue\Broker\BrokerFactory;
use Ineersa\SqliteQueue\Command\BrokerCommand;
use Ineersa\SqliteQueue\Sqlite\SqliteQueueStorage;
use Ineersa\SqliteQueue\Tests\Driver\DriverTestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Symfony\Component\Console\Tester\CommandTester;

final class SynchronousModeTest extends DriverTestCase
{
    public static function allowedModes(): iterable
    {
        yield 'normal' => [SqliteSynchronousMode::Normal, 1];
        yield 'full' => [SqliteSynchronousMode::Full, 2];
    }

    public function testOpenDefaultsToNormal(): void
    {
        $storage = SqliteQueueStorage::open($this->database->path());
        try {
            $connection = new \ReflectionProperty($storage, 'connection')->getValue($storage);
            $this->assertInstanceOf(SqliteConnection::class, $connection);
            $this->assertSame(SqliteSynchronousMode::Normal, $connection->getConfig()->getSynchronousMode());
            $result = $connection->query('PRAGMA synchronous');
            $this->assertSame(1, $result->fetchRow()['synchronous']);
            $result->close();
        } finally {
            $storage->close();
        }
    }

    #[DataProvider('allowedModes')]
    public function testOpenSelectsEffectiveMode(SqliteSynchronousMode $mode, int $expected): void
    {
        $storage = SqliteQueueStorage::open($this->database->path(), synchronous: $mode);
        try {
            $connection = new \ReflectionProperty($storage, 'connection')->getValue($storage);
            $result = $connection->query('PRAGMA synchronous');
            $this->assertSame($expected, $result->fetchRow()['synchronous']);
            $result->close();
        } finally {
            $storage->close();
        }
    }

    #[DataProvider('allowedModes')]
    public function testReadbackRejectsDifferentEffectiveMode(SqliteSynchronousMode $mode, int $expected): void
    {
        $connection = (new SqliteConnector())->connect((new SqliteConfig($this->database->path()))->withJournalMode(SqliteJournalMode::Wal)->withSynchronousMode($mode));
        $this->assertSame(SqliteSynchronousMode::Normal === $mode ? 1 : 2, $expected);
        $connection->query('PRAGMA synchronous='.(SqliteSynchronousMode::Normal === $mode ? 'FULL' : 'NORMAL'))->close();
        try {
            new SqliteQueueStorage($connection);
            $this->fail('Changed effective mode must be rejected.');
        } catch (\RuntimeException $error) {
            $this->assertSame('Effective synchronous mode does not match the configured mode.', $error->getMessage());
            $this->assertTrue($connection->isClosed());
        }
    }

    public static function unsupportedModes(): iterable
    {
        yield [SqliteSynchronousMode::Off];
        yield [SqliteSynchronousMode::Extra];
        yield [SqliteSynchronousMode::Automatic];
    }

    #[DataProvider('unsupportedModes')]
    public function testFactoryRejectsUnsupportedModeBeforeAcquisition(SqliteSynchronousMode $mode): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new BrokerFactory('/does/not/exist/db', '/does/not/exist/socket', synchronous: $mode);
    }

    #[DataProvider('unsupportedModes')]
    public function testOpenRejectsUnsupportedModeBeforeAcquisition(SqliteSynchronousMode $mode): void
    {
        $this->expectException(\InvalidArgumentException::class);
        SqliteQueueStorage::open('/does/not/exist/db', synchronous: $mode);
    }

    public static function invalidCliModes(): iterable
    {
        foreach (['off', 'extra', 'automatic', 'invalid', 'NORMAL', ''] as $value) {
            yield [$value];
        }
    }

    #[DataProvider('invalidCliModes')]
    public function testCliRejectsInvalidMode(string $value): void
    {
        $tester = new CommandTester(new BrokerCommand());
        $this->assertSame(1, $tester->execute(['--synchronous' => $value]));
        $this->assertStringContainsString('InvalidArgumentException', $tester->getDisplay());
    }

    public function testStandaloneDefaultsAndExplicitFull(): void
    {
        $this->assertSame('normal', (new BrokerCommand())->getDefinition()->getOption('synchronous')->getDefault());
        $this->assertSame('full', (new BrokerCommand(synchronous: SqliteSynchronousMode::Full))->getDefinition()->getOption('synchronous')->getDefault());
    }

    #[DataProvider('allowedModes')]
    public function testFactoryPropagatesEffectiveMode(SqliteSynchronousMode $mode, int $expected): void
    {
        $broker = (new BrokerFactory($this->database->path(), $this->database->directory().'/queue.sock', synchronous: $mode))->create();
        try {
            $storage = new \ReflectionProperty($broker, 'storage')->getValue($broker);
            $connection = new \ReflectionProperty($storage, 'connection')->getValue($storage);
            $result = $connection->query('PRAGMA synchronous');
            $this->assertSame($expected, $result->fetchRow()['synchronous']);
            $result->close();
        } finally {
            $broker->stop();
            $this->assertSame(0, $broker->run());
        }
    }
}
