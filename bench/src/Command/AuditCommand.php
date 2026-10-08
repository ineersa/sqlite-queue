<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Command;

use Ineersa\SqliteQueue\Bench\Backend;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

/** Runs only in a separate observer process after measure and drain. */
final class AuditCommand extends Command
{
    public function __construct()
    {
        parent::__construct('audit');
    }

    protected function configure(): void
    {
        $this->setHidden(true)->addArgument('database', InputArgument::REQUIRED)->addArgument('backend', InputArgument::REQUIRED);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $database = $input->getArgument('database');
        if (!\is_string($database) || !is_file($database)) {
            throw new \InvalidArgumentException('Audit requires an existing database file.');
        }
        $backend = $input->getArgument('backend');
        if (!\is_string($backend)) {
            throw new \InvalidArgumentException('Audit requires a backend name.');
        }
        $selected = Backend::from($backend);
        $table = $selected->table();
        $connection = self::openReadOnly($database);
        $wall = (int) floor(microtime(true) * 1000);
        $ready = Backend::Broker === $selected ? 'available_at <= '.$wall.' AND coalesce(reserved_until, 0) <= '.$wall : "available_at <= '".gmdate('Y-m-d H:i:s')."' AND delivered_at IS NULL";
        $inflight = Backend::Broker === $selected ? 'reserved_until > '.$wall : 'delivered_at IS NOT NULL';
        $row = $connection->query('SELECT COUNT(*) AS remaining, coalesce(SUM(CASE WHEN '.$ready.' THEN 1 ELSE 0 END), 0) AS ready, coalesce(SUM(CASE WHEN '.$inflight.' THEN 1 ELSE 0 END), 0) AS inflight FROM '.$table)->fetch(\PDO::FETCH_ASSOC);
        if (!\is_array($row)) {
            throw new \RuntimeException('Inventory audit returned no row.');
        }
        $output->writeln(json_encode(['remaining' => (int) $row['remaining'], 'ready' => (int) $row['ready'], 'inflight' => (int) $row['inflight'], 'wall_anchor_ms' => $wall, 'reservation_coverage' => Backend::Doctrine === $selected ? 'persisted delivered_at markers; expired reservations are not reclassified' : 'active persisted reservation deadlines'], \JSON_THROW_ON_ERROR));

        return Command::SUCCESS;
    }

    private static function openReadOnly(string $database): \PDO
    {
        $absolute = realpath($database);
        if (false === $absolute) {
            throw new \InvalidArgumentException('Audit database path cannot be resolved.');
        }
        // SQLite URI mode=ro rejects writes and cannot create a missing database.
        $uri = 'file:'.str_replace('%2F', '/', rawurlencode($absolute)).'?mode=ro';

        return new \PDO('sqlite:'.$uri);
    }
}
