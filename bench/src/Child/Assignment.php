<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench\Child;

final readonly class Assignment
{
    public function __construct(
        public string $directory,
        public string $database,
        public array $workload,
        public Role $role,
        public int $index,
    ) {
        if ($index < 0) {
            throw new \InvalidArgumentException('Worker index must be nonnegative.');
        }
    }

    public static function fromFile(string $path, Role $role, int $index): self
    {
        if (!is_file($path) || !is_readable($path)) {
            throw new \InvalidArgumentException('Cannot read worker assignment: ' . $path);
        }

        $config = json_decode(file_get_contents($path), true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($config) || !is_string($config['directory'] ?? null)
            || !is_string($config['database'] ?? null) || !is_array($config['workload'] ?? null)) {
            throw new \InvalidArgumentException('Worker assignment requires directory, database, and workload.');
        }

        return new self($config['directory'], $config['database'], $config['workload'], $role, $index);
    }

    public function label(): string
    {
        return $this->role->value . '-' . $this->index;
    }

    public function path(string $relative): string
    {
        return $this->directory . '/' . $relative;
    }
}
