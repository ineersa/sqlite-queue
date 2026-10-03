<?php

declare(strict_types=1);

namespace Ineersa\SqliteQueue\Bench;

/** Explicit child environment isolation with the caller's effective debug override. */
final class Process
{
    /** @param array<string, string|false> $overrides
     * @return array<string, string|false>
     */
    public static function environment(array $overrides = []): array
    {
        $inherited = array_keys(getenv() + $_ENV);
        $xdebugMode = getenv('XDEBUG_MODE');
        $runtime = false === $xdebugMode ? [] : ['XDEBUG_MODE' => $xdebugMode];

        return $overrides + $runtime + ['PATH' => '/usr/bin:/bin', 'LANG' => 'C', 'TZ' => 'UTC'] + array_fill_keys($inherited, false);
    }
}
