#!/usr/bin/env bash
# Opt-in network probe. Normal QA never runs Composer against external applications.
set -euo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd)
report="$root/var/flex-install/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$report"
composer create-project symfony/skeleton "$report/app" '^8.0' --no-dev --no-interaction >"$report/create.log" 2>&1
cd "$report/app"
# Start without any package bundle registration. Flex must create it during require.
if grep -q 'SqliteQueueBundle' config/bundles.php; then exit 1; fi
composer config repositories.sqlite-queue path "$root" >"$report/repository.log" 2>&1
composer require ineersa/sqlite-queue:@dev symfony/messenger:^8.0 --update-no-dev --no-interaction >"$report/require.log" 2>&1
composer install --no-dev --no-interaction >"$report/install.log" 2>&1
grep -q 'SqliteQueueBundle::class' config/bundles.php
cp config/bundles.php "$report/bundles.php"
php bin/console list --raw >"$report/commands.log" 2>&1
grep -q '^sqlite-queue:broker ' "$report/commands.log"
cat > config/packages/sqlite_queue.yaml <<'YAML'
sqlite_queue:
    visibility_timeout: 15000
framework:
    messenger:
        transports:
            offline_probe:
                dsn: 'sqlite-queue://jobs'
                options:
                    endpoint: '/tmp/sqlite-queue-flex-offline-no-broker.sock'
YAML
cat >> config/services.yaml <<'YAML'
    probe_transport:
        alias: messenger.transport.offline_probe
        public: true
YAML
php bin/console debug:config sqlite_queue >"$report/config.log" 2>&1
test ! -e /tmp/sqlite-queue-flex-offline-no-broker.sock
php -r '
require "vendor/autoload.php";
$kernel = new App\Kernel("dev", false);
$kernel->boot();
$transport = $kernel->getContainer()->get("probe_transport");
if (!$transport instanceof Ineersa\SqliteQueue\Messenger\Transport) {
    throw new RuntimeException("Offline DSN did not resolve to package transport.");
}
$transport->close();
$kernel->shutdown();
echo "offline_transport_resolved\n";
' >"$report/offline.log" 2>&1
printf '{"probe":"flex-install","status":"passed","report":"%s"}\n' "$report"
