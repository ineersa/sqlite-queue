<?php

declare(strict_types=1);

use Ineersa\SqliteQueue\Protocol\ErrorCode;
use Ineersa\SqliteQueue\Protocol\Frame;
use Ineersa\SqliteQueue\Protocol\Limits;
use Ineersa\SqliteQueue\Protocol\Operation;
use Ineersa\SqliteQueue\Tests\Messenger\Fixtures\KeepalivePeerReply;

require dirname(__DIR__, 3).'/vendor/autoload.php';

$endpoint = $argv[1];
$gates = array_map(Operation::from(...), explode(',', $argv[2]));
$mode = KeepalivePeerReply::from($argv[3]);
$server = stream_socket_server('unix://'.$endpoint);
if (false === $server) {
    throw new RuntimeException('Could not create keepalive peer.');
}
fwrite(\STDOUT, "ready\n");
$socket = stream_socket_accept($server, 5);
if (false === $socket) {
    throw new RuntimeException('Keepalive peer did not accept the client.');
}
stream_set_timeout($socket, 5);
try {
    while (null !== $request = Frame::readUsing(static function (int $maximumBytes) use ($socket): ?string {
        $bytes = fread($socket, $maximumBytes);

        return false === $bytes || '' === $bytes ? null : $bytes;
    })) {
        $operation = Operation::from($request->control['op']);
        fwrite(\STDOUT, json_encode($request->control, \JSON_THROW_ON_ERROR)."\n");
        if (in_array($operation, $gates, true) && "release\n" !== fgets(\STDIN)) {
            throw new RuntimeException('Keepalive peer gate was not released.');
        }
        if (Operation::Keepalive === $operation && KeepalivePeerReply::Disconnect === $mode) {
            break;
        }
        $result = match ($operation) {
            Operation::Hello => ['max_payload' => Frame::MAX_PAYLOAD, 'max_connections' => Limits::MAX_CONNECTIONS],
            Operation::Send => 42,
            Operation::Keepalive => KeepalivePeerReply::Malformed === $mode ? 'not-an-integer' : 1_700_000_060_000,
            default => null,
        };
        $control = ['v' => 1, 'id' => $request->control['id'], 'ok' => true, 'result' => $result];
        if (Operation::Keepalive === $operation && KeepalivePeerReply::Expired === $mode) {
            $control = ['v' => 1, 'id' => $request->control['id'], 'ok' => false, 'error' => ['code' => ErrorCode::ExpiredReceipt->value]];
        }
        $bytes = (new Frame($control))->encode();
        if (false === @fwrite($socket, $bytes)) {
            break;
        }
    }
} finally {
    fclose($socket);
    fclose($server);
}
