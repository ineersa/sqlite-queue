<?php

declare(strict_types=1);

while (($line = fgets(STDIN)) !== false) {
    fwrite(STDOUT, hrtime(true) . "\n");
    fflush(STDOUT);
}
