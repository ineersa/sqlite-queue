<?php

declare(strict_types=1);

use function Castor\import;

// Composer provides Castor's types and version; avoid generated stubs and update notices.
putenv('CASTOR_GENERATE_STUBS=0');
putenv('CASTOR_DISABLE_VERSION_CHECK=1');

if (!defined('CASTOR_USE_CHDIR')) {
    define('CASTOR_USE_CHDIR', true);
}

import(__DIR__.'/.castor/reports.php');
import(__DIR__.'/.castor/tasks.php');
