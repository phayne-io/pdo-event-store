<?php
//phpcs:ignorefile

/**
 * This file is part of phayne-io/pdo-event-store package.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @see       https://github.com/phayne-io/pdo-event-store for the canonical source repository
 * @copyright Copyright (c) 2023 Phayne. (https://phayne.io)
 */

declare(strict_types=1);

namespace Phayne\EventStore\Pdo\WriteLockStrategy;

use PDO;
use Phayne\EventStore\Pdo\WriteLockStrategy;

/**
 * Class PostgresAdvisoryLockStrategy
 *
 * @package Phayne\EventStore\Pdo\WriteLockStrategy
 * @author Julien Guittard <julien@phayne.com>
 */
final readonly class PostgresAdvisoryLockStrategy implements WriteLockStrategy
{
    public function __construct(private PDO $connection)
    {
    }

    public function getLock(string $name): bool
    {
        $this->connection->exec('SELECT pg_advisory_lock( hashtext(\'' . $name . '\') );');
        return true;
    }

    public function releaseLock(string $name): bool
    {
        $this->connection->exec('SELECT pg_advisory_unlock( hashtext(\'' . $name . '\') );');
        return true;
    }
}
