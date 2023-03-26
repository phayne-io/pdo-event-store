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
use PDOException;
use Phayne\EventStore\Pdo\WriteLockStrategy;

/**
 * Class MysqlMetadataLockStrategy
 *
 * @package Phayne\EventStore\Pdo\WriteLockStrategy
 * @author Julien Guittard <julien@phayne.com>
 */
final readonly class MysqlMetadataLockStrategy implements WriteLockStrategy
{
    public function __construct(private PDO $connection, private int $timeout = -1)
    {
    }

    public function getLock(string $name): bool
    {
        try {
            $res = $this->connection->query('SELECT GET_LOCK(\'' . $name . '\', ' . $this->timeout . ') as \'get_lock\'');
        } catch (PDOException $e) {
            // ER_USER_LOCK_DEADLOCK: we only care for deadlock errors and fail locking
            if ('3058' === $this->connection->errorCode()) {
                return false;
            }

            throw $e;
        }

        if (! $res) {
            return false;
        }

        $lockStatus = $res->fetchAll();

        if ('1' === $lockStatus[0]['get_lock'] || 1 === $lockStatus[0]['get_lock']) {
            return true;
        }

        return false;
    }

    public function releaseLock(string $name): bool
    {
        $this->connection->exec('DO RELEASE_LOCK(\'' . $name . '\') as \'release_lock\'');
        return true;
    }
}
