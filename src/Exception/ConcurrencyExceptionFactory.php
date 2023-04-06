<?php

/**
 * This file is part of phayne-io/pdo-event-store package.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @see       https://github.com/phayne-io/pdo-event-store for the canonical source repository
 * @copyright Copyright (c) 2023 Phayne. (https://phayne.io)
 */

declare(strict_types=1);

namespace Phayne\EventStore\Pdo\Exception;

use Phayne\EventStore\Exception\ConcurrencyException;

use function sprintf;

/**
 * Class ConcurrencyExceptionFactory
 *
 * @package Phayne\EventStore\Pdo\Exception
 * @author Julien Guittard <julien@phayne.com>
 */
class ConcurrencyExceptionFactory
{
    public static function fromStatementErrorInfo(array $errorInfo): ConcurrencyException
    {
        return new ConcurrencyException(
            sprintf(
                "Some of the aggregate IDs or event IDs have already been used in the same stream. The PDO error should contain more information:\nError %s.\nError-Info: %s", // phpcs:ignore
                $errorInfo[0],
                $errorInfo[2]
            )
        );
    }

    public static function failedToAcquireLock(): ConcurrencyException
    {
        return new ConcurrencyException('Failed to acquire lock for writing to stream');
    }
}
