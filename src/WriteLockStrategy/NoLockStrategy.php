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

namespace Phayne\EventStore\Pdo\WriteLockStrategy;

use Phayne\EventStore\Pdo\WriteLockStrategy;

/**
 * Class NoLockStrategy
 *
 * @package Phayne\EventStore\Pdo\WriteLockStrategy
 * @author Julien Guittard <julien@phayne.com>
 */
final class NoLockStrategy implements WriteLockStrategy
{
    public function getLock(string $name): bool
    {
        return true;
    }

    public function releaseLock(string $name): bool
    {
        return true;
    }
}
