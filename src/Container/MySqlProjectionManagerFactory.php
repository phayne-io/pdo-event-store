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

namespace Phayne\EventStore\Pdo\Container;

use Phayne\EventStore\Pdo\Projection\MySqlProjectionManager;

/**
 * Class MySqlProjectionManagerFactory
 *
 * @package Phayne\EventStore\Pdo\Container
 * @author Julien Guittard <julien@phayne.com>
 */
class MySqlProjectionManagerFactory extends AbstractProjectionManagerFactory
{
    protected function projectionManagerClassName(): string
    {
        return MySqlProjectionManager::class;
    }
}
