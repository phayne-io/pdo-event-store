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

namespace PhayneTest\EventStore\Pdo\WriteLockStrategy;

use Phayne\EventStore\Pdo\WriteLockStrategy\NoLockStrategy;
use PHPUnit\Framework\TestCase;

/**
 * Class NoLockStrategyTest
 *
 * @package PhayneTest\EventStore\Pdo\WriteLockStrategy
 * @author Julien Guittard <julien@phayne.com>
 */
class NoLockStrategyTest extends TestCase
{
    public function testAlwaysSucceedsLockingAndReleasing(): void
    {
        $strategy = new NoLockStrategy();

        $this->assertTrue($strategy->getLock('write_lock'));
        $this->assertTrue($strategy->releaseLock('write_lock'));
    }
}
