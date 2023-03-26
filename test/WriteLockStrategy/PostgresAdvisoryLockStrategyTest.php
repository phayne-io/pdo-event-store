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

use PDO;
use Phayne\EventStore\Pdo\WriteLockStrategy\PostgresAdvisoryLockStrategy;
use PHPUnit\Framework\TestCase;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * Class PostgresAdvisoryLockStrategyTest
 *
 * @package PhayneTest\EventStore\Pdo\WriteLockStrategy
 * @author Julien Guittard <julien@phayne.com>
 */
class PostgresAdvisoryLockStrategyTest extends TestCase
{
    use ProphecyTrait;

    private string $sqlLock = 'SELECT pg_advisory_lock( hashtext(\'lock\') );';
    private string $sqlUnlock = 'SELECT pg_advisory_unlock( hashtext(\'lock\') );';

    public function testReturnsTrueWhenLockSuccessful(): void
    {
        $connection = $this->prophesize(PDO::class);

        $connection->exec($this->sqlLock)->willReturn(1);

        $strategy = new PostgresAdvisoryLockStrategy($connection->reveal());

        $this->assertTrue($strategy->getLock('lock'));
    }

    public function testRequestsLockWithGivenName(): void
    {
        $connection = $this->prophesize(PDO::class);

        $connection->exec($this->sqlLock)
            ->shouldBeCalled()
            ->willReturn(1);

        $strategy = new PostgresAdvisoryLockStrategy($connection->reveal());

        $strategy->getLock('lock');
    }

    public function testReleasesLock(): void
    {
        $connection = $this->prophesize(PDO::class);

        $connection->exec($this->sqlUnlock)
            ->shouldBeCalled()
            ->willReturn(1);

        $strategy = new PostgresAdvisoryLockStrategy($connection->reveal());

        $this->assertTrue($strategy->releaseLock('lock'));
    }
}
