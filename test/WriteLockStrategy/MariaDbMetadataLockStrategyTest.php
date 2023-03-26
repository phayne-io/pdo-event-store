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
use PDOException;
use PDOStatement;
use Phayne\EventStore\Pdo\WriteLockStrategy\MariaDbMetadataLockStrategy;
use Phayne\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Prophecy\Argument;
use Prophecy\PhpUnit\ProphecyTrait;

/**
 * Class MariaDbMetadataLockStrategyTest
 *
 * @package PhayneTest\EventStore\Pdo\WriteLockStrategy
 * @author Julien Guittard <julien@phayne.com>
 */
class MariaDbMetadataLockStrategyTest extends TestCase
{
    use ProphecyTrait;

    public function testThrowsExceptionWhenPassingNegativeTimeout(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $connection = $this->prophesize(PDO::class);

        new MariaDbMetadataLockStrategy($connection->reveal(), -5);
    }

    public function testReturnsTrueWhenLockSuccessful(): void
    {
        $statement = $this->prophesize(PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $this->assertTrue($strategy->getLock('lock'));
    }

    public function testRequestsLockWithGivenName(): void
    {
        $statement = $this->prophesize(PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::containingString('GET_LOCK(\'lock\''))
            ->willReturn($statement->reveal())
            ->shouldBeCalled();

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $strategy->getLock('lock');
    }

    public function testRequestsLockWithoutTimeout(): void
    {
        $statement = $this->prophesize(PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::containingString('16777215'))
            ->willReturn($statement->reveal())
            ->shouldBeCalled();

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $strategy->getLock('lock');
    }

    public function testRequestsLockWithConfiguredTimeout(): void
    {
        $statement = $this->prophesize(PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '1'],
        ]);

        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::containingString('100'))
            ->willReturn($statement->reveal())
            ->shouldBeCalled();

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal(), 100);

        $strategy->getLock('lock');
    }

    public function testReturnsFalseOnStatementError(): void
    {
        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::any())->willReturn(false);

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    public function testReturnsFalseOnLockFailure(): void
    {
        $statement = $this->prophesize(PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => '0'],
        ]);

        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    public function testReturnsFalseOnLockKilled(): void
    {
        $statement = $this->prophesize(PDOStatement::class);
        $statement->fetchAll()->willReturn([
            0 => ['get_lock' => null],
        ]);

        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::any())->willReturn($statement->reveal());

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    public function testReturnsFalseOnDeadlockException()
    {
        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::any())->willThrow($this->prophesize(PDOException::class)->reveal());
        $connection->errorCode()->willReturn('3058');

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $this->assertFalse($strategy->getLock('lock'));
    }

    public function testReleasesLock(): void
    {
        $releaseStatement = $this->prophesize(PDOStatement::class);
        $releaseStatement->fetchAll()->shouldBeCalled();

        $connection = $this->prophesize(PDO::class);

        $connection->query(Argument::containingString('RELEASE_LOCK(\'lock\''))
            ->shouldBeCalled()
            ->willReturn($releaseStatement);

        $strategy = new MariaDbMetadataLockStrategy($connection->reveal());

        $this->assertTrue($strategy->releaseLock('lock'));
    }
}
