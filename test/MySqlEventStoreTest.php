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

namespace PhayneTest\EventStore\Pdo;

use ArrayIterator;
use Phayne\EventStore\Exception\ConcurrencyException;
use Phayne\EventStore\Metadata\MetadataMatcher;
use Phayne\EventStore\Metadata\Operator;
use Phayne\EventStore\Pdo\Exception\RuntimeException;
use Phayne\EventStore\Pdo\MySqlEventStore;
use Phayne\EventStore\Pdo\PersistenceStrategy\MySqlAggregateStreamStrategy;
use Phayne\EventStore\Pdo\PersistenceStrategy\MySqlPersistenceStrategy;
use Phayne\EventStore\Pdo\PersistenceStrategy\MySqlSingleStreamStrategy;
use Phayne\EventStore\Pdo\WriteLockStrategy;
use Phayne\EventStore\Pdo\WriteLockStrategy\MysqlMetadataLockStrategy;
use Phayne\EventStore\Stream;
use Phayne\EventStore\StreamName;
use Phayne\Messaging\Messaging\FQCNMessageFactory;
use Phayne\Messaging\Messaging\NoOpMessageConverter;
use PhayneTest\EventStore\Mock\UserCreated;
use PhayneTest\EventStore\Mock\UsernameChanged;
use Prophecy\Argument;
use Prophecy\PhpUnit\ProphecyTrait;
use Ramsey\Uuid\Uuid;

use function array_pop;
use function iterator_to_array;

/**
 * Class MySqlEventStoreTest
 *
 * @package PhayneTest\EventStore\Pdo
 * @author Julien Guittard <julien@phayne.com>
 * @group mysql
 */
class MySqlEventStoreTest extends AbstractPdoEventStoreTest
{
    use ProphecyTrait;

    /**
     * @var MySqlEventStore
     */
    protected $eventStore;

    protected function setUp(): void
    {
        if (TestUtil::getDatabaseDriver() !== 'pdo_mysql') {
            throw new RuntimeException('Invalid database driver');
        }

        $this->connection = TestUtil::getConnection();
        TestUtil::initDefaultDatabaseTables($this->connection);

        $this->setupEventStoreWith(new MySqlAggregateStreamStrategy(new NoOpMessageConverter()));
    }

    /**
     * @medium
     */
    public function testFetchesStreamNames(): void
    {
        parent::testFetchesStreamNames();
    }

    /**
     * @medium
     */
    public function testFetchesStreamCategories(): void
    {
        parent::testFetchesStreamNames();
    }

    public function testCannotCreateNewStreamIfTableNameIsAlreadyUsed(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Error during createSchemaFor');

        $streamName = new StreamName('foo');
        $schema = $this->persistenceStrategy->createSchema($this->persistenceStrategy->generateTableName($streamName));

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            $statement->execute();
        }

        $this->eventStore->create(new Stream($streamName, new ArrayIterator()));
    }

    public function testLoadsCorrectlyUsingSingleStreamPerAggregateTypeStrategy(): void
    {
        $this->setupEventStoreWith(new MySqlSingleStreamStrategy(new NoOpMessageConverter()), 5);

        $streamName = new StreamName('Phayne\Model\User');

        $stream = new Stream($streamName, new ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS, 'one');
        $events = iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser1Event = array_pop($events);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('_aggregate_id', Operator::EQUALS, 'two');
        $events = iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(100, $events);
        $lastUser2Event = array_pop($events);

        $this->assertEquals('Sandro', $lastUser1Event->payload()['name']);
        $this->assertEquals('Bradley', $lastUser2Event->payload()['name']);
    }

    public function testLoadsCorrectlyUsingSingleStream(): void
    {
        $batchMaxSize = 2;
        $this->setupEventStoreWith(new MySqlSingleStreamStrategy(new NoOpMessageConverter()), $batchMaxSize);

        $streamName = new StreamName('Phayne\Model\User');

        $stream = new Stream($streamName, new ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $iterator = $this->eventStore->load($streamName, 1, 5, $metadataMatcher);

        $this->assertCount(5, $iterator);
    }

    public function testFailsToWriteWithDuplicateVersionAndMulitpleStreamsPerAggregateStrategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->setupEventStoreWith(new MySqlSingleStreamStrategy(new NoOpMessageConverter()));

        $streamEvent = UserCreated::with(
            ['name' => 'Bog Sponge', 'email' => 'bob@sponge.net'],
            1
        );

        $aggregateId = Uuid::uuid4()->toString();

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $stream = new Stream(new StreamName('Phayne\Model\User'), new ArrayIterator([$streamEvent]));

        $this->eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->eventStore->appendTo(new StreamName('Phayne\Model\User'), new ArrayIterator([$streamEvent]));
    }

    public function testRequestsAndReleasesLocksWhenAppendingStreams(): void
    {
        $writeLockName = '__9a8289c32e4964a89cdc31fa900bc773ee5c23fa_write_lock';

        $lockStrategy = $this->prophesize(WriteLockStrategy::class);
        $lockStrategy->getLock(Argument::exact($writeLockName))->shouldBeCalled()->willReturn(true);
        $lockStrategy->releaseLock(Argument::exact($writeLockName))->shouldBeCalled()->willReturn(true);

        $connection = $this->prophesize(\PDO::class);

        $appendStatement = $this->prophesize(\PDOStatement::class);
        $appendStatement->execute(Argument::any())->willReturn(true);
        $appendStatement->errorInfo()->willReturn([0 => '00000']);
        $appendStatement->errorCode()->willReturn('00000');

        $connection->inTransaction()->willReturn(false);
        $connection->beginTransaction()->willReturn(true);
        $connection->prepare(Argument::any())->willReturn($appendStatement);

        $eventStore = new MySqlEventStore(
            new FQCNMessageFactory(),
            $connection->reveal(),
            new MySqlAggregateStreamStrategy(new NoOpMessageConverter()),
            10000,
            'event_streams',
            false,
            $lockStrategy->reveal()
        );

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $eventStore->appendTo(new StreamName('Phayne\Model\User'), new ArrayIterator([$streamEvent]));
    }

    public function testThrowsExceptionWhenLockFails(): void
    {
        $this->expectException(ConcurrencyException::class);

        $lockStrategy = $this->prophesize(WriteLockStrategy::class);
        $lockStrategy->getLock(Argument::any())->shouldBeCalled()->willReturn(false);

        $connection = $this->prophesize(\PDO::class);

        $eventStore = new MySqlEventStore(
            new FQCNMessageFactory(),
            $connection->reveal(),
            new MySqlAggregateStreamStrategy(new NoOpMessageConverter()),
            10000,
            'event_streams',
            false,
            $lockStrategy->reveal()
        );

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $eventStore->appendTo(new StreamName('Phayne\Model\User'), new ArrayIterator([$streamEvent]));
    }

    public function testCanWriteToDbWithLocksEnabled(): void
    {
        $eventStore = new MySqlEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new MySqlSingleStreamStrategy(new NoOpMessageConverter()),
            10000,
            'event_streams',
            false,
            new MysqlMetadataLockStrategy($this->connection)
        );

        $streamName = new StreamName('Phayne\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([]));

        $eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', Uuid::uuid4()->toString());
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $eventStore->appendTo($streamName, new ArrayIterator([$streamEvent]));

        $metadataMatcher = new MetadataMatcher();
        $iterator = $eventStore->load($streamName, 1, 5, $metadataMatcher);

        $this->assertCount(1, $iterator);
    }

    public function testRemovesStreamIfStreamTableHasntBeenCreated(): void
    {
        $strategy = $this->createMock(MySqlPersistenceStrategy::class);
        $strategy->method('createSchema')->willReturn(["SIGNAL SQLSTATE '45000';"]);
        $strategy->method('generateTableName')->willReturn('_non_existing_table');

        $this->setupEventStoreWith($strategy);

        $stream = new Stream(new StreamName('Phayne\Model\User'), new ArrayIterator());

        try {
            $this->eventStore->create($stream);
        } catch (RuntimeException) {
        }

        $this->assertFalse($this->eventStore->hasStream($stream->streamName));
    }
}
