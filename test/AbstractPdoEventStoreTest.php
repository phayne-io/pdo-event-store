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
use DateTimeImmutable;
use DateTimeZone;
use PDO;
use Phayne\EventStore\Exception\ConcurrencyException;
use Phayne\EventStore\Metadata\FieldType;
use Phayne\EventStore\Metadata\MetadataMatcher;
use Phayne\EventStore\Metadata\Operator;
use Phayne\EventStore\Pdo\Exception\RuntimeException;
use Phayne\EventStore\Pdo\MariaDbEventStore;
use Phayne\EventStore\Pdo\MySqlEventStore;
use Phayne\EventStore\Pdo\PersistenceStrategy;
use Phayne\EventStore\Pdo\PostgresEventStore;
use Phayne\EventStore\Pdo\Util\PostgresHelper;
use Phayne\EventStore\Stream;
use Phayne\EventStore\StreamName;
use Phayne\Messaging\Messaging\FQCNMessageFactory;
use Phayne\Messaging\Messaging\Message;
use PhayneTest\EventStore\AbstractEventStoreTest;
use PhayneTest\EventStore\Mock\TestDomainEvent;
use PhayneTest\EventStore\Mock\UserCreated;
use PhayneTest\EventStore\Mock\UsernameChanged;
use Ramsey\Uuid\Uuid;

use function uniqid;
use function str_replace;

/**
 * Class AbstractPdoEventStoreTest
 *
 * @package PhayneTest\EventStore\Pdo
 * @author Julien Guittard <julien@phayne.com>
 */
abstract class AbstractPdoEventStoreTest extends AbstractEventStoreTest
{
    use PostgresHelper {
        quoteIdent as pgQuoteIdent;
        extractSchema as pgExtractSchema;
    }

    protected ?PDO $connection = null;

    protected PersistenceStrategy $persistenceStrategy;

    protected function setupEventStoreWith(
        PersistenceStrategy $persistenceStrategy,
        int $loadBatchSize = 10000,
        string $eventStreamsTable = 'event_streams',
        bool $disableTransactionHandling = false
    ): void {
        $this->persistenceStrategy = $persistenceStrategy;

        switch (TestUtil::getDatabaseVendor()) {
            case 'mariadb':
                $class = MariaDbEventStore::class;
                break;
            case 'mysql':
                $class = MySqlEventStore::class;
                break;
            case 'postgres':
                $class = PostgresEventStore::class;
                break;
        }

        $this->eventStore = new $class(
            new FQCNMessageFactory(),
            $this->connection,
            $persistenceStrategy,
            $loadBatchSize,
            $eventStreamsTable,
            $disableTransactionHandling
        );
    }

    protected function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    protected function eventStreamsTable(): string
    {
        return 'event_streams';
    }

    protected function quoteTableName(string $tableName): string
    {
        return match (TestUtil::getDatabaseVendor()) {
            'postgres' => $this->pgQuoteIdent($tableName),
            default => "`$tableName`",
        };
    }

    public function dataProviderPayloadStaysSameThroughStore(): array
    {
        return [
            [[]],
            [['null' => null]],
            [['an int' => 10]],
            [['an int' => -10]],
            [['a float' => -1.1]],
            [['a float' => -1.0]],
            [['a float' => -0.0]],
            [['a float' => 0.0]],
            [['a float' => 1.0]],
            [['a float' => 1.1]],
            [['a unicoded char' => "\u{1000}"]],
        ];
    }

    /**
     * @dataProvider dataProviderPayloadStaysSameThroughStore
     */
    public function testPayloadStaysSameThroughStore(array $payload): void
    {
        $event = TestDomainEvent::with($payload, 1);
        $streamName = new StreamName('Phayne\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]));
        $this->eventStore->create($stream);

        $eventStream = $this->eventStore->load($streamName);

        /** @var TestDomainEvent $event */
        foreach ($eventStream as $event) {
            $this->assertEquals($payload, $event->payload());
        }
    }

    public function testHandlesNotExistingEventStreamsTable(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Maybe the event streams table is not setup?');

        $this->connection->exec("DROP TABLE {$this->quoteTableName($this->eventStreamsTable())};");

        $this->eventStore->create($this->getTestStream());
    }

    public function testThrowsExceptionUsingAggregateStreamStrategyIfAggregateVersionIsMissingInMetadata(): void
    {
        $this->expectException(RuntimeException::class);

        $event = TestDomainEvent::fromArray([
            'uuid' => Uuid::uuid4()->toString(),
            'message_name' => 'test-message',
            'created_at' => new DateTimeImmutable('now', new DateTimeZone('UTC')),
            'payload' => [],
            'metadata' => [],
        ]);

        $stream = new Stream(new StreamName('Phayne\Model\User'), new ArrayIterator([$event]));

        $this->eventStore->create($stream);
    }

    public function testFailsToWriteDuplicateVersionUsingAggregateStreamStrategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $streamEvent = UserCreated::with(
            ['name' => 'John Doe', 'email' => 'john@doe.cpm'],
            1
        );

        $aggregateId = Uuid::uuid4()->toString();

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $stream = new Stream(new StreamName('Phayne\Model\User'), new ArrayIterator([$streamEvent]));

        $this->eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->eventStore->appendTo(new StreamName('Phayne\Model\User'), new ArrayIterator([$streamEvent]));
    }

    public function testThrowsExceptionWhenFetchingStreamNamesWithMissingDbTable(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec("DROP TABLE {$this->quoteTableName($this->eventStreamsTable())};");
        $this->eventStore->fetchStreamNames(null, null, 200, 0);
    }

    public function testThrowsExceptionWhenFetchingStreamNamesRegexWithMissingDbTable(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec("DROP TABLE {$this->quoteTableName($this->eventStreamsTable())};");
        $this->eventStore->fetchStreamNamesRegex('^foo', null, 200, 0);
    }

    public function itThrowsExceptionWhenFetchingCategoryNamesWithMissingDbTable(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec("DROP TABLE {$this->quoteTableName($this->eventStreamsTable())};");
        $this->eventStore->fetchCategoryNames(null, 200, 0);
    }

    public function testThrowsExceptionWhenFetchingCategoryNamesRegexWithMissingDbTable(): void
    {
        $this->expectException(RuntimeException::class);

        $this->connection->exec("DROP TABLE {$this->quoteTableName($this->eventStreamsTable())};");
        $this->eventStore->fetchCategoryNamesRegex('^foo', 200, 0);
    }

    public function testReturnsOnlyMatchedMetadata(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);

        $uuid = $event->uuid()->toString();
        $uuid2 = Uuid::uuid4()->toString();
        $before = $event->createdAt()->modify('-5 secs')->format('Y-m-d\TH:i:s.u');
        $later = $event->createdAt()->modify('+5 secs')->format('Y-m-d\TH:i:s.u');

        $stream = new Stream(new StreamName('Phayne\Model\User'), new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::EQUALS, 'bar');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::NOT_EQUALS, 'baz');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::GREATER_THAN, 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int2', Operator::GREATER_THAN_EQUALS, 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::IN, [4, 5, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int3', Operator::LOWER_THAN, 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int4', Operator::LOWER_THAN_EQUALS, 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::NOT_IN, [4, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::REGEX, '^b[a]r$');

        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::EQUALS,
            $uuid,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::NOT_EQUALS,
            $uuid2,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN,
            $before,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN_EQUALS,
            $before,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::IN,
            [$uuid, $uuid2],
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN,
            $later,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN_EQUALS,
            $later,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::NOT_IN,
            [$before, $later],
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_name',
            Operator::REGEX,
            '.+UserCreated$',
            FieldType::MESSAGE_PROPERTY
        );

        $streamEvents = $this->eventStore->load($stream->streamName, 1, null, $metadataMatcher);

        $this->assertCount(1, $streamEvents);
    }

    public function testReturnsOnlyMatchedMetadataReverse(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);

        $uuid = $event->uuid()->toString();
        $uuid2 = Uuid::uuid4()->toString();
        $before = $event->createdAt()->modify('-5 secs')->format('Y-m-d\TH:i:s.u');
        $later = $event->createdAt()->modify('+5 secs')->format('Y-m-d\TH:i:s.u');

        $streamName = new StreamName('Phayne\Model\User');

        $stream = new Stream($streamName, new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::EQUALS, 'bar');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::NOT_EQUALS, 'baz');
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::GREATER_THAN, 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int2', Operator::GREATER_THAN_EQUALS, 4);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::IN, [4, 5, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int3', Operator::LOWER_THAN, 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int4', Operator::LOWER_THAN_EQUALS, 7);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('int', Operator::NOT_IN, [4, 6]);
        $metadataMatcher = $metadataMatcher->withMetadataMatch('foo', Operator::REGEX, '^b[a]r$');

        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::EQUALS,
            $uuid,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::NOT_EQUALS,
            $uuid2,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN,
            $before,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN_EQUALS,
            $before,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::IN,
            [$uuid, $uuid2],
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN,
            $later,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN_EQUALS,
            $later,
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::NOT_IN,
            [$before, $later],
            FieldType::MESSAGE_PROPERTY
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_name',
            Operator::REGEX,
            '.+UserCreated$',
            FieldType::MESSAGE_PROPERTY
        );

        $streamEvents = $this->eventStore->loadReverse($stream->streamName, 1, null, $metadataMatcher);

        $this->assertCount(1, $streamEvents);
    }

    public function testReturnsOnlyMatchedMessageProperty(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);

        $uuid = $event->uuid()->toString();
        $uuid2 = Uuid::uuid4()->toString();
        $createdAt = $event->createdAt()->format('Y-m-d\TH:i:s.u');

        $before = $event->createdAt()->modify('-5 secs')->format('Y-m-d\TH:i:s.u');
        $later = $event->createdAt()->modify('+5 secs')->format('Y-m-d\TH:i:s.u');

        $streamName = new StreamName('Phayne\Model\User');

        $stream = new Stream($streamName, new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::EQUALS,
            $uuid2,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::NOT_EQUALS,
            $uuid,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN,
            $later,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN_EQUALS,
            $later,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::IN,
            [$before, $later],
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN,
            $before,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN_EQUALS,
            $before,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::NOT_IN,
            [$before, $createdAt, $later],
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_name',
            Operator::REGEX,
            'foobar',
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->load($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());
    }

    public function testReturnsOnlyMatchedMessagePropertyReverse(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('foo', 'bar');
        $event = $event->withAddedMetadata('int', 5);
        $event = $event->withAddedMetadata('int2', 4);
        $event = $event->withAddedMetadata('int3', 6);
        $event = $event->withAddedMetadata('int4', 7);

        $uuid = $event->uuid()->toString();
        $uuid2 = Uuid::uuid4()->toString();
        $createdAt = $event->createdAt()->format('Y-m-d\TH:i:s.u');
        $before = $event->createdAt()->modify('-5 secs')->format('Y-m-d\TH:i:s.u');
        $later = $event->createdAt()->modify('+5 secs')->format('Y-m-d\TH:i:s.u');

        $streamName = new StreamName('Phayne\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::EQUALS,
            $uuid2,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_id',
            Operator::NOT_EQUALS,
            $uuid,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN,
            $later,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::GREATER_THAN_EQUALS,
            $later,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::IN,
            [$before, $later],
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN,
            $before,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::LOWER_THAN_EQUALS,
            $before,
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'created_at',
            Operator::NOT_IN,
            [$before, $createdAt, $later],
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            'event_name',
            Operator::REGEX,
            'foobar',
            FieldType::MESSAGE_PROPERTY
        );

        $result = $this->eventStore->loadReverse($streamName, 1, null, $metadataMatcher);

        $this->assertFalse($result->valid());
    }

    public function testAddsEventPositionToMetadataIfFieldNotOccupied(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);

        $streamName = new StreamName('Phayne\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $streamEvents = $this->eventStore->load($streamName);

        $readEvent = $streamEvents->current();

        $this->assertArrayHasKey('_position', $readEvent->metadata());
        $this->assertEquals(1, $readEvent->metadata()['_position']);
    }

    public function testDoesNotAddEventPositionToMetadataIfFieldIsOccupied(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('_position', 'foo');

        $streamName = new StreamName('Phayne\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $streamEvents = $this->eventStore->load($streamName);

        $readEvent = $streamEvents->current();

        $this->assertArrayHasKey('_position', $readEvent->metadata());
        $this->assertSame('foo', $readEvent->metadata()['_position']);
    }

    public function testDoesntDoubleEscapeMetadata(): void
    {
        $event = UserCreated::with(['name' => 'John'], 1);
        $event = $event->withAddedMetadata('_aggregate_type', 'Phayne\Model\User');

        $streamName = new StreamName('Phayne\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            '_aggregate_type',
            Operator::EQUALS,
            'Phayne\Model\User'
        );
        $streamEvents = $this->eventStore->load($streamName, 0, 10, $metadataMatcher);

        $this->assertCount(1, $streamEvents);
    }

    public function testDoesNotUseJsonForceObjectForStreamMetadataAndEventPayloadAndMetadata(): void
    {
        $event = UserCreated::with(['name' => ['John', 'Jane']], 1);
        $event = $event->withAddedMetadata('key', 'value');

        $streamName = new StreamName('Phayne\Model\User');
        $stream = new Stream($streamName, new ArrayIterator([$event]), ['some' => ['metadata', 'as', 'well']]);

        $this->eventStore->create($stream);

        $statement = $this->connection->prepare("SELECT * FROM {$this->quoteTableName($this->eventStreamsTable())}");
        $statement->execute();

        $result = $statement->fetch(\PDO::FETCH_ASSOC);

        // mariadb does not add spaces to json, while mysql and postgres do, so strip them
        $this->assertSame('{"some":["metadata","as","well"]}', \str_replace(' ', '', $result['metadata']));

        $statement = $this->connection->prepare(
            sprintf(
                'SELECT * FROM %s',
                $this->quoteTableName($this->persistenceStrategy->generateTableName($streamName))
            )
        );

        $statement->execute();

        $result = $statement->fetch(PDO::FETCH_ASSOC);

        // mariadb does not add spaces to json, while mysql and postgres do, so strip them
        $this->assertSame('{"name":["John","Jane"]}', str_replace(' ', '', $result['payload']));
    }

    /**
     * @return Message[]
     */
    protected function getMultipleTestEvents(): array
    {
        $events = [];

        $event = UserCreated::with(['name' => 'Alex'], 1);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');

        $event = UserCreated::with(['name' => 'Sascha'], 1);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

        for ($i = 2; $i < 100; $i++) {
            $event = UsernameChanged::with(['name' => uniqid('name_')], $i);
            $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

            $event = UsernameChanged::with(['name' => uniqid('name_')], $i);
            $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');
        }

        $event = UsernameChanged::with(['name' => 'Sandro'], 100);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');

        $event = UsernameChanged::with(['name' => 'Bradley'], 100);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

        return $events;
    }
}
