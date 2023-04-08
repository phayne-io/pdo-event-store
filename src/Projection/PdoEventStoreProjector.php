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

namespace Phayne\EventStore\Pdo\Projection;

use ArrayIterator;
use Closure;
use DateTimeImmutable;
use DateTimeZone;
use EmptyIterator;
use PDO;
use PDOException;
use Phayne\EventStore\EventStore;
use Phayne\EventStore\EventStoreDecorator;
use Phayne\EventStore\Exception\StreamNotFound;
use Phayne\EventStore\Metadata\MetadataMatcher;
use Phayne\EventStore\Pdo\Exception\ProjectionNotCreatedException;
use Phayne\EventStore\Pdo\Exception\RuntimeException;
use Phayne\EventStore\Pdo\PdoEventStore;
use Phayne\EventStore\Pdo\Util\Json;
use Phayne\EventStore\Pdo\Util\PostgresHelper;
use Phayne\EventStore\Projection\ProjectionStatus;
use Phayne\EventStore\Projection\Projector;
use Phayne\EventStore\Stream;
use Phayne\EventStore\StreamIterator\MergedStreamIterator;
use Phayne\EventStore\StreamName;
use Phayne\EventStore\Util\ArrayCache;
use Phayne\Exception\InvalidArgumentException;
use Phayne\Messaging\Messaging\Message;

use function array_fill;
use function array_keys;
use function array_merge;
use function array_values;
use function count;
use function floor;
use function implode;
use function is_array;
use function is_callable;
use function is_string;
use function pcntl_signal_dispatch;
use function usleep;

/**
 * Class PdoEventStoreProjector
 *
 * @package Phayne\EventStore\Pdo\Projection
 * @author Julien Guittard <julien@phayne.com>
 */
class PdoEventStoreProjector implements Projector
{
    use PostgresHelper {
        quoteIdent as pgQuoteIdent;
        extractSchema as pgExtractSchema;
    }

    public const OPTION_GAP_DETECTION = 'gap_detection';
    public const OPTION_LOAD_COUNT = 'load_count';
    public const DEFAULT_LOAD_COUNT = null;

    private ProjectionStatus $status = ProjectionStatus::IDLE;

    private ArrayCache $cachedStreamNames;

    /**
     * @var callable | null
     */
    private $initCallback = null;

    private ?Closure $handler = null;

    private array $handlers = [];

    private array $state = [];

    private ?array $query = null;

    private ?string $currentStreamName;

    private array $streamPositions = [];

    private string $vendor;

    private ?MetadataMatcher $metadataMatcher = null;

    private int $eventCounter = 0;

    private bool $streamCreated = false;

    private bool $isStopped = false;

    private ?DateTimeImmutable $lastLockUpdate = null;

    public function __construct(
        private readonly EventStore $eventStore,
        private readonly PDO $connection,
        private readonly string $name,
        private readonly string $eventStreamsTable,
        private readonly string $projectionsTable,
        private readonly int $lockTimeoutMs,
        int $cacheSize,
        private readonly int $persistBlockSize,
        private readonly int $sleep,
        private readonly ?int $loadCount = null,
        private readonly bool $triggerPcntlSignalDispatch = false,
        private readonly int $updateLockThreshold = 0,
        private readonly ?GapDetection $gapDetection = null
    ) {
        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof PdoEventStore) {
            throw new InvalidArgumentException('Unknown event store instance given');
        }

        $this->cachedStreamNames = new ArrayCache($cacheSize);
        $this->vendor = $this->connection->getAttribute(PDO::ATTR_DRIVER_NAME);
    }

    public function init(Closure $callback): Projector
    {
        if (null !== $this->initCallback) {
            throw new RuntimeException('Projection already initialized');
        }

        $callback = Closure::bind($callback, $this->createHandlerContext($this->currentStreamName));

        $result = $callback();

        if (is_array($result)) {
            $this->state = $result;
        }

        $this->initCallback = $callback;

        return $this;
    }

    public function fromStream(string $streamName, MetadataMatcher $metadataMatcher = null): Projector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        $this->query['streams'][] = $streamName;
        $this->metadataMatcher = $metadataMatcher;

        return $this;
    }

    public function fromStreams(string ...$streamNames): Projector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        foreach ($streamNames as $streamName) {
            $this->query['streams'][] = $streamName;
        }

        return $this;
    }

    public function fromCategory(string $name): Projector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        $this->query['categories'][] = $name;

        return $this;
    }

    public function fromCategories(string ...$names): Projector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        foreach ($names as $name) {
            $this->query['categories'][] = $name;
        }

        return $this;
    }

    public function fromAll(): Projector
    {
        if (null !== $this->query) {
            throw new RuntimeException('From was already called');
        }

        $this->query['all'] = true;

        return $this;
    }

    public function when(array $handlers): Projector
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new RuntimeException('When was already called');
        }

        foreach ($handlers as $eventName => $handler) {
            if (! is_string($eventName)) {
                throw new InvalidArgumentException('Invalid event name given, string expected');
            }

            if (! $handler instanceof Closure) {
                throw new InvalidArgumentException('Invalid handler given, Closure expected');
            }

            $this->handlers[$eventName] = Closure::bind(
                $handler,
                $this->createHandlerContext($this->currentStreamName)
            );
        }

        return $this;
    }

    public function whenAny(Closure $closure): Projector
    {
        if (null !== $this->handler || ! empty($this->handlers)) {
            throw new RuntimeException('When was already called');
        }

        $this->handler = Closure::bind($closure, $this->createHandlerContext($this->currentStreamName));

        return $this;
    }

    public function reset(): void
    {
        $this->streamPositions = [];
        $callback = $this->initCallback;
        $this->state = [];

        if (is_callable($callback)) {
            $result = $callback();

            if (is_array($result)) {
                $this->state = $result;
            }
        }

        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
UPDATE $projectionsTable SET position = ?, state = ?, status = ?
WHERE name = ?
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                Json::encode($this->streamPositions),
                Json::encode($this->state),
                $this->status->value,
                $this->name,
            ]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        try {
            $this->eventStore->delete(new StreamName($this->name));
        } catch (StreamNotFound) {
        }
    }

    public function stop(): void
    {
        $this->persist();
        $this->isStopped = true;

        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $stopProjectionSql = <<<EOT
UPDATE $projectionsTable SET status = ? WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($stopProjectionSql);
        try {
            $statement->execute([ProjectionStatus::IDLE->value, $this->name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $this->status = ProjectionStatus::IDLE;
    }

    public function state(): array
    {
        return $this->state;
    }

    public function name(): string
    {
        return $this->name;
    }

    public function emit(Message $event): void
    {
        if (! $this->streamCreated && ! $this->eventStore->hasStream(new StreamName($this->name))) {
            $this->eventStore->create(new Stream(new StreamName($this->name), new EmptyIterator()));
            $this->streamCreated = true;
        }

        $this->linkTo($this->name, $event);
    }

    public function linkTo(string $streamName, Message $event): void
    {
        $sn = new StreamName($streamName);

        if ($this->cachedStreamNames->has($streamName)) {
            $append = true;
        } else {
            $this->cachedStreamNames->rollingAppend($streamName);
            $append = $this->eventStore->hasStream($sn);
        }

        if ($append) {
            $this->eventStore->appendTo($sn, new ArrayIterator([$event]));
        } else {
            $this->eventStore->create(new Stream($sn, new ArrayIterator([$event])));
        }
    }

    public function delete(bool $deleteEmittedEvents): void
    {
        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $deleteProjectionSql = <<<EOT
DELETE FROM $projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($deleteProjectionSql);
        try {
            $statement->execute([$this->name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if ($deleteEmittedEvents) {
            try {
                $this->eventStore->delete(new StreamName($this->name));
            } catch (StreamNotFound) {
            }
        }

        $this->isStopped = true;

        $callback = $this->initCallback;

        $this->state = [];

        if (is_callable($callback)) {
            $result = $callback();

            if (is_array($result)) {
                $this->state = $result;
            }
        }

        $this->streamPositions = [];
    }

    public function run(bool $keepRunning = true): void
    {
        if (null === $this->query || (null === $this->handler && empty($this->handlers))) {
            throw new RuntimeException('No handlers configured');
        }

        switch ($this->fetchRemoteStatus()) {
            case ProjectionStatus::STOPPING:
                $this->load();
                $this->stop();

                return;
            case ProjectionStatus::DELETING:
                $this->delete(false);

                return;
            case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS:
                $this->delete(true);

                return;
            case ProjectionStatus::RESETTING:
                $this->reset();
                break;
            default:
                break;
        }

        if (! $this->projectionExists()) {
            $this->createProjection();
        }

        $this->acquireLock();

        $this->prepareStreamPositions();
        $this->load();

        $singleHandler = null !== $this->handler;

        $this->isStopped = false;

        try {
            do {
                $eventStreams = [];
                $streamEvents = []; // free up memory from PDO statement

                foreach ($this->streamPositions as $streamName => $position) {
                    try {
                        $eventStreams[$streamName] = $this->eventStore->load(
                            new StreamName($streamName),
                            $position + 1,
                            $this->loadCount,
                            $this->metadataMatcher
                        );
                    } catch (StreamNotFound) {
                        continue;
                    }
                }

                $streamEvents = new MergedStreamIterator(array_keys($eventStreams), ...array_values($eventStreams));

                if ($singleHandler) {
                    $gapDetected = ! $this->handleStreamWithSingleHandler($streamEvents);
                } else {
                    $gapDetected = ! $this->handleStreamWithHandlers($streamEvents);
                }

                if ($gapDetected && $this->gapDetection) {
                    $sleep = $this->gapDetection->getSleepForNextRetry();

                    usleep($sleep);
                    $this->gapDetection->trackRetry();
                    $this->persist();
                } else {
                    $this->gapDetection && $this->gapDetection->resetRetries();

                    if (0 === $this->eventCounter) {
                        usleep($this->sleep);
                        $this->updateLock();
                    } else {
                        $this->persist();
                    }
                }

                $this->eventCounter = 0;

                if ($this->triggerPcntlSignalDispatch) {
                    pcntl_signal_dispatch();
                }

                switch ($this->fetchRemoteStatus()) {
                    case ProjectionStatus::STOPPING:
                        $this->stop();
                        break;
                    case ProjectionStatus::DELETING:
                        $this->delete(false);
                        break;
                    case ProjectionStatus::DELETING_INCL_EMITTED_EVENTS:
                        $this->delete(true);
                        break;
                    case ProjectionStatus::RESETTING:
                        $this->reset();
                        if ($keepRunning) {
                            $this->startAgain();
                        }
                        break;
                    default:
                        break;
                }

                $this->prepareStreamPositions();
            } while ($keepRunning && ! $this->isStopped);
        } finally {
            $this->releaseLock();
        }
    }

    private function fetchRemoteStatus(): ProjectionStatus
    {
        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
SELECT status FROM $projectionsTable WHERE name = ? LIMIT 1;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([$this->name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the projection table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetch(PDO::FETCH_OBJ);

        if (false === $result) {
            return ProjectionStatus::RUNNING;
        }

        return ProjectionStatus::from($result->status);
    }

    private function handleStreamWithSingleHandler(MergedStreamIterator $events): bool
    {
        $handler = $this->handler;

        /* @var Message $event */
        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                pcntl_signal_dispatch();
            }

            $this->currentStreamName = $events->streamName();

            if (
                $this->gapDetection
                && $this->gapDetection->isGapInStreamPosition(
                    (int)$this->streamPositions[$this->currentStreamName],
                    (int)$key
                )
                && $this->gapDetection->shouldRetryToFillGap(
                    new DateTimeImmutable('now', new DateTimeZone('UTC')),
                    $event
                )
            ) {
                return false;
            }

            $this->streamPositions[$this->currentStreamName] = $key;
            $this->eventCounter++;

            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            $this->persistAndFetchRemoteStatusWhenBlockSizeThresholdReached();

            if ($this->isStopped) {
                break;
            }
        }

        return true;
    }

    private function handleStreamWithHandlers(MergedStreamIterator $events): bool
    {
        /* @var Message $event */
        foreach ($events as $key => $event) {
            if ($this->triggerPcntlSignalDispatch) {
                pcntl_signal_dispatch();
            }

            $this->currentStreamName = $events->streamName();

            if (
                $this->gapDetection &&
                $this->gapDetection->isGapInStreamPosition(
                    (int)$this->streamPositions[$this->currentStreamName],
                    (int)$key
                ) &&
                $this->gapDetection->shouldRetryToFillGap(new DateTimeImmutable('now', new DateTimeZone('UTC')), $event)
            ) {
                return false;
            }

            $this->streamPositions[$this->currentStreamName] = $key;

            $this->eventCounter++;

            if (! isset($this->handlers[$event->messageName()])) {
                $this->persistAndFetchRemoteStatusWhenBlockSizeThresholdReached();

                if ($this->isStopped) {
                    break;
                }

                continue;
            }

            $handler = $this->handlers[$event->messageName()];
            $result = $handler($this->state, $event);

            if (is_array($result)) {
                $this->state = $result;
            }

            $this->persistAndFetchRemoteStatusWhenBlockSizeThresholdReached();

            if ($this->isStopped) {
                break;
            }
        }

        return true;
    }

    private function persistAndFetchRemoteStatusWhenBlockSizeThresholdReached(): void
    {
        if ($this->eventCounter === $this->persistBlockSize) {
            $this->persist();
            $this->eventCounter = 0;

            $this->status = $this->fetchRemoteStatus();

            if ($this->status !== ProjectionStatus::RUNNING && $this->status !== ProjectionStatus::IDLE) {
                $this->isStopped = true;
            }
        }
    }

    private function createHandlerContext(?string &$streamName): object
    {
        return new class ($this, $streamName)
        {
            private Projector $projector;

            private ?string $streamName;

            public function __construct(Projector $projector, ?string &$streamName)
            {
                $this->projector = $projector;
                $this->streamName = &$streamName;
            }

            public function stop(): void
            {
                $this->projector->stop();
            }

            public function linkTo(string $streamName, Message $event): void
            {
                $this->projector->linkTo($streamName, $event);
            }

            public function emit(Message $event): void
            {
                $this->projector->emit($event);
            }

            public function streamName(): ?string
            {
                return $this->streamName;
            }
        };
    }

    private function load(): void
    {
        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
SELECT position, state FROM $projectionsTable WHERE name = ? LIMIT 1;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([$this->name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $result = $statement->fetch(PDO::FETCH_OBJ);

        $this->streamPositions = array_merge($this->streamPositions, Json::decode($result->position));
        $state = Json::decode($result->state);

        if (! empty($state)) {
            $this->state = $state;
        }
    }

    private function projectionExists(): bool
    {
        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
SELECT 1 FROM $projectionsTable WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([$this->name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        return (bool) $statement->fetch(PDO::FETCH_NUM);
    }

    private function createProjection(): void
    {
        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
INSERT INTO $projectionsTable (name, position, state, status, locked_until)
VALUES (?, '{}', '{}', ?, NULL);
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([$this->name, $this->status->value]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw ProjectionNotCreatedException::with($this->name);
        }
    }

    private function acquireLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
        $nowString = $now->format('Y-m-d\TH:i:s.u');

        $lockUntilString = $this->createLockUntilString($now);

        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
UPDATE $projectionsTable SET locked_until = ?, status = ? WHERE name = ? AND (locked_until IS NULL OR locked_until < ?);
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([$lockUntilString, ProjectionStatus::RUNNING->value, $this->name, $nowString]);
        } catch (PDOException) {
        }

        if ($statement->rowCount() !== 1) {
            if ($statement->errorCode() !== '00000') {
                $errorCode = $statement->errorCode();
                $errorInfo = $statement->errorInfo()[2];

                throw new RuntimeException(
                    "Error $errorCode. Maybe the projection table is not setup?\nError-Info: $errorInfo"
                );
            }

            throw new RuntimeException('Another projection process is already running');
        }

        $this->status = ProjectionStatus::RUNNING;
        $this->lastLockUpdate = $now;
    }

    private function updateLock(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        if (! $this->shouldUpdateLock($now)) {
            return;
        }

        $lockUntilString = $this->createLockUntilString($now);

        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
UPDATE $projectionsTable SET locked_until = ? WHERE name = ?;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute(
                [
                    $lockUntilString,
                    $this->name,
                ]
            );
        } catch (PDOException) {
        }

        if ($statement->rowCount() !== 1) {
            if ($statement->errorCode() !== '00000') {
                $errorCode = $statement->errorCode();
                $errorInfo = $statement->errorInfo()[2];

                throw new RuntimeException(
                    "Error $errorCode. Maybe the projection table is not setup?\nError-Info: $errorInfo"
                );
            }

            throw new RuntimeException('Unknown error occurred');
        }

        $this->lastLockUpdate = $now;
    }

    private function releaseLock(): void
    {
        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
UPDATE $projectionsTable SET locked_until = NULL, status = ? WHERE name = ?;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([ProjectionStatus::IDLE->value, $this->name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $this->status = ProjectionStatus::IDLE;
    }

    private function persist(): void
    {
        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        $lockUntilString = $this->createLockUntilString($now);

        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $sql = <<<EOT
UPDATE $projectionsTable SET position = ?, state = ?, locked_until = ? 
WHERE name = ?
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                Json::encode($this->streamPositions),
                Json::encode($this->state),
                $lockUntilString,
                $this->name,
            ]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }
    }

    private function prepareStreamPositions(): void
    {
        $streamPositions = [];

        if (isset($this->query['all'])) {
            $eventStreamsTable = $this->quoteTableName($this->eventStreamsTable);
            $sql = <<<EOT
SELECT real_stream_name FROM $eventStreamsTable WHERE real_stream_name NOT LIKE '$%';
EOT;
            $statement = $this->connection->prepare($sql);
            try {
                $statement->execute();
            } catch (PDOException) {
            }

            if ($statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
            }

            while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
                $streamPositions[$row->real_stream_name] = 0;
            }

            $this->streamPositions = array_merge($streamPositions, $this->streamPositions);

            return;
        }

        if (isset($this->query['categories'])) {
            $rowPlaces = implode(', ', array_fill(0, count($this->query['categories']), '?'));

            $eventStreamsTable = $this->quoteTableName($this->eventStreamsTable);
            $sql = <<<EOT
SELECT real_stream_name FROM $eventStreamsTable WHERE category IN ($rowPlaces);
EOT;
            $statement = $this->connection->prepare($sql);

            try {
                $statement->execute($this->query['categories']);
            } catch (PDOException $exception) {
                // ignore and check error code
            }

            if ($statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
            }

            while ($row = $statement->fetch(PDO::FETCH_OBJ)) {
                $streamPositions[$row->real_stream_name] = 0;
            }

            $this->streamPositions = array_merge($streamPositions, $this->streamPositions);

            return;
        }

        // stream names given
        foreach ($this->query['streams'] as $streamName) {
            $streamPositions[$streamName] = 0;
        }

        $this->streamPositions = array_merge($streamPositions, $this->streamPositions);
    }

    private function createLockUntilString(DateTimeImmutable $from): string
    {
        $lockTimeoutMs = $this->lockTimeoutMs % 1000;
        $lockTimeoutSeconds = ($this->lockTimeoutMs - $lockTimeoutMs) / 1000;

        return $from->modify("+{$lockTimeoutSeconds} seconds +{$lockTimeoutMs} milliseconds")->format('Y-m-d\TH:i:s.u');
    }

    private function shouldUpdateLock(DateTimeImmutable $now): bool
    {
        if ($this->lastLockUpdate === null || $this->updateLockThreshold === 0) {
            return true;
        }

        $intervalSeconds = floor($this->updateLockThreshold / 1000);

        //Create an interval based on seconds
        $updateLockThreshold = new \DateInterval("PT{$intervalSeconds}S");
        //and manually add split seconds
        $updateLockThreshold->f = ($this->updateLockThreshold % 1000) / 1000;

        $threshold = $this->lastLockUpdate->add($updateLockThreshold);

        return $threshold <= $now;
    }

    private function quoteTableName(string $tableName): string
    {
        return match ($this->vendor) {
            'pgsql' => $this->pgQuoteIdent($tableName),
            default => "`$tableName`",
        };
    }

    private function startAgain(): void
    {
        $this->isStopped = false;

        $newStatus = ProjectionStatus::RUNNING;

        $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));

        $projectionsTable = $this->quoteTableName($this->projectionsTable);
        $startProjectionSql = <<<EOT
UPDATE $projectionsTable SET status = ?, locked_until = ? WHERE name = ?;
EOT;
        $statement = $this->connection->prepare($startProjectionSql);
        try {
            $statement->execute([
                $newStatus->value,
                $this->createLockUntilString($now),
                $this->name,
            ]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $this->status = $newStatus;
        $this->lastLockUpdate = $now;
    }
}
