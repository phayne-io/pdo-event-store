<?php
// phpcs:ignorefile

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

use PDO;
use PDOException;
use Phayne\EventStore\EventStore;
use Phayne\EventStore\EventStoreDecorator;
use Phayne\EventStore\Exception\ProjectionNotFound;
use Phayne\EventStore\Pdo\Exception\RuntimeException;
use Phayne\EventStore\Pdo\MySqlEventStore;
use Phayne\EventStore\Pdo\Util\Json;
use Phayne\EventStore\Projection\ProjectionManager;
use Phayne\EventStore\Projection\ProjectionStatus;
use Phayne\EventStore\Projection\Projector;
use Phayne\EventStore\Projection\Query;
use Phayne\EventStore\Projection\ReadModel;
use Phayne\EventStore\Projection\ReadModelProjector;
use Phayne\Exception\InvalidArgumentException;
use Phayne\Exception\OutOfBoundsException;

use function preg_match;
use function sprintf;

/**
 * Class MySqlProjectionManager
 *
 * @package Phayne\EventStore\Pdo\Projection
 * @author Julien Guittard <julien@phayne.com>
 */
final readonly class MySqlProjectionManager implements ProjectionManager
{
    public function __construct(
        private EventStore $eventStore,
        private PDO $connection,
        private string $eventStreamsTable = 'event_streams',
        private string $projectionsTable = 'projections'
    ) {
        while ($eventStore instanceof EventStoreDecorator) {
            $eventStore = $eventStore->getInnerEventStore();
        }

        if (! $eventStore instanceof MySqlEventStore) {
            throw new InvalidArgumentException('Unknown event store instance given');
        }
    }

    public function createQuery(array $options = []): Query
    {
        return new PdoEventStoreQuery(
            $this->eventStore,
            $this->connection,
            $this->eventStreamsTable,
            $options[Query::OPTION_PCNTL_DISPATCH] ?? Query::DEFAULT_PCNTL_DISPATCH
        );
    }

    public function createProjection(
        string $name,
        array $options = []
    ): Projector {
        return new PdoEventStoreProjector(
            $this->eventStore,
            $this->connection,
            $name,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[Projector::OPTION_LOCK_TIMEOUT_MS]
                ?? Projector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[Projector::OPTION_CACHE_SIZE]
                ?? Projector::DEFAULT_CACHE_SIZE,
            $options[Projector::OPTION_PERSIST_BLOCK_SIZE]
                ?? Projector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[Projector::OPTION_SLEEP]
                ?? Projector::DEFAULT_SLEEP,
            $options[PdoEventStoreProjector::OPTION_LOAD_COUNT]
                ?? PdoEventStoreProjector::DEFAULT_LOAD_COUNT,
            $options[Projector::OPTION_PCNTL_DISPATCH]
                ?? Projector::DEFAULT_PCNTL_DISPATCH,
            $options[Projector::OPTION_UPDATE_LOCK_THRESHOLD]
                ?? Projector::DEFAULT_UPDATE_LOCK_THRESHOLD,
            $options[PdoEventStoreProjector::OPTION_GAP_DETECTION] ?? null
        );
    }

    public function createReadModelProjection(
        string $name,
        ReadModel $readModel,
        array $options = []
    ): ReadModelProjector {
        return new PdoEventStoreReadModelProjector(
            $this->eventStore,
            $this->connection,
            $name,
            $readModel,
            $this->eventStreamsTable,
            $this->projectionsTable,
            $options[ReadModelProjector::OPTION_LOCK_TIMEOUT_MS]
                ?? ReadModelProjector::DEFAULT_LOCK_TIMEOUT_MS,
            $options[ReadModelProjector::OPTION_PERSIST_BLOCK_SIZE]
                ?? ReadModelProjector::DEFAULT_PERSIST_BLOCK_SIZE,
            $options[ReadModelProjector::OPTION_SLEEP]
                ?? ReadModelProjector::DEFAULT_SLEEP,
            $options[PdoEventStoreReadModelProjector::OPTION_LOAD_COUNT]
                ?? PdoEventStoreReadModelProjector::DEFAULT_LOAD_COUNT,
            $options[ReadModelProjector::OPTION_PCNTL_DISPATCH]
                ?? ReadModelProjector::DEFAULT_PCNTL_DISPATCH,
            $options[ReadModelProjector::OPTION_UPDATE_LOCK_THRESHOLD]
                ?? ReadModelProjector::DEFAULT_UPDATE_LOCK_THRESHOLD,
            $options[PdoEventStoreReadModelProjector::OPTION_GAP_DETECTION] ?? null
        );
    }

    public function deleteProjection(string $name, bool $deleteEmittedEvents): void
    {
        $sql = <<<EOT
UPDATE `$this->projectionsTable` SET status = ? WHERE name = ? LIMIT 1;
EOT;

        $status = $deleteEmittedEvents
            ? ProjectionStatus::DELETING_INCL_EMITTED_EVENTS->value
            : ProjectionStatus::DELETING->value;

        $statement = $this->connection->prepare($sql);

        try {
            $statement->execute([
                $status,
                $name,
            ]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (0 === $statement->rowCount()) {
            $sql = <<<EOT
SELECT * FROM `$this->projectionsTable` WHERE name = ? LIMIT 1;
EOT;
            $statement = $this->connection->prepare($sql);
            try {
                $statement->execute([$name]);
            } catch (PDOException) {
            }

            if ($statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
            }

            if (0 === $statement->rowCount()) {
                throw ProjectionNotFound::withName($name);
            }
        }
    }

    public function resetProjection(string $name): void
    {
        $sql = <<<EOT
UPDATE `$this->projectionsTable` SET status = ? WHERE name = ? LIMIT 1;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                ProjectionStatus::RESETTING->value,
                $name,
            ]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (0 === $statement->rowCount()) {
            $sql = <<<EOT
SELECT * FROM `$this->projectionsTable` WHERE name = ? LIMIT 1;
EOT;
            $statement = $this->connection->prepare($sql);
            try {
                $statement->execute([$name]);
            } catch (PDOException) {
            }

            if ($statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
            }

            if (0 === $statement->rowCount()) {
                throw ProjectionNotFound::withName($name);
            }
        }
    }

    public function stopProjection(string $name): void
    {
        $sql = <<<EOT
UPDATE `$this->projectionsTable` SET status = ? WHERE name = ? LIMIT 1;
EOT;

        $statement = $this->connection->prepare($sql);
        try {
            $statement->execute([
                ProjectionStatus::STOPPING->value,
                $name,
            ]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        if (0 === $statement->rowCount()) {
            $sql = <<<EOT
SELECT * FROM `$this->projectionsTable` WHERE name = ? LIMIT 1;
EOT;
            $statement = $this->connection->prepare($sql);
            try {
                $statement->execute([$name]);
            } catch (PDOException) {
            }

            if ($statement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
            }

            if (0 === $statement->rowCount()) {
                throw ProjectionNotFound::withName($name);
            }
        }
    }

    public function fetchProjectionNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        if (1 > $limit) {
            throw new OutOfBoundsException(sprintf(
                'Invalid limit "%d" given. Must be greater than 0.',
                $limit
            ));
        }

        if (0 > $offset) {
            throw new OutOfBoundsException(sprintf(
                'Invalid offset "%d" given. Must be greater or equal than 0.',
                $offset
            ));
        }

        $values = [];
        $whereCondition = '';

        if (null !== $filter) {
            $values[':filter'] = $filter;

            $whereCondition = 'WHERE `name` = :filter';
        }

        $query = <<<SQL
SELECT `name` FROM `$this->projectionsTable`
$whereCondition
ORDER BY `name` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $projectionNames = [];

        foreach ($result as $projectionName) {
            $projectionNames[] = $projectionName->name;
        }

        return $projectionNames;
    }

    public function fetchProjectionNamesRegex(string $regex, int $limit = 20, int $offset = 0): array
    {
        if (1 > $limit) {
            throw new OutOfBoundsException(sprintf(
                'Invalid limit "%d" given. Must be greater than 0.',
                $limit
            ));
        }

        if (0 > $offset) {
            throw new OutOfBoundsException(sprintf(
                'Invalid offset "%d" given. Must be greater or equal than 0.',
                $offset
            ));
        }

        if (empty($regex) || false === @preg_match("/$regex/", '')) {
            throw new InvalidArgumentException('Invalid regex pattern given');
        }

        $values = [];

        $values[':filter'] = $regex;

        $whereCondition = 'WHERE `name` REGEXP :filter';

        $query = <<<SQL
SELECT `name` FROM `$this->projectionsTable`
$whereCondition
ORDER BY `name` ASC
LIMIT $offset, $limit
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute($values);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo()[2];

            throw new RuntimeException(
                "Error $errorCode. Maybe the event streams table is not setup?\nError-Info: $errorInfo"
            );
        }

        $result = $statement->fetchAll();

        $projectionNames = [];

        foreach ($result as $projectionName) {
            $projectionNames[] = $projectionName->name;
        }

        return $projectionNames;
    }

    public function fetchProjectionStatus(string $name): ProjectionStatus
    {
        $query = <<<SQL
SELECT `status` FROM `$this->projectionsTable`
WHERE `name` = ?
LIMIT 1
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute([$name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $result = $statement->fetch();

        if (false === $result) {
            throw ProjectionNotFound::withName($name);
        }

        return ProjectionStatus::from($result->status);
    }

    public function fetchProjectionStreamPositions(string $name): array
    {
        $query = <<<SQL
SELECT `position` FROM `$this->projectionsTable`
WHERE `name` = ?
LIMIT 1
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute([$name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $result = $statement->fetch();

        if (false === $result) {
            throw ProjectionNotFound::withName($name);
        }

        return Json::decode($result->position);
    }

    public function fetchProjectionState(string $name): array
    {
        $query = <<<SQL
SELECT `state` FROM `$this->projectionsTable`
WHERE `name` = ?
LIMIT 1
SQL;

        $statement = $this->connection->prepare($query);
        $statement->setFetchMode(PDO::FETCH_OBJ);
        try {
            $statement->execute([$name]);
        } catch (PDOException) {
        }

        if ($statement->errorCode() !== '00000') {
            throw RuntimeException::fromStatementErrorInfo($statement->errorInfo());
        }

        $result = $statement->fetch();

        if (false === $result) {
            throw ProjectionNotFound::withName($name);
        }

        return Json::decode($result->state);
    }
}
