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

namespace Phayne\EventStore\Pdo\PersistenceStrategy;

use Iterator;
use Phayne\EventStore\Pdo\DefaultMessageConverter;
use Phayne\EventStore\Pdo\Exception\RuntimeException;
use Phayne\EventStore\Pdo\Util\Json;
use Phayne\EventStore\Pdo\Util\PostgresHelper;
use Phayne\EventStore\StreamName;
use Phayne\Messaging\Messaging\MessageConverter;

use function array_merge;
use function sha1;
use function sprintf;

/**
 * Class PostgresAggregateStreamStrategy
 *
 * @package Phayne\EventStore\Pdo\PersistenceStrategy
 * @author Julien Guittard <julien@phayne.com>
 */
class PostgresAggregateStreamStrategy implements PostgresPersistenceStrategy
{
    use PostgresHelper;

    private MessageConverter $messageConverter;

    public function __construct(?MessageConverter $messageConverter = null)
    {
        $this->messageConverter = $messageConverter ?? new DefaultMessageConverter();
    }

    public function createSchema(string $tableName): array
    {
        $tableName = $this->quoteIdent($tableName);

        $statement = <<<EOT
CREATE TABLE $tableName (
    no BIGSERIAL,
    event_id UUID NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    payload JSON NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (no),
    UNIQUE (event_id)
);
EOT;

        return array_merge($this->getSchemaCreationSchema($tableName), [
            $statement,
            "CREATE UNIQUE INDEX on $tableName ((metadata->>'_aggregate_version'));",
        ]);
    }

    public function columnNames(): array
    {
        return [
            'no',
            'event_id',
            'event_name',
            'payload',
            'metadata',
            'created_at',
        ];
    }

    public function prepareData(Iterator $streamEvents): array
    {
        $data = [];

        foreach ($streamEvents as $event) {
            $eventData = $this->messageConverter->convertToArray($event);

            if (! isset($eventData['metadata']['_aggregate_version'])) {
                throw new RuntimeException('_aggregate_version is missing in metadata');
            }

            $data[] = $eventData['metadata']['_aggregate_version'];
            $data[] = $eventData['uuid'];
            $data[] = $eventData['message_name'];
            $data[] = Json::encode($eventData['payload']);
            $data[] = Json::encode($eventData['metadata']);
            $data[] = $eventData['created_at']->format('Y-m-d\TH:i:s.u');
        }

        return $data;
    }

    public function generateTableName(StreamName $streamName): string
    {
        $streamName = $streamName->toString();
        $table = '_' . sha1($streamName);

        if ($schema = $this->extractSchema($streamName)) {
            $table = $schema . '.' . $table;
        }

        return $table;
    }

    private function getSchemaCreationSchema(string $tableName): array
    {
        if (! $schemaName = $this->extractSchema($tableName)) {
            return [];
        }

        return [sprintf(
            'CREATE SCHEMA IF NOT EXISTS %s',
            $schemaName
        )];
    }
}
