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

use Phayne\EventStore\ActionEventEmitterEventStore;
use Phayne\EventStore\EventStore;
use Phayne\EventStore\Pdo\PostgresEventStore;
use Phayne\EventStore\TransactionalActionEventEmitterEventStore;
use Phayne\Messaging\Event\PhayneActionEventEmitter;
use Phayne\Messaging\Messaging\FQCNMessageFactory;

/**
 * Class PostgresEventStoreFactory
 *
 * @package Phayne\EventStore\Pdo\Container
 * @author Julien Guittard <julien@phayne.com>
 */
class PostgresEventStoreFactory extends AbstractEventStoreFactory
{
    protected function createActionEventEmitterEventStore(EventStore $eventStore): ActionEventEmitterEventStore
    {
        return new TransactionalActionEventEmitterEventStore(
            $eventStore,
            new PhayneActionEventEmitter([
                ActionEventEmitterEventStore::EVENT_APPEND_TO,
                ActionEventEmitterEventStore::EVENT_CREATE,
                ActionEventEmitterEventStore::EVENT_LOAD,
                ActionEventEmitterEventStore::EVENT_LOAD_REVERSE,
                ActionEventEmitterEventStore::EVENT_DELETE,
                ActionEventEmitterEventStore::EVENT_HAS_STREAM,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_METADATA,
                ActionEventEmitterEventStore::EVENT_UPDATE_STREAM_METADATA,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_NAMES,
                ActionEventEmitterEventStore::EVENT_FETCH_STREAM_NAMES_REGEX,
                ActionEventEmitterEventStore::EVENT_FETCH_CATEGORY_NAMES,
                ActionEventEmitterEventStore::EVENT_FETCH_CATEGORY_NAMES_REGEX,
                TransactionalActionEventEmitterEventStore::EVENT_BEGIN_TRANSACTION,
                TransactionalActionEventEmitterEventStore::EVENT_COMMIT,
                TransactionalActionEventEmitterEventStore::EVENT_ROLLBACK,
            ])
        );
    }

    protected function eventStoreClassName(): string
    {
        return PostgresEventStore::class;
    }

    public function defaultOptions(): iterable
    {
        return [
            'load_batch_size' => 1000,
            'event_streams_table' => 'event_streams',
            'message_factory' => FQCNMessageFactory::class,
            'wrap_action_event_emitter' => true,
            'metadata_enrichers' => [],
            'plugins' => [],
            'disable_transaction_handling' => false,
            'write_lock_strategy' => null,
        ];
    }
}
