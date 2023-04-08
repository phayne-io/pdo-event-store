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

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use Phayne\EventStore\ActionEventEmitterEventStore;
use Phayne\EventStore\EventStore;
use Phayne\EventStore\Exception\ConfigurationException;
use Phayne\EventStore\Metadata\MetadataEnricher;
use Phayne\EventStore\Metadata\MetadataEnricherAggregate;
use Phayne\EventStore\Metadata\MetadataEnricherPlugin;
use Phayne\EventStore\Pdo\WriteLockStrategy\NoLockStrategy;
use Phayne\EventStore\Plugin\Plugin;
use Phayne\Exception\InvalidArgumentException;
use Psr\Container\ContainerInterface;

use function sprintf;

/**
 * Class AbstractEventStoreFactory
 *
 * @package Phayne\EventStore\Pdo\Container
 * @author Julien Guittard <julien@phayne.com>
 */
abstract class AbstractEventStoreFactory implements
    ProvidesDefaultOptions,
    RequiresConfigId,
    RequiresMandatoryOptions
{
    use ConfigurationTrait;

    public static function __callStatic(string $name, array $arguments): EventStore
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }

        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(private readonly string $configId = 'default')
    {
    }

    public function __invoke(ContainerInterface $container): EventStore
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        if (isset($config['write_lock_strategy'])) {
            $writeLockStrategy = $container->get($config['write_lock_strategy']);
        } else {
            $writeLockStrategy = new NoLockStrategy();
        }

        $eventStoreClassName = $this->eventStoreClassName();

        $eventStore = new $eventStoreClassName(
            $container->get($config['message_factory']),
            $container->get($config['connection']),
            $container->get($config['persistence_strategy']),
            $config['load_batch_size'],
            $config['event_streams_table'],
            $config['disable_transaction_handling'],
            $writeLockStrategy
        );

        if (! $config['wrap_action_event_emitter']) {
            return $eventStore;
        }

        $wrapper = $this->createActionEventEmitterEventStore($eventStore);

        foreach ($config['plugins'] as $pluginAlias) {
            $plugin = $container->get($pluginAlias);

            if (! $plugin instanceof Plugin) {
                throw ConfigurationException::configurationError(sprintf(
                    'Plugin %s does not implement the Plugin interface',
                    $pluginAlias
                ));
            }

            $plugin->attachToEventStore($wrapper);
        }

        $metadataEnrichers = [];

        foreach ($config['metadata_enrichers'] as $metadataEnricherAlias) {
            $metadataEnricher = $container->get($metadataEnricherAlias);

            if (! $metadataEnricher instanceof MetadataEnricher) {
                throw ConfigurationException::configurationError(sprintf(
                    'Metadata enricher %s does not implement the MetadataEnricher interface',
                    $metadataEnricherAlias
                ));
            }

            $metadataEnrichers[] = $metadataEnricher;
        }

        if (\count($metadataEnrichers) > 0) {
            $plugin = new MetadataEnricherPlugin(
                new MetadataEnricherAggregate($metadataEnrichers)
            );

            $plugin->attachToEventStore($wrapper);
        }

        return $wrapper;
    }

    abstract protected function createActionEventEmitterEventStore(
        EventStore $eventStore
    ): ActionEventEmitterEventStore;

    abstract protected function eventStoreClassName(): string;

    public function dimensions(): iterable
    {
        return ['phayne', 'event_store'];
    }

    public function mandatoryOptions(): iterable
    {
        return [
            'connection',
            'persistence_strategy',
        ];
    }
}
