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
use Phayne\EventStore\EventStore;
use Phayne\EventStore\Projection\ProjectionManager;
use Phayne\Exception\InvalidArgumentException;
use Psr\Container\ContainerInterface;

use function sprintf;

/**
 * Class AbstractProjectionManagerFactory
 *
 * @package Phayne\EventStore\Pdo\Container
 * @author Julien Guittard <julien@phayne.com>
 */
abstract class AbstractProjectionManagerFactory implements
    ProvidesDefaultOptions,
    RequiresConfigId,
    RequiresMandatoryOptions
{
    use ConfigurationTrait;

    public static function __callStatic(string $name, array $arguments): ProjectionManager
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

    public function __invoke(ContainerInterface $container): ProjectionManager
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        $projectionManagerClassName = $this->projectionManagerClassName();

        return new $projectionManagerClassName(
            $container->get($config['event_store']),
            $container->get($config['connection']),
            $config['event_streams_table'],
            $config['projections_table']
        );
    }

    abstract protected function projectionManagerClassName(): string;

    public function dimensions(): iterable
    {
        return ['phayne', 'projection_manager'];
    }

    public function mandatoryOptions(): iterable
    {
        return ['connection'];
    }

    public function defaultOptions(): iterable
    {
        return [
            'event_store' => EventStore::class,
            'event_streams_table' => 'event_streams',
            'projections_table' => 'projections',
        ];
    }
}
