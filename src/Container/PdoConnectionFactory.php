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

use Phayne\Exception\InvalidArgumentException;
use Psr\Container\ContainerInterface;
use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use PDO;

use function sprintf;

/**
 * Class PdoConnectionFactory
 *
 * @package Phayne\EventStore\Pdo\Container
 * @author Julien Guittard <julien@phayne.com>
 */
class PdoConnectionFactory implements ProvidesDefaultOptions, RequiresConfigId, RequiresMandatoryOptions
{
    use ConfigurationTrait;

    public static function __callStatic(string $name, array $arguments): PDO
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

    public function __invoke(ContainerInterface $container): PDO
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        return new PDO(
            $this->buildConnectionDns($config),
            $config['user'],
            $config['password']
        );
    }

    public function dimensions(): iterable
    {
        return [
            'phayne',
            'pdo_connection',
        ];
    }

    public function defaultOptions(): iterable
    {
        return [
            'host' => '127.0.0.1',
            'dbname' => 'event_store',
            'charset' => 'utf8',
        ];
    }

    public function mandatoryOptions(): iterable
    {
        return [
            'schema',
            'user',
            'password',
            'port',
        ];
    }

    private function buildConnectionDns(array $params): string
    {
        $dsn = $params['schema'] . ':';

        if ($params['host'] !== '') {
            $dsn .= 'host=' . $params['host'] . ';';
        }

        if ($params['port'] !== '') {
            $dsn .= 'port=' . $params['port'] . ';';
        }

        $dsn .= 'dbname=' . $params['dbname'] . ';';

        if ('mysql' === $params['schema']) {
            $dsn .= 'charset=' . $params['charset'] . ';';
        } elseif ('pgsql' === $params['schema']) {
            $dsn .= "options='--client_encoding=" . $params['charset'] . "'";
        }

        return $dsn;
    }
}
