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

namespace Phayne\EventStore\Pdo\Exception;

use Phayne\Exception\RuntimeException;

use function sprintf;

/**
 * Class ProjectionNotCreatedException
 *
 * @package Phayne\EventStore\Pdo\Exception
 * @author Julien Guittard <julien@phayne.com>
 */
class ProjectionNotCreatedException extends RuntimeException
{
    public static function with(string $projectionName): ProjectionNotCreatedException
    {
        return new self(sprintf('Projection "%s" was not created', $projectionName));
    }
}
