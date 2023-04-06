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

namespace Phayne\EventStore\Pdo\Util;

use function strpos;
use function substr;

/**
 * Trait PostgresHelper
 *
 * @package Phayne\EventStore\Pdo\Util
 * @author Julien Guittard <julien@phayne.com>
 */
trait PostgresHelper
{
    private function quoteIdent(string $ident): string
    {
        $pos = strpos($ident, '.');

        if (false === $pos) {
            return '"' . $ident . '"';
        }

        $schema = substr($ident, 0, $pos);
        $table = substr($ident, $pos + 1);

        return '"' . $schema . '"."' . $table . '"';
    }

    private function extractSchema(string $ident): ?string
    {
        if (false === ($pos = strpos($ident, '.'))) {
            return null;
        }

        return substr($ident, 0, $pos);
    }
}
