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

use Phayne\EventStore\Pdo\Exception\JsonException;

use function json_decode;
use function json_encode;
use function json_last_error;
use function json_last_error_msg;

use const JSON_UNESCAPED_UNICODE;
use const JSON_UNESCAPED_SLASHES;
use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_BIGINT_AS_STRING;
use const JSON_ERROR_NONE;

/**
 * Class Json
 *
 * @package Phayne\EventStore\Pdo\Util
 * @author Julien Guittard <julien@phayne.com>
 */
class Json
{
    public static function encode($value): string
    {
        $flags = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION;

        $json = json_encode($value, $flags);

        if (JSON_ERROR_NONE !== $error = json_last_error()) {
            throw new JsonException(json_last_error_msg(), $error);
        }

        return $json;
    }

    /**
     * @param string $json
     * @return array
     * @throws JsonException
     */
    public static function decode(string $json): array
    {
        $data = json_decode($json, true, 512, JSON_BIGINT_AS_STRING);

        if (JSON_ERROR_NONE !== $error = json_last_error()) {
            throw new JsonException(json_last_error_msg(), $error);
        }

        return $data;
    }
}
