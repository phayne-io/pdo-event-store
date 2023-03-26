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

namespace Phayne\EventStore\Pdo;

use Phayne\Messaging\Messaging\Message;
use Phayne\Messaging\Messaging\MessageConverter;

/**
 * Class DefaultMessageConverter
 *
 * @package Phayne\EventStore\Pdo
 * @author Julien Guittard <julien@phayne.com>
 */
class DefaultMessageConverter implements MessageConverter
{
    public function convertToArray(Message $domainMessage): array
    {
        return [
            'message_name' => $domainMessage->messageName(),
            'uuid' => $domainMessage->uuid()->toString(),
            'payload' => $domainMessage->payload(),
            'metadata' => $domainMessage->metadata(),
            'created_at' => $domainMessage->createdAt(),
        ];
    }
}
