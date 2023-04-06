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

use DateTimeImmutable;
use DateTimeZone;
use PDO;
use PDOException;
use PDOStatement;
use Phayne\EventStore\Pdo\Exception\RuntimeException;
use Phayne\EventStore\Pdo\Util\Json;
use Phayne\EventStore\StreamIterator\StreamIterator;
use Phayne\Messaging\Messaging\Message;
use Phayne\Messaging\Messaging\MessageFactory;
use stdClass;

use function array_key_exists;
use function strlen;

/**
 * Class PdoStreamIterator
 *
 * @package Phayne\EventStore\Pdo
 * @author Julien Guittard <julien@phayne.com>
 */
class PdoStreamIterator implements StreamIterator
{
    private stdClass|false|null $currentItem = null;

    private int $currentKey = -1;

    private int $batchPosition = 0;

    private int $currentFromNumber;

    public function __construct(
        private PDOStatement $selectStatement,
        private readonly PDOStatement $countStatement,
        private readonly MessageFactory $messageFactory,
        private readonly int $batchSize,
        private readonly int $fromNumber,
        private readonly ?int $count,
        private readonly bool $forward
    ) {
        $this->currentFromNumber = $fromNumber;
        $this->next();
    }

    public function current(): ?Message
    {
        if (false === $this->currentItem) {
            return null;
        }

        $createdAt = $this->currentItem->created_at;

        if (strlen($createdAt) === 19) {
            $createdAt = $createdAt . '.000';
        }

        $createdAt = DateTimeImmutable::createFromFormat(
            'Y-m-d H:i:s.u',
            $createdAt,
            new DateTimeZone('UTC')
        );

        $metadata = Json::decode($this->currentItem->metadata);

        if (! array_key_exists('_position', $metadata)) {
            $metadata['_position'] = $this->currentItem->no;
        }

        $payload = Json::decode($this->currentItem->payload);

        return $this->messageFactory->createMessageFromArray($this->currentItem->event_name, [
            'uuid' => $this->currentItem->event_id,
            'created_at' => $createdAt,
            'payload' => $payload,
            'metadata' => $metadata,
        ]);
    }

    public function next(): void
    {
        if ($this->count && ($this->count - 1) === $this->currentKey) {
            $this->currentKey = -1;
            $this->currentItem = false;

            return;
        }

        $this->currentItem = $this->selectStatement->fetch();

        if (false !== $this->currentItem) {
            $this->currentKey++;
            $this->currentItem->no = (int) $this->currentItem->no;
            $this->currentFromNumber = $this->currentItem->no;
        } else {
            $this->batchPosition++;
            if ($this->forward) {
                $from = $this->currentFromNumber + 1;
            } else {
                $from = $this->currentFromNumber - 1;
            }
            $this->selectStatement = $this->buildSelectStatement($from);
            try {
                $this->selectStatement->execute();
            } catch (PDOException) {
            }

            if ($this->selectStatement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($this->selectStatement->errorInfo());
            }

            $this->selectStatement->setFetchMode(PDO::FETCH_OBJ);

            $this->currentItem = $this->selectStatement->fetch();

            if (false === $this->currentItem) {
                $this->currentKey = -1;
            } else {
                $this->currentKey++;
                $this->currentItem->no = (int) $this->currentItem->no;
                $this->currentFromNumber = $this->currentItem->no;
            }
        }
    }

    public function key(): mixed
    {
        if (null === $this->currentItem) {
            return false;
        }

        return $this->currentItem->no;
    }

    public function valid(): bool
    {
        return false !== $this->currentItem;
    }

    public function rewind(): void
    {
        if ($this->currentKey !== 0) {
            $this->batchPosition = 0;

            $this->selectStatement = $this->buildSelectStatement($this->fromNumber);
            try {
                $this->selectStatement->execute();
            } catch (PDOException) {
            }

            if ($this->selectStatement->errorCode() !== '00000') {
                throw RuntimeException::fromStatementErrorInfo($this->selectStatement->errorInfo());
            }

            $this->currentItem = null;
            $this->currentKey = -1;
            $this->currentFromNumber = $this->fromNumber;

            $this->next();
        }
    }

    public function count(): int
    {
        $this->countStatement->bindValue(':fromNumber', $this->fromNumber, PDO::PARAM_INT);

        try {
            if ($this->countStatement->execute()) {
                $count = (int) $this->countStatement->fetchColumn();

                return null === $this->count ? $count : \min($count, $this->count);
            }
        } catch (PDOException) {
        }

        return 0;
    }

    private function buildSelectStatement(int $fromNumber): PDOStatement
    {
        if (null === $this->count || $this->count < ($this->batchSize * ($this->batchPosition + 1))) {
            $limit = $this->batchSize;
        } else {
            $limit = $this->count - ($this->batchSize * ($this->batchPosition + 1));
        }

        $this->selectStatement->bindValue(':fromNumber', $fromNumber, PDO::PARAM_INT);
        $this->selectStatement->bindValue(':limit', $limit, PDO::PARAM_INT);

        return $this->selectStatement;
    }
}
