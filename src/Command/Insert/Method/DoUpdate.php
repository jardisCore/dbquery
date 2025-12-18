<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Insert\Method;

use JardisCore\DbQuery\Data\InsertState;
use JardisPsr\DbQuery\DbInsertBuilderInterface;
use JardisPsr\DbQuery\ExpressionInterface;

/**
 * Handles DO UPDATE action for ON CONFLICT clause
 *
 * Defines which fields to update when a conflict occurs in PostgreSQL/SQLite upserts.
 */
class DoUpdate
{
    /**
     * @template T of DbInsertBuilderInterface
     * @param T $builder
     * @param array<string, string|int|float|bool|null|ExpressionInterface> $fields
     * @return T
     */
    public function __invoke(
        InsertState $state,
        DbInsertBuilderInterface $builder,
        array $fields
    ): DbInsertBuilderInterface {
        $state->setDoUpdateFields($fields);

        return $builder;
    }
}
