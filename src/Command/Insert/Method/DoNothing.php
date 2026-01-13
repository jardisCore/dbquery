<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Insert\Method;

use JardisCore\DbQuery\Data\InsertState;
use JardisPsr\DbQuery\DbInsertBuilderInterface;

/**
 * Handles DO NOTHING action for ON CONFLICT clause
 *
 * Silently ignores conflicts without updating when a conflict occurs in PostgreSQL/SQLite upserts.
 */
class DoNothing
{
    /**
     * @template T of DbInsertBuilderInterface
     * @param T $builder
     * @return T
     */
    public function __invoke(
        InsertState $state,
        DbInsertBuilderInterface $builder
    ): DbInsertBuilderInterface {
        $state->setDoNothing(true);

        return $builder;
    }
}
