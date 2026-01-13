<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Insert\Method;

use JardisCore\DbQuery\Data\InsertState;
use JardisPsr\DbQuery\DbInsertBuilderInterface;

/**
 * Handles OR IGNORE modifier for INSERT statements
 *
 * Silently ignores duplicate key errors instead of failing the insert operation.
 */
class OrIgnore
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
        $state->setOrIgnore(true);

        return $builder;
    }
}
