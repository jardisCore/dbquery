<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Insert\Method;

use JardisCore\DbQuery\Data\InsertState;
use JardisPsr\DbQuery\DbInsertBuilderInterface;

/**
 * Handles REPLACE modifier for INSERT statements
 *
 * Replaces existing row on duplicate key (DELETE then INSERT operation).
 */
class Replace
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
        $state->setReplace(true);

        return $builder;
    }
}
