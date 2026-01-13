<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Query\Builder\Method;

use JardisCore\DbQuery\Data\Contract\JoinStateInterface;
use JardisPsr\DbQuery\DbDeleteBuilderInterface;
use JardisPsr\DbQuery\DbQueryBuilderInterface;
use JardisPsr\DbQuery\DbUpdateBuilderInterface;

/**
 * Stateless builder for leftJoin() method logic
 *
 * Adds LEFT JOIN data to state.
 * Can be reused across DbQuery, DbUpdate, DbDelete without side effects.
 */
class LeftJoin
{
    /**
     * Add LEFT JOIN to state
     *
     * @template T of DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface
     * @param JoinStateInterface $state The state object (QueryState, UpdateState, DeleteState)
     * @param T $context The calling context
     * @param string|DbQueryBuilderInterface $container Table or subquery
     * @param string $constraint JOIN condition
     * @param string|null $alias Optional alias
     * @return T Returns context for chaining
     */
    public function __invoke(
        JoinStateInterface $state,
        DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface $context,
        string|DbQueryBuilderInterface $container,
        string $constraint,
        ?string $alias
    ): DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface {
        $join = [
            'join' => 'LEFT JOIN',
            'container' => $container,
            'alias' => $alias,
            'constraint' => $constraint
        ];
        $state->addJoin($join);

        return $context;
    }
}
