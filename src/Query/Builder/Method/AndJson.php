<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Query\Builder\Method;

use JardisCore\DbQuery\Query\Condition\QueryJsonCondition;
use JardisPsr\DbQuery\DbQueryJsonConditionBuilderInterface;

/**
 * Stateless builder for JSON AND clause initialization
 *
 * Can be reused across DbQuery, DbUpdate, DbDelete without side effects.
 */
class AndJson
{
    /**
     * Initialize JSON AND condition
     *
     * @param QueryJsonCondition $queryJsonCondition The JSON condition builder
     * @param string $field The JSON field name
     * @param string|null $openBracket Opening brackets
     * @return DbQueryJsonConditionBuilderInterface
     */
    public function __invoke(
        QueryJsonCondition $queryJsonCondition,
        string $field,
        ?string $openBracket
    ): DbQueryJsonConditionBuilderInterface {
        $queryJsonCondition->initCondition($field, ' AND ' . $openBracket);

        return $queryJsonCondition;
    }
}
