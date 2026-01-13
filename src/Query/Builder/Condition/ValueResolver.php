<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Query\Builder\Condition;

use JardisCore\DbQuery\Data\QueryConditionCollector;
use JardisPsr\DbQuery\DbDeleteBuilderInterface;
use JardisPsr\DbQuery\DbQueryBuilderInterface;
use JardisPsr\DbQuery\DbUpdateBuilderInterface;
use JardisPsr\DbQuery\ExpressionInterface;

/**
 * Resolves values for database queries
 *
 * Supports:
 * - ExpressionInterface - raw SQL expressions (validated and inserted directly)
 * - Normal values - converted to parameter bindings
 * - Arrays - converted to a list of parameter bindings
 * - Subqueries - handled as QueryBuilderInterface
 */
class ValueResolver
{
    /**
     * @param QueryConditionCollector $collector
     * @param mixed $value
     * @param callable $validator Validation function for raw SQL
     * @return string The formatted value as a parameterized string or raw SQL
     */
    public function __invoke(
        QueryConditionCollector $collector,
        mixed $value,
        callable $validator
    ): string {
        // Handle ExpressionInterface (raw SQL expressions)
        if ($value instanceof ExpressionInterface) {
            $sql = $value->toSql();
            $validator($sql);  // Security check
            return $sql;  // Return directly, NO binding!
        }

        if (
            $value instanceof DbQueryBuilderInterface ||
            $value instanceof DbUpdateBuilderInterface ||
            $value instanceof DbDeleteBuilderInterface
        ) {
            $collector->addBinding($value);  // Store subquery as binding
            return $collector->generateParamName();  // Will be replaced by SqlBuilder with actual subquery SQL
        }

        if (is_array($value)) {
            $placeholders = [];
            foreach ($value as $item) {
                $collector->addBinding($item);
                $placeholders[] = $collector->generateParamName();
            }
            return '(' . implode(', ', $placeholders) . ')';
        }

        $collector->addBinding($value);
        return $collector->generateParamName();
    }
}
