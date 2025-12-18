<?php

declare(strict_types=1);

namespace JardisCore\DbQuery;

use InvalidArgumentException;
use JardisCore\DbQuery\Data\DeleteState;
use JardisCore\DbQuery\Data\QueryConditionCollector;
use JardisCore\DbQuery\Query\Condition\QueryCondition;
use JardisCore\DbQuery\Query\Condition\QueryJsonCondition;
use JardisCore\DbQuery\Query\Builder\Method;
use JardisCore\DbQuery\Factory\BuilderRegistry;
use JardisCore\DbQuery\Factory\SqlBuilderFactory;
use JardisCore\DbQuery\Query\Validator\QueryBracketValidator;
use JardisPsr\DbQuery\DbDeleteBuilderInterface;
use JardisPsr\DbQuery\DbPreparedQueryInterface;
use JardisPsr\DbQuery\DbQueryBuilderInterface;
use JardisPsr\DbQuery\DbQueryConditionBuilderInterface;
use JardisPsr\DbQuery\DbQueryJsonConditionBuilderInterface;
use JardisPsr\DbQuery\DbUpdateBuilderInterface;
use JardisPsr\DbQuery\ExpressionInterface;
use UnexpectedValueException;

/**
 * Provides methods for creating and managing SQL DELETE commands.
 *
 * This class implements DbDeleteBuilderInterface to facilitate
 * DELETE command building with a fluent interface.
 *
 * Supports:
 * - WHERE conditions (standard and JSON)
 * - JOINs (MySQL/MariaDB)
 * - ORDER BY and LIMIT (MySQL/MariaDB, SQLite 3.24.0+)
 */
class DbDelete implements DbDeleteBuilderInterface
{
    private DeleteState $state;
    private QueryConditionCollector $collector;
    private QueryCondition $queryCondition;
    private QueryJsonCondition $queryJsonCondition;

    public function __construct()
    {
        $this->state = new DeleteState();
        $this->collector = new QueryConditionCollector();
        $this->queryCondition = new QueryCondition($this, $this->collector);
        $this->queryJsonCondition = new QueryJsonCondition($this, $this->collector);
    }

    /**
     * Specifies the table to delete from
     *
     * @param string $container The table name
     * @param string|null $alias Optional alias for the table
     * @return self
     */
    public function from(string $container, ?string $alias = null): self
    {
        $this->state->setTable($container);
        $this->state->setAlias($alias);
        return $this;
    }

    /**
     * Starts a WHERE condition clause
     *
     * @param string|ExpressionInterface|null $field The column name or expression to compare
     * @param string|null $openBracket Optional opening bracket(s) for grouping conditions
     * @return DbQueryConditionBuilderInterface
     */
    public function where(
        string|ExpressionInterface|null $field = null,
        ?string $openBracket = null
    ): DbQueryConditionBuilderInterface {
        $resolvedField = BuilderRegistry::get(Method\ResolveField::class)($field);

        return BuilderRegistry::get(Method\Where::class)(
            $this->collector,
            $this->queryCondition,
            $resolvedField,
            $openBracket
        );
    }

    /**
     * Adds an AND condition to the WHERE clause
     *
     * @param string|ExpressionInterface|null $field The column name or expression to compare
     * @param string|null $openBracket Optional opening bracket(s) for grouping conditions
     * @return DbQueryConditionBuilderInterface
     */
    public function and(
        string|ExpressionInterface|null $field = null,
        ?string $openBracket = null
    ): DbQueryConditionBuilderInterface {
        $resolvedField = BuilderRegistry::get(Method\ResolveField::class)($field);

        return BuilderRegistry::get(Method\AndCondition::class)(
            $this->collector,
            $this->queryCondition,
            $resolvedField,
            $openBracket,
            false  // DbDelete does NOT support HAVING
        );
    }

    /**
     * Adds an OR condition to the WHERE clause
     *
     * @param string|ExpressionInterface|null $field The column name or expression to compare
     * @param string|null $openBracket Optional opening bracket(s) for grouping conditions
     * @return DbQueryConditionBuilderInterface
     */
    public function or(
        string|ExpressionInterface|null $field = null,
        ?string $openBracket = null
    ): DbQueryConditionBuilderInterface {
        $resolvedField = BuilderRegistry::get(Method\ResolveField::class)($field);

        return BuilderRegistry::get(Method\OrCondition::class)(
            $this->collector,
            $this->queryCondition,
            $resolvedField,
            $openBracket,
            false  // DbDelete does NOT support HAVING
        );
    }

    /**
     * Starts a JSON-specific WHERE condition
     *
     * @param string $field The JSON column name
     * @param string|null $openBracket Optional opening bracket(s)
     * @return DbQueryJsonConditionBuilderInterface
     */
    public function whereJson(string $field, ?string $openBracket = null): DbQueryJsonConditionBuilderInterface
    {
        return BuilderRegistry::get(Method\WhereJson::class)(
            $this->collector,
            $this->queryJsonCondition,
            $field,
            $openBracket
        );
    }

    /**
     * Starts a JSON-specific AND condition
     *
     * @param string $field The JSON column name
     * @param string|null $openBracket Optional opening bracket(s)
     * @return DbQueryJsonConditionBuilderInterface
     */
    public function andJson(string $field, ?string $openBracket = null): DbQueryJsonConditionBuilderInterface
    {
        return BuilderRegistry::get(Method\AndJson::class)(
            $this->queryJsonCondition,
            $field,
            $openBracket
        );
    }

    /**
     * Starts a JSON-specific OR condition
     *
     * @param string $field The JSON column name
     * @param string|null $openBracket Optional opening bracket(s)
     * @return DbQueryJsonConditionBuilderInterface
     */
    public function orJson(string $field, ?string $openBracket = null): DbQueryJsonConditionBuilderInterface
    {
        return BuilderRegistry::get(Method\OrJson::class)(
            $this->collector,
            $this->queryJsonCondition,
            $field,
            $openBracket
        );
    }

    /**
     * Adds an EXISTS condition to the query.
     *
     * @param DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface $query
     * @param string|null $closeBracket Optional closing bracket for the condition
     * @return DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface
     */
    public function exists(
        DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface $query,
        ?string $closeBracket = null
    ): DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface {
        return BuilderRegistry::get(Method\Exists::class)(
            $this->collector,
            $this,
            $query,
            $closeBracket
        );
    }

    /**
     * Adds a "NOT EXISTS" condition to the query builder.
     *
     * @param DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface $query
     * @param string|null $closeBracket Optional closing bracket for the condition.
     * @return DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface
     */
    public function notExists(
        DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface $query,
        ?string $closeBracket = null
    ): DbQueryBuilderInterface|DbUpdateBuilderInterface|DbDeleteBuilderInterface {
        return BuilderRegistry::get(Method\NotExists::class)(
            $this->collector,
            $this,
            $query,
            $closeBracket
        );
    }

    /**
     * Adds an INNER JOIN clause
     *
     * @param string|DbQueryBuilderInterface $container The table name or subquery to join
     * @param string $constraint The join condition
     * @param string|null $alias Optional alias for the joined table
     * @return self
     */
    public function innerJoin(
        string|DbQueryBuilderInterface $container,
        string $constraint,
        ?string $alias = null
    ): self {
        return BuilderRegistry::get(Method\InnerJoin::class)(
            $this->state,
            $this,
            $container,
            $constraint,
            $alias
        );
    }

    /**
     * Adds a LEFT JOIN clause
     *
     * @param string|DbQueryBuilderInterface $container The table name or subquery to join
     * @param string $constraint The join condition
     * @param string|null $alias Optional alias for the joined table
     * @return self
     */
    public function leftJoin(
        string|DbQueryBuilderInterface $container,
        string $constraint,
        ?string $alias = null
    ): self {
        return BuilderRegistry::get(Method\LeftJoin::class)(
            $this->state,
            $this,
            $container,
            $constraint,
            $alias
        );
    }

    /**
     * Specifies the column and direction for sorting results
     *
     * @param string $field The column name to sort by
     * @param string $direction The sort direction: 'ASC' or 'DESC'
     * @return self
     */
    public function orderBy(string $field, string $direction = 'ASC'): self
    {
        return BuilderRegistry::get(Method\OrderBy::class)(
            $this->state,
            $this,
            $field,
            $direction
        );
    }

    /**
     * Limits the number of rows affected
     *
     * @param int|null $limit Maximum number of rows to affect
     * @param int|null $offset Ignored in DELETE (not supported)
     * @return self
     */
    public function limit(?int $limit = null, ?int $offset = null): self
    {
        return BuilderRegistry::get(Method\Limit::class)(
            $this->state,
            $this,
            $limit,
            $offset
        );
    }

    /**
     * Generate DELETE SQL
     *
     * @param string $dialect The SQL dialect (mysql, postgres, sqlite)
     * @param bool $prepared Whether to generate a prepared statement (default: true)
     * @param string|null $version Database version (e.g., '5.7', '8.0'). Uses default if null.
     * @return string|DbPreparedQueryInterface
     * @throws InvalidArgumentException If validation fails
     */
    public function sql(
        string $dialect,
        bool $prepared = true,
        ?string $version = null
    ): string|DbPreparedQueryInterface {
        // Validate state
        if (empty($this->state->getTable())) {
            throw new InvalidArgumentException('Table name must be specified with from()');
        }

        $validator = new QueryBracketValidator();
        if (
            !$validator->hasValidBrackets(
                $this->collector->whereConditions(),
                $this->collector->havingConditions()
            )
        ) {
            throw new UnexpectedValueException('Invalid brackets in query');
        }

        $sqlBuilder = SqlBuilderFactory::createDelete($dialect, $version);

        return $sqlBuilder(
            $this->state,
            $this->collector,
            $prepared
        );
    }
}
