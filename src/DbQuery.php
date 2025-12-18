<?php

declare(strict_types=1);

namespace JardisCore\DbQuery;

use JardisCore\DbQuery\Data\QueryConditionCollector;
use JardisCore\DbQuery\Data\QueryState;
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
use JardisPsr\DbQuery\DbWindowBuilderInterface;
use JardisPsr\DbQuery\ExpressionInterface;
use UnexpectedValueException;

/**
 * Provides methods for creating and managing SQL queries,
 * including support for SELECT, JOIN, WHERE, GROUP BY, and more.
 *
 * This class implements the DbQueryBuilderInterface to facilitate
 * query building with a fluent interface for dynamic query construction.
 */
class DbQuery implements DbQueryBuilderInterface
{
    private QueryConditionCollector $collector;
    private QueryCondition $queryCondition;
    private QueryJsonCondition $queryJsonCondition;
    private QueryState $state;

    public function __construct()
    {
        $this->state = new QueryState();
        $this->collector = new QueryConditionCollector();
        $this->queryCondition = new QueryCondition($this, $this->collector);
        $this->queryJsonCondition = new QueryJsonCondition($this, $this->collector);
    }

    public function with(string $name, DbQueryBuilderInterface $query): DbQueryBuilderInterface
    {
        $this->state->addCte($name, $query);
        return $this;
    }

    public function withRecursive(string $name, DbQueryBuilderInterface $query): DbQueryBuilderInterface
    {
        $this->state->addCteRecursive($name, $query);
        return $this;
    }

    public function select(string $fields = "*"): self
    {
        $this->state->setFields(empty($fields) ? '*' : $fields);
        return $this;
    }

    public function selectSubquery(DbQueryBuilderInterface $query, string $alias): DbQueryBuilderInterface
    {
        $this->state->addSelectSubquery($alias, $query);
        return $this;
    }

    public function distinct(bool $isDistinctQuery = false): self
    {
        $this->state->setDistinct($isDistinctQuery);
        return $this;
    }

    public function from(string|DbQueryBuilderInterface $container, ?string $alias = null): self
    {
        $this->state->setContainer($container);
        $this->state->setAlias($alias);
        return $this;
    }

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
            true  // DbQuery supports HAVING
        );
    }

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
            true  // DbQuery supports HAVING
        );
    }

    /**
     * Starts a JSON-specific WHERE condition
     *
     * @param string $field The JSON column name (without a path)
     * @param string|null $openBracket Optional opening bracket(s)
     * @return DbQueryJsonConditionBuilderInterface For JSON-specific operations
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
     * @param string $field The JSON column name (without a path)
     * @param string|null $openBracket Optional opening bracket(s)
     * @return DbQueryJsonConditionBuilderInterface For JSON-specific operations
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
     * @param string $field The JSON column name (without a path)
     * @param string|null $openBracket Optional opening bracket(s)
     * @return DbQueryJsonConditionBuilderInterface For JSON-specific operations
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

    public function rightJoin(
        string|DbQueryBuilderInterface $container,
        string $constraint,
        ?string $alias = null
    ): self {
        return BuilderRegistry::get(Method\RightJoin::class)(
            $this->state,
            $this,
            $container,
            $constraint,
            $alias
        );
    }

    public function fullJoin(
        string|DbQueryBuilderInterface $container,
        string $constraint,
        ?string $alias = null
    ): self {
        return BuilderRegistry::get(Method\FullJoin::class)(
            $this->state,
            $this,
            $container,
            $constraint,
            $alias
        );
    }

    public function crossJoin(string|DbQueryBuilderInterface $container, ?string $alias = null): self
    {
        return BuilderRegistry::get(Method\CrossJoin::class)(
            $this->state,
            $this,
            $container,
            $alias
        );
    }

    public function union(DbQueryBuilderInterface $query): DbQueryBuilderInterface
    {
        $this->state->addUnion($query);
        return $this;
    }

    public function unionAll(DbQueryBuilderInterface $query): DbQueryBuilderInterface
    {
        $this->state->addUnionAll($query);
        return $this;
    }

    public function groupBy(string ...$columns): self
    {
        foreach ($columns as $column) {
            $this->state->addGroupBy($column);
        }
        return $this;
    }

    public function having(string $expression, ?string $openBracket = null): DbQueryConditionBuilderInterface
    {
        $this->queryCondition->initCondition($openBracket . $expression, true);

        return $this->queryCondition;
    }

    /**
     * Starts a JSON-specific HAVING condition
     *
     * @param string $field The JSON column name (without a path)
     * @param string|null $openBracket Optional opening bracket(s)
     * @return DbQueryJsonConditionBuilderInterface For JSON-specific operations
     */
    public function havingJson(string $field, ?string $openBracket = null): DbQueryJsonConditionBuilderInterface
    {
        $this->queryJsonCondition->initCondition($field, $openBracket ?? '', true);

        return $this->queryJsonCondition;
    }

    public function orderBy(string $field, string $direction = 'ASC'): self
    {
        return BuilderRegistry::get(Method\OrderBy::class)(
            $this->state,
            $this,
            $field,
            $direction
        );
    }

    public function limit(int $limit = null, int $offset = null): self
    {
        return BuilderRegistry::get(Method\Limit::class)(
            $this->state,
            $this,
            $limit,
            $offset
        );
    }

    public function selectWindow(string $function, string $alias, ?string $args = null): DbWindowBuilderInterface
    {
        return BuilderRegistry::get(Method\SelectWindow::class)(
            $this->state,
            $this,
            $function,
            $alias,
            $args
        );
    }

    public function window(string $name): DbWindowBuilderInterface
    {
        return BuilderRegistry::get(Method\Window::class)(
            $this->state,
            $this,
            $name
        );
    }

    public function selectWindowRef(string $function, string $windowName, string $alias, ?string $args = null): self
    {
        BuilderRegistry::get(Method\SelectWindowRef::class)(
            $this->state,
            $this,
            $function,
            $windowName,
            $alias,
            $args
        );

        return $this;
    }

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
     * Generates and returns the SQL query string or prepared query interface based on the given dialect.
     *
     * @param string $dialect The SQL dialect to be used for query generation (e.g., MySQL, PostgresSQL).
     * @param bool $prepared Whether to generate a prepared SQL query with bound parameters. Defaults to true.
     * @param string|null $version Database version (e.g., '5.7', '8.0'). Uses default if null.
     * @return string|DbPreparedQueryInterface Returns SQL query string or DbPreparedQueryInterface if prepared is true.
     * @throws UnexpectedValueException Thrown if the query contains invalid bracket structures.
     */
    public function sql(
        string $dialect,
        bool $prepared = true,
        ?string $version = null
    ): string|DbPreparedQueryInterface {
        $sqlBuilder = SqlBuilderFactory::createSelect($dialect, $version);

        // Validate bracket balance across all query parts
        $validator = new QueryBracketValidator();
        if (
            !$validator->hasValidBrackets(
                $this->collector->whereConditions(),
                $this->collector->havingConditions()
            )
        ) {
            throw new UnexpectedValueException('Invalid brackets in query');
        }

        return $sqlBuilder(
            $this->state,
            $this->collector,
            $prepared
        );
    }
}
