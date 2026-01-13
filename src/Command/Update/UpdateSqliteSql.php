<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Update;

use JardisCore\DbQuery\Data\Dialect;
use JardisCore\DbQuery\Factory\BuilderRegistry;

/**
 * SQLite UPDATE SQL Generator
 *
 * Requires SQLite 3.8.3+ for CTE support
 * JSON functions require SQLite 3.38.0+ (2022-02-22)
 *
 * Note: SQLite has limited JOIN support in UPDATE (FROM clause since 3.33.0)
 */
class UpdateSqliteSql extends UpdateSqlBuilder
{
    protected string $dialect = Dialect::SQLite->value;

    /**
     * Quote identifier with backticks for SQLite
     *
     * @param string $identifier
     * @return string
     */
    protected function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    /**
     * SQLite does not support FULL OUTER JOIN
     *
     * @param string $joinType
     * @return bool
     */
    protected function shouldSkipJoinType(string $joinType): bool
    {
        return $joinType === 'FULL JOIN' || $joinType === 'FULL OUTER JOIN';
    }

    /**
     * Builds JSON_EXTRACT expression for SQLite
     *
     * @param string $column The JSON column name
     * @param string $path The JSON path (e.g., '$.age')
     * @return string The SQLite JSON extract expression
     */
    protected function buildJsonExtract(string $column, string $path): string
    {
        return "json_extract("
            . $this->quoteIdentifier($column)
            . ", '" . $this->escapeString($path) . "')";
    }

    /**
     * Builds JSON contains check for SQLite
     *
     * @param string $column The JSON column name
     * @param string $value The value parameter (with ? placeholder)
     * @param string|null $path Optional JSON path
     * @return string The SQLite JSON contains expression
     */
    protected function buildJsonContains(string $column, string $value, ?string $path): string
    {
        if ($path !== null) {
            return "EXISTS (SELECT 1 FROM json_each("
                . "json_extract(" . $this->quoteIdentifier($column)
                . ", '" . $this->escapeString($path) . "')"
                . ") WHERE value = " . $value . ")";
        }

        return "EXISTS (SELECT 1 FROM json_each("
            . $this->quoteIdentifier($column)
            . ") WHERE value = " . $value . ")";
    }

    /**
     * Builds negated JSON contains for SQLite
     *
     * @param string $column The JSON column name
     * @param string $value The value parameter (with ? placeholder)
     * @param string|null $path Optional JSON path
     * @return string The SQLite negated contains expression
     */
    protected function buildJsonNotContains(string $column, string $value, ?string $path): string
    {
        if ($path !== null) {
            return "NOT EXISTS (SELECT 1 FROM json_each("
                . "json_extract(" . $this->quoteIdentifier($column)
                . ", '" . $this->escapeString($path) . "')"
                . ") WHERE value = " . $value . ")";
        }

        return "NOT EXISTS (SELECT 1 FROM json_each("
            . $this->quoteIdentifier($column)
            . ") WHERE value = " . $value . ")";
    }

    /**
     * Builds JSON length expression for SQLite
     *
     * @param string $column The JSON column name
     * @param string|null $path Optional JSON path
     * @return string The SQLite JSON length expression
     */
    protected function buildJsonLength(string $column, ?string $path): string
    {
        if ($path !== null) {
            return "json_array_length("
                . "json_extract(" . $this->quoteIdentifier($column)
                . ", '" . $this->escapeString($path) . "')"
                . ")";
        }

        return "json_array_length(" . $this->quoteIdentifier($column) . ")";
    }

    /**
     * SQLite does not support ORDER BY in UPDATE statements (without SQLITE_ENABLE_UPDATE_DELETE_LIMIT)
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildOrderBy(): string
    {
        return '';
    }

    /**
     * SQLite does not support LIMIT in UPDATE statements (without SQLITE_ENABLE_UPDATE_DELETE_LIMIT)
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildLimit(): string
    {
        return '';
    }

    /**
     * SQLite does not support JOIN in UPDATE statements (with standard syntax)
     * Override parent method to return empty string
     *
     * @param bool $prepared
     * @return string
     */
    protected function buildJoins(bool $prepared): string
    {
        return '';
    }
}
