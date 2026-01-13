<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Delete;

use JardisCore\DbQuery\Data\Dialect;

/**
 * SQLite DELETE it SQL Generator
 *
 * SQLite DELETE supports WHERE conditions and limited JOIN support.
 * ORDER BY and LIMIT are supported in SQLite 3.24.0+.
 * JSON functions require SQLite 3.38.0+ with JSON1 extension.
 */
class DeleteSqliteSql extends DeleteSqlBuilder
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
     * SQLite does not support FULL OUTER JOIN or RIGHT JOIN
     *
     * @param string $joinType
     * @return bool
     */
    protected function shouldSkipJoinType(string $joinType): bool
    {
        return in_array($joinType, ['FULL JOIN', 'FULL OUTER JOIN', 'RIGHT JOIN'], true);
    }

    /**
     * Builds JSON extract expression for SQLite using json_extract
     *
     * @param string $column The JSON column name
     * @param string $path The JSON path (e.g., '$.age')
     * @return string The SQLite JSON extract expression
     */
    protected function buildJsonExtract(string $column, string $path): string
    {
        return "json_extract(" . $this->quoteIdentifier($column) . ", '" . $this->escapeString($path) . "')";
    }

    /**
     * Builds JSON contains an expression for SQLite
     * SQLite doesn't have a native JSON_CONTAINS, so we simulate it
     *
     * @param string $column The JSON column name
     * @param string $value The value parameter (with ? placeholder)
     * @param string|null $path Optional JSON path
     * @return string The SQLite JSON contains expression
     */
    protected function buildJsonContains(string $column, string $value, ?string $path): string
    {
        if ($path !== null) {
            // Extract the path and check if it contains the value
            return "json_extract(" . $this->quoteIdentifier($column) . ", '"
                . $this->escapeString($path) . "') LIKE '%' || " . $value . " || '%'";
        }

        return $this->quoteIdentifier($column) . " LIKE '%' || " . $value . " || '%'";
    }

    /**
     * Builds JSON length expression for SQLite using json_array_length
     *
     * @param string $column The JSON column name
     * @param string|null $path Optional JSON path
     * @return string The SQLite JSON length expression
     */
    protected function buildJsonLength(string $column, ?string $path): string
    {
        if ($path !== null) {
            return "json_array_length(" . $this->quoteIdentifier($column) . ", '" . $this->escapeString($path) . "')";
        }

        return "json_array_length(" . $this->quoteIdentifier($column) . ")";
    }

    /**
     * SQLite does not support ORDER BY in DELETE statements (without SQLITE_ENABLE_UPDATE_DELETE_LIMIT)
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildOrderBy(): string
    {
        return '';
    }

    /**
     * SQLite does not support LIMIT in DELETE statements (without SQLITE_ENABLE_UPDATE_DELETE_LIMIT)
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildLimit(): string
    {
        return '';
    }

    /**
     * SQLite does not support JOIN in DELETE statements (with standard syntax)
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
