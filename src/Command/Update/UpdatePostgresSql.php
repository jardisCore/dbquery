<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Update;

use JardisCore\DbQuery\Data\Dialect;

/**
 * PostgresSQL UPDATE SQL Generator
 *
 * Requires PostgresSQL 8.4+
 * JSON functions require PostgresSQL 9.2+, JSONB functions require 9.4+
 *
 * Note: PostgresSQL supports FROM clause in UPDATE for joins,
 * but this implementation uses standard UPDATE syntax compatible
 * with the JoinBuilder pattern (UPDATE...JOIN is MySQL-specific).
 */
class UpdatePostgresSql extends UpdateSqlBuilder
{
    protected string $dialect = Dialect::PostgreSQL->value;

    /**
     * Quote identifier with double quotes (PostgresSQL standard)
     *
     * @param string $identifier
     * @return string
     */
    public function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    /**
     * PostgresSQL uses TRUE/FALSE for booleans
     *
     * @param bool $value
     * @return string
     */
    protected function formatBoolean(bool $value): string
    {
        return $value ? 'TRUE' : 'FALSE';
    }

    /**
     * Builds JSON path extraction for PostgresSQL
     *
     * @param string $column The JSON column name
     * @param string $path The JSON path (e.g., '$.age' or 'age')
     * @return string The PostgresSQL JSON extract expression
     */
    protected function buildJsonExtract(string $column, string $path): string
    {
        $pgPath = $this->convertJsonPathToPostgres($path);

        if (strpos($pgPath, '.') !== false) {
            $parts = explode('.', $pgPath);
            $result = $this->quoteIdentifier($column);

            foreach ($parts as $index => $part) {
                $isLast = ($index === count($parts) - 1);
                $operator = $isLast ? '->>' : '->';
                $result .= $operator . "'" . $this->escapeString($part) . "'";
            }

            return $result;
        }

        return $this->quoteIdentifier($column) . "->>'" . $this->escapeString($pgPath) . "'";
    }

    /**
     * Builds JSON contains check for PostgresSQL
     *
     * @param string $column The JSON column name
     * @param string $value The value parameter (with ? placeholder)
     * @param string|null $path Optional JSON path
     * @return string The PostgresSQL JSON contains an expression
     */
    protected function buildJsonContains(string $column, string $value, ?string $path): string
    {
        if ($path !== null) {
            $pgPath = $this->convertJsonPathToPostgres($path);
            return $this->quoteIdentifier($column)
                . "->'" . $this->escapeString($pgPath) . "' @> "
                . "to_jsonb(" . $value . ")";
        }

        return $this->quoteIdentifier($column) . " @> to_jsonb(" . $value . ")";
    }

    /**
     * Builds negated JSON contains for PostgresSQL
     *
     * @param string $column The JSON column name
     * @param string $value The value parameter (with ? placeholder)
     * @param string|null $path Optional JSON path
     * @return string The PostgresSQL negated contains expression
     */
    protected function buildJsonNotContains(string $column, string $value, ?string $path): string
    {
        if ($path !== null) {
            $pgPath = $this->convertJsonPathToPostgres($path);
            return "NOT (" . $this->quoteIdentifier($column)
                . "->'" . $this->escapeString($pgPath) . "' @> "
                . "to_jsonb(" . $value . "))";
        }

        return "NOT (" . $this->quoteIdentifier($column) . " @> to_jsonb(" . $value . "))";
    }

    /**
     * Builds JSON length expression for PostgresSQL
     *
     * @param string $column The JSON column name
     * @param string|null $path Optional JSON path
     * @return string The PostgresSQL JSON length expression
     */
    protected function buildJsonLength(string $column, ?string $path): string
    {
        if ($path !== null) {
            $pgPath = $this->convertJsonPathToPostgres($path);
            return "jsonb_array_length("
                . $this->quoteIdentifier($column)
                . "->'" . $this->escapeString($pgPath) . "')";
        }

        return "jsonb_array_length(" . $this->quoteIdentifier($column) . ")";
    }

    /**
     * Converts JSON path notation ($.path.to.field) to PostgresSQL notation (path.to.field)
     *
     * @param string $jsonPath JSON path with $ prefix
     * @return string PostgresSQL-compatible path
     */
    private function convertJsonPathToPostgres(string $jsonPath): string
    {
        if (strpos($jsonPath, '$.') === 0) {
            return substr($jsonPath, 2);
        }

        if (strpos($jsonPath, '$') === 0) {
            return substr($jsonPath, 1);
        }

        return $jsonPath;
    }

    /**
     * PostgreSQL does not support ORDER BY in UPDATE statements
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildOrderBy(): string
    {
        return '';
    }

    /**
     * PostgreSQL does not support LIMIT in UPDATE statements
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildLimit(): string
    {
        return '';
    }

    /**
     * PostgreSQL does not support JOIN in UPDATE statements (with standard syntax)
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
