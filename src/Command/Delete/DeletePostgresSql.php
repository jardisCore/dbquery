<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Delete;

use JardisCore\DbQuery\Data\Dialect;

/**
 * PostgresSQL DELETE SQL Generator
 *
 * PostgresSQL DELETE supports WHERE conditions but not JOINs, ORDER BY, or LIMIT.
 * For complex multi-table deletes, use DELETE with WHERE EXISTS subquery.
 * JSON operations use native PostgresSQL JSON operators (->>, ->, etc.).
 */
class DeletePostgresSql extends DeleteSqlBuilder
{
    protected string $dialect = Dialect::PostgreSQL->value;

    /**
     * Quote identifier with double quotes for PostgresSQL
     *
     * @param string $identifier
     * @return string
     */
    protected function quoteIdentifier(string $identifier): string
    {
        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    /**
     * Builds JSON extract expression for PostgresSQL using ->> operator
     *
     * @param string $column The JSON column name
     * @param string $path The JSON path (e.g., '$.age' or 'age')
     * @return string The PostgresSQL JSON extract expression
     */
    protected function buildJsonExtract(string $column, string $path): string
    {
        // Convert MySQL-style $.path to PostgresSQL style
        $pgPath = str_starts_with($path, '$.') ? substr($path, 2) : $path;

        // Handle nested paths: $.user.name -> 'user','name'
        $parts = explode('.', $pgPath);

        if (count($parts) === 1) {
            // Simple path: column->>'field'
            return $this->quoteIdentifier($column) . "->>'" . $this->escapeString($parts[0]) . "'";
        }

        // Nested path: column->'user'->>'name'
        $result = $this->quoteIdentifier($column);
        $lastIndex = count($parts) - 1;

        foreach ($parts as $index => $part) {
            if ($index === $lastIndex) {
                $result .= "->>'" . $this->escapeString($part) . "'";
            } else {
                $result .= "->'" . $this->escapeString($part) . "'";
            }
        }

        return $result;
    }

    /**
     * Builds JSON contains an expression for PostgresSQL using @> operator
     *
     * @param string $column The JSON column name
     * @param string $value The value parameter (with ? placeholder)
     * @param string|null $path Optional JSON path
     * @return string The PostgresSQL JSON contains an expression
     */
    protected function buildJsonContains(string $column, string $value, ?string $path): string
    {
        if ($path !== null) {
            // For path-specific contents, extract the path first then check containment
            $pgPath = str_starts_with($path, '$.') ? substr($path, 2) : $path;
            return $this->quoteIdentifier($column) . "->'" . $this->escapeString($pgPath)
                . "' @> " . $value . "::jsonb";
        }

        return $this->quoteIdentifier($column) . " @> " . $value . "::jsonb";
    }

    /**
     * Builds JSON length expression for PostgresSQL using jsonb_array_length
     *
     * @param string $column The JSON column name
     * @param string|null $path Optional JSON path
     * @return string The PostgresSQL JSON length expression
     */
    protected function buildJsonLength(string $column, ?string $path): string
    {
        if ($path !== null) {
            $pgPath = str_starts_with($path, '$.') ? substr($path, 2) : $path;
            return "jsonb_array_length(" . $this->quoteIdentifier($column) . "->'"
                . $this->escapeString($pgPath) . "')";
        }

        return "jsonb_array_length(" . $this->quoteIdentifier($column) . ")";
    }

    /**
     * PostgreSQL does not support ORDER BY in DELETE statements
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildOrderBy(): string
    {
        return '';
    }

    /**
     * PostgreSQL does not support LIMIT in DELETE statements
     * Override parent method to return empty string
     *
     * @return string
     */
    protected function buildLimit(): string
    {
        return '';
    }

    /**
     * PostgreSQL does not support JOIN in DELETE statements (with standard syntax)
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
