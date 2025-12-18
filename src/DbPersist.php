<?php

declare(strict_types=1);

namespace JardisCore\DbQuery;

use InvalidArgumentException;
use JardisPsr\DbQuery\DbPersistInterface;
use JardisPsr\DbQuery\DbPreparedQueryInterface;

/**
 * Provides shortcut methods for simple CRUD operations based on primary keys.
 *
 * This class offers a simplified interface for common database operations
 * when working with single records identified by their primary key.
 * Internally uses DbInsert, DbUpdate, and DbDelete builders with prepared statements.
 *
 * Usage:
 * - insert(): Create INSERT query with auto-increment primary key handling
 * - update(): Create UPDATE query with WHERE condition on primary key
 * - delete(): Create DELETE query with WHERE condition on primary key
 */
class DbPersist implements DbPersistInterface
{
    /**
     * Creates an INSERT query with automatic primary key handling
     *
     * @param string $table The table name
     * @param array<string, mixed> $data Associative array of column => value pairs
     * @param string $primaryKey The name of the primary key column
     * @param bool $autoIncrement Whether the primary key is auto-increment (removes it from INSERT)
     * @param string $dialect The SQL dialect (mysql, postgres, sqlite)
     * @param string|null $version Database version (e.g., '8.0'). Uses default if null.
     * @return DbPreparedQueryInterface The prepared query with SQL and bindings
     * @throws InvalidArgumentException If data is empty or invalid
     */
    public function insert(
        string $table,
        array $data,
        string $primaryKey,
        bool $autoIncrement = true,
        string $dialect = 'mysql',
        ?string $version = null
    ): DbPreparedQueryInterface {
        if (empty($data)) {
            throw new InvalidArgumentException('Data array cannot be empty for INSERT operation');
        }

        if (empty($table)) {
            throw new InvalidArgumentException('Table name cannot be empty');
        }

        // Remove primary key from data if it's auto-increment
        if ($autoIncrement && isset($data[$primaryKey])) {
            unset($data[$primaryKey]);
        }

        // Ensure we still have data after potential primary key removal
        if (empty($data)) {
            throw new InvalidArgumentException(
                'Data array is empty after removing auto-increment primary key. ' .
                'Provide at least one other column.'
            );
        }

        $insert = new DbInsert();
        $insert->into($table)->set($data);

        $result = $insert->sql($dialect, true, $version);
        assert($result instanceof DbPreparedQueryInterface);

        return $result;
    }

    /**
     * Creates an UPDATE query with WHERE condition on primary key
     *
     * @param string $table The table name
     * @param array<string, mixed> $data Associative array of column => value pairs to update
     * @param string $primaryKey The name of the primary key column
     * @param mixed $primaryValue The primary key value for the WHERE condition
     * @param string $dialect The SQL dialect (mysql, postgres, sqlite)
     * @param string|null $version Database version (e.g., '8.0'). Uses default if null.
     * @return DbPreparedQueryInterface The prepared query with SQL and bindings
     * @throws InvalidArgumentException If data is empty, primary value is invalid, or primary key is in data
     */
    public function update(
        string $table,
        array $data,
        string $primaryKey,
        mixed $primaryValue,
        string $dialect = 'mysql',
        ?string $version = null
    ): DbPreparedQueryInterface {
        if (empty($data)) {
            throw new InvalidArgumentException('Data array cannot be empty for UPDATE operation');
        }

        if (empty($table)) {
            throw new InvalidArgumentException('Table name cannot be empty');
        }

        if ($primaryValue === null || $primaryValue === '') {
            throw new InvalidArgumentException('Primary key value cannot be null or empty');
        }

        // Ensure primary key is not in the update data
        if (isset($data[$primaryKey])) {
            throw new InvalidArgumentException(
                sprintf(
                    'Cannot update primary key "%s". Remove it from data array.',
                    $primaryKey
                )
            );
        }

        $update = new DbUpdate();
        $update->table($table)
            ->setMultiple($data)
            ->where($primaryKey)->equals($primaryValue);

        $result = $update->sql($dialect, true, $version);
        assert($result instanceof DbPreparedQueryInterface);

        return $result;
    }

    /**
     * Creates a DELETE query with WHERE condition on primary key
     *
     * @param string $table The table name
     * @param string $primaryKey The name of the primary key column
     * @param mixed $primaryValue The primary key value for the WHERE condition
     * @param string $dialect The SQL dialect (mysql, postgres, sqlite)
     * @param string|null $version Database version (e.g., '8.0'). Uses default if null.
     * @return DbPreparedQueryInterface The prepared query with SQL and bindings
     * @throws InvalidArgumentException If primary value is invalid
     */
    public function delete(
        string $table,
        string $primaryKey,
        mixed $primaryValue,
        string $dialect = 'mysql',
        ?string $version = null
    ): DbPreparedQueryInterface {
        if (empty($table)) {
            throw new InvalidArgumentException('Table name cannot be empty');
        }

        if ($primaryValue === null || $primaryValue === '') {
            throw new InvalidArgumentException('Primary key value cannot be null or empty');
        }

        $delete = new DbDelete();
        $delete->from($table)
            ->where($primaryKey)->equals($primaryValue);

        $result = $delete->sql($dialect, true, $version);
        assert($result instanceof DbPreparedQueryInterface);

        return $result;
    }
}
