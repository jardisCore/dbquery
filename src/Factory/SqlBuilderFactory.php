<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Factory;

use InvalidArgumentException;
use JardisCore\DbQuery\Command\Delete\DeleteMySql;
use JardisCore\DbQuery\Command\Delete\DeletePostgresSql;
use JardisCore\DbQuery\Command\Delete\DeleteSqlBuilder;
use JardisCore\DbQuery\Command\Delete\DeleteSqliteSql;
use JardisCore\DbQuery\Command\Insert\InsertMySql;
use JardisCore\DbQuery\Command\Insert\InsertPostgresSql;
use JardisCore\DbQuery\Command\Insert\InsertSqlBuilder;
use JardisCore\DbQuery\Command\Insert\InsertSqliteSql;
use JardisCore\DbQuery\Command\Update\UpdateMySql;
use JardisCore\DbQuery\Command\Update\UpdatePostgresSql;
use JardisCore\DbQuery\Command\Update\UpdateSqlBuilder;
use JardisCore\DbQuery\Command\Update\UpdateSqliteSql;
use JardisCore\DbQuery\Data\Dialect;
use JardisCore\DbQuery\Query\MySql;
use JardisCore\DbQuery\Query\PostgresSql;
use JardisCore\DbQuery\Query\SqlBuilder;
use JardisCore\DbQuery\Query\SqliteSql;

/**
 * Factory for creating SQL builders based on database dialect
 *
 * Provides methods for creating SELECT, INSERT, UPDATE, and DELETE SQL builders
 * for different database dialects (MySQL/MariaDB, PostgreSQL, SQLite).
 */
class SqlBuilderFactory
{
    /**
     * Create a SELECT SQL builder for the specified dialect
     *
     * @param string $dialect Database dialect: 'mysql', 'mariadb', 'postgres', 'sqlite'
     * @param string|null $version Database version (e.g., '8.0', '14'). Uses default if null.
     * @return SqlBuilder
     * @throws InvalidArgumentException If dialect is not supported
     */
    public static function createSelect(string $dialect, ?string $version = null): SqlBuilder
    {
        $dialectEnum = self::parseDialect($dialect);
        self::setBuilderContext($dialectEnum, $version);

        return match ($dialectEnum) {
            Dialect::PostgreSQL => new PostgresSql(),
            Dialect::SQLite => new SqliteSql(),
            Dialect::MySQL, Dialect::MariaDB => new MySql(),
        };
    }

    /**
     * Create an INSERT SQL builder for the specified dialect
     *
     * @param string $dialect Database dialect: 'mysql', 'mariadb', 'postgres', 'sqlite'
     * @param string|null $version Database version (e.g., '8.0', '14'). Uses default if null.
     * @return InsertSqlBuilder
     * @throws InvalidArgumentException If dialect is not supported
     */
    public static function createInsert(string $dialect, ?string $version = null): InsertSqlBuilder
    {
        $dialectEnum = self::parseDialect($dialect);
        self::setBuilderContext($dialectEnum, $version);

        return match ($dialectEnum) {
            Dialect::PostgreSQL => new InsertPostgresSql(),
            Dialect::SQLite => new InsertSqliteSql(),
            Dialect::MySQL, Dialect::MariaDB => new InsertMySql(),
        };
    }

    /**
     * Create an UPDATE SQL builder for the specified dialect
     *
     * @param string $dialect Database dialect: 'mysql', 'mariadb', 'postgres', 'sqlite'
     * @param string|null $version Database version (e.g., '8.0', '14'). Uses default if null.
     * @return UpdateSqlBuilder
     * @throws InvalidArgumentException If dialect is not supported
     */
    public static function createUpdate(string $dialect, ?string $version = null): UpdateSqlBuilder
    {
        $dialectEnum = self::parseDialect($dialect);
        self::setBuilderContext($dialectEnum, $version);

        return match ($dialectEnum) {
            Dialect::PostgreSQL => new UpdatePostgresSql(),
            Dialect::SQLite => new UpdateSqliteSql(),
            Dialect::MySQL, Dialect::MariaDB => new UpdateMySql(),
        };
    }

    /**
     * Create a DELETE SQL builder for the specified dialect
     *
     * @param string $dialect Database dialect: 'mysql', 'mariadb', 'postgres', 'sqlite'
     * @param string|null $version Database version (e.g., '8.0', '14'). Uses default if null.
     * @return DeleteSqlBuilder
     * @throws InvalidArgumentException If dialect is not supported
     */
    public static function createDelete(string $dialect, ?string $version = null): DeleteSqlBuilder
    {
        $dialectEnum = self::parseDialect($dialect);
        self::setBuilderContext($dialectEnum, $version);

        return match ($dialectEnum) {
            Dialect::PostgreSQL => new DeletePostgresSql(),
            Dialect::SQLite => new DeleteSqliteSql(),
            Dialect::MySQL, Dialect::MariaDB => new DeleteMySql(),
        };
    }

    /**
     * Parse dialect string to Dialect enum
     *
     * @param string $dialect Database dialect string
     * @return Dialect The parsed dialect enum
     * @throws InvalidArgumentException If dialect is not supported
     */
    private static function parseDialect(string $dialect): Dialect
    {
        return Dialect::tryFromString($dialect)
            ?? throw new InvalidArgumentException("Unsupported dialect: {$dialect}");
    }

    /**
     * Set BuilderRegistry context for version-aware builder resolution
     *
     * @param Dialect $dialect Database dialect enum
     * @param string|null $version Database version, uses default if null
     */
    private static function setBuilderContext(Dialect $dialect, ?string $version): void
    {
        $resolvedVersion = $version ?? $dialect->defaultVersion();
        BuilderRegistry::setContext($dialect->value, $resolvedVersion);
    }
}
