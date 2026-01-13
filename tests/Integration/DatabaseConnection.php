<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Tests\Integration;

use PDO;
use PDOException;

/**
 * Database Connection Helper for Integration Tests
 *
 * Provides PDO connections to all supported databases and helper methods
 * for table setup/teardown and test data management.
 */
class DatabaseConnection
{
    private static ?PDO $mysqlConnection = null;
    private static ?PDO $mariadbConnection = null;
    private static ?PDO $postgresConnection = null;
    private static ?PDO $sqliteConnection = null;

    /**
     * Get MySQL connection
     */
    public function getMysqlConnection(): PDO
    {
        if (self::$mysqlConnection === null) {
            $host = getenv('MYSQL_HOST') ?: 'mysql';
            $port = getenv('MYSQL_PORT') ?: '3306';
            $database = getenv('MYSQL_DATABASE') ?: 'test_db';
            $user = getenv('MYSQL_USER') ?: 'test_user';
            $password = getenv('MYSQL_PASSWORD') ?: 'test_password';

            $dsn = "mysql:host={$host};port={$port};dbname={$database};charset=utf8mb4";
            self::$mysqlConnection = new PDO($dsn, $user, $password, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]);
        }

        return self::$mysqlConnection;
    }

    /**
     * Get MariaDB connection
     */
    public function getMariaDbConnection(): PDO
    {
        if (self::$mariadbConnection === null) {
            $host = getenv('MARIADB_HOST') ?: 'mariadb';
            $port = getenv('MARIADB_PORT') ?: '3306';
            $database = getenv('MARIADB_DATABASE') ?: 'test_db';
            $user = getenv('MARIADB_USER') ?: 'test_user';
            $password = getenv('MARIADB_PASSWORD') ?: 'test_password';

            $dsn = "mysql:host={$host};port={$port};dbname={$database};charset=utf8mb4";
            self::$mariadbConnection = new PDO($dsn, $user, $password, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]);
        }

        return self::$mariadbConnection;
    }

    /**
     * Get PostgreSQL connection
     */
    public function getPostgresConnection(): PDO
    {
        if (self::$postgresConnection === null) {
            $host = getenv('POSTGRES_HOST') ?: 'postgres';
            $port = getenv('POSTGRES_PORT') ?: '5432';
            $database = getenv('POSTGRES_DATABASE') ?: 'test_db';
            $user = getenv('POSTGRES_USER') ?: 'test_user';
            $password = getenv('POSTGRES_PASSWORD') ?: 'test_password';

            $dsn = "pgsql:host={$host};port={$port};dbname={$database}";
            self::$postgresConnection = new PDO($dsn, $user, $password, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]);
        }

        return self::$postgresConnection;
    }

    /**
     * Get SQLite connection
     */
    public function getSqliteConnection(): PDO
    {
        if (self::$sqliteConnection === null) {
            self::$sqliteConnection = new PDO('sqlite::memory:', null, null, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]);
        }

        return self::$sqliteConnection;
    }

    /**
     * Create a test table for integration tests
     *
     * @param PDO $pdo Database connection
     * @param string $dbType Database type (mysql, mariadb, postgres, sqlite)
     * @param string $tableName Table name
     */
    public function createTestTable(PDO $pdo, string $dbType, string $tableName = 'users'): void
    {
        $this->dropTestTable($pdo, $tableName);

        $sql = match ($dbType) {
            'mysql', 'mariadb' => "CREATE TABLE `{$tableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(255) NOT NULL,
                `email` VARCHAR(255) NOT NULL,
                `status` VARCHAR(50) NOT NULL DEFAULT 'active',
                `age` INT NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `email_unique` (`email`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            'postgres' => "CREATE TABLE {$tableName} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                status VARCHAR(50) NOT NULL DEFAULT 'active',
                age INT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            'sqlite' => "CREATE TABLE {$tableName} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'active',
                age INTEGER NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
            default => throw new PDOException("Unsupported database type: {$dbType}")
        };

        $pdo->exec($sql);
    }

    /**
     * Create a secondary test table for JOIN tests
     *
     * @param PDO $pdo Database connection
     * @param string $dbType Database type
     * @param string $tableName Table name
     */
    public function createOrdersTable(PDO $pdo, string $dbType, string $tableName = 'orders'): void
    {
        $this->dropTestTable($pdo, $tableName);

        $sql = match ($dbType) {
            'mysql', 'mariadb' => "CREATE TABLE `{$tableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `user_id` INT NOT NULL,
                `product` VARCHAR(255) NOT NULL,
                `amount` DECIMAL(10,2) NOT NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            'postgres' => "CREATE TABLE {$tableName} (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                product VARCHAR(255) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            'sqlite' => "CREATE TABLE {$tableName} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                product TEXT NOT NULL,
                amount REAL NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
            default => throw new PDOException("Unsupported database type: {$dbType}")
        };

        $pdo->exec($sql);
    }

    /**
     * Drop a test table
     *
     * @param PDO $pdo Database connection
     * @param string $tableName Table name
     */
    public function dropTestTable(PDO $pdo, string $tableName): void
    {
        try {
            $pdo->exec("DROP TABLE IF EXISTS {$tableName}");
        } catch (PDOException $e) {
            // Ignore errors if table doesn't exist
        }
    }

    /**
     * Insert test data into a table
     *
     * @param PDO $pdo Database connection
     * @param string $tableName Table name
     * @param array<int, array<string, mixed>> $rows Array of rows to insert
     */
    public function insertTestData(PDO $pdo, string $tableName, array $rows): void
    {
        if (empty($rows)) {
            return;
        }

        $fields = array_keys($rows[0]);
        $placeholders = array_fill(0, count($fields), '?');

        $sql = sprintf(
            "INSERT INTO %s (%s) VALUES (%s)",
            $tableName,
            implode(', ', $fields),
            implode(', ', $placeholders)
        );

        $stmt = $pdo->prepare($sql);

        foreach ($rows as $row) {
            $stmt->execute(array_values($row));
        }
    }

    /**
     * Get all rows from a table
     *
     * @param PDO $pdo Database connection
     * @param string $tableName Table name
     * @return array<int, array<string, mixed>>
     */
    public function getAllRows(PDO $pdo, string $tableName): array
    {
        $stmt = $pdo->query("SELECT * FROM {$tableName}");
        return $stmt->fetchAll();
    }

    /**
     * Count rows in a table
     *
     * @param PDO $pdo Database connection
     * @param string $tableName Table name
     */
    public function countRows(PDO $pdo, string $tableName): int
    {
        $stmt = $pdo->query("SELECT COUNT(*) as count FROM {$tableName}");
        $result = $stmt->fetch();
        return (int)$result['count'];
    }

    /**
     * Truncate a table
     *
     * @param PDO $pdo Database connection
     * @param string $tableName Table name
     */
    public function truncateTable(PDO $pdo, string $tableName): void
    {
        try {
            $pdo->exec("TRUNCATE TABLE {$tableName}");
        } catch (PDOException $e) {
            // For SQLite, use DELETE instead
            $pdo->exec("DELETE FROM {$tableName}");
        }
    }
}
