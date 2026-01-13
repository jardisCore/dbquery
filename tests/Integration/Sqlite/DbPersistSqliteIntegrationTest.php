<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Tests\Integration\Sqlite;

use JardisCore\DbQuery\DbPersist;
use JardisCore\DbQuery\Tests\Integration\DatabaseConnection;
use PDO;
use PHPUnit\Framework\TestCase;

/**
 * SQLite Integration Tests for DbPersist
 *
 * Tests actual INSERT/UPDATE/DELETE query execution against SQLite database
 */
class DbPersistSqliteIntegrationTest extends TestCase
{
    private DatabaseConnection $db;
    private PDO $connection;
    private DbPersist $persist;

    protected function setUp(): void
    {
        $this->db = new DatabaseConnection();
        $this->connection = $this->db->getSqliteConnection();
        $this->db->createTestTable($this->connection, 'sqlite', 'users');
        $this->persist = new DbPersist();
    }

    protected function tearDown(): void
    {
        $this->db->dropTestTable($this->connection, 'users');
    }

    // ==================== INSERT Tests ====================

    public function testInsertWithAutoIncrementPrimaryKey(): void
    {
        $prepared = $this->persist->insert(
            'users',
            ['name' => 'John Doe', 'email' => 'john@example.com', 'age' => 30],
            'id',
            true,
            'sqlite'
        );

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        $this->assertEquals(1, $this->db->countRows($this->connection, 'users'));

        $rows = $this->db->getAllRows($this->connection, 'users');
        $this->assertEquals('John Doe', $rows[0]['name']);
        $this->assertEquals('john@example.com', $rows[0]['email']);
        $this->assertEquals(30, $rows[0]['age']);
        $this->assertIsInt($rows[0]['id']);
    }

    public function testInsertWithoutAutoIncrement(): void
    {
        $prepared = $this->persist->insert(
            'users',
            ['id' => 999, 'name' => 'Manual ID', 'email' => 'manual@example.com'],
            'id',
            false,
            'sqlite'
        );

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        $rows = $this->db->getAllRows($this->connection, 'users');
        $this->assertEquals(999, $rows[0]['id']);
        $this->assertEquals('Manual ID', $rows[0]['name']);
    }

    public function testInsertWithNullValues(): void
    {
        $prepared = $this->persist->insert(
            'users',
            ['name' => 'No Age User', 'email' => 'noage@example.com', 'age' => null],
            'id',
            true,
            'sqlite'
        );

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        $rows = $this->db->getAllRows($this->connection, 'users');
        $this->assertNull($rows[0]['age']);
    }

    // ==================== UPDATE Tests ====================

    public function testUpdateByPrimaryKey(): void
    {
        // Insert a user
        $this->db->insertTestData($this->connection, 'users', [
            ['name' => 'John Doe', 'email' => 'john@example.com', 'age' => 30]
        ]);

        $rows = $this->db->getAllRows($this->connection, 'users');
        $userId = $rows[0]['id'];

        // Update the user
        $prepared = $this->persist->update(
            'users',
            ['name' => 'John Updated', 'age' => 31],
            'id',
            $userId,
            'sqlite'
        );

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        // Verify update
        $updatedRows = $this->db->getAllRows($this->connection, 'users');
        $this->assertEquals('John Updated', $updatedRows[0]['name']);
        $this->assertEquals(31, $updatedRows[0]['age']);
        $this->assertEquals('john@example.com', $updatedRows[0]['email']); // Unchanged
    }

    public function testUpdateSingleColumn(): void
    {
        $this->db->insertTestData($this->connection, 'users', [
            ['name' => 'Jane Doe', 'email' => 'jane@example.com', 'age' => 25]
        ]);

        $rows = $this->db->getAllRows($this->connection, 'users');
        $userId = $rows[0]['id'];

        $prepared = $this->persist->update(
            'users',
            ['age' => 26],
            'id',
            $userId,
            'sqlite'
        );

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        $updatedRows = $this->db->getAllRows($this->connection, 'users');
        $this->assertEquals(26, $updatedRows[0]['age']);
        $this->assertEquals('Jane Doe', $updatedRows[0]['name']); // Unchanged
    }

    public function testUpdateWithNullValue(): void
    {
        $this->db->insertTestData($this->connection, 'users', [
            ['name' => 'Bob', 'email' => 'bob@example.com', 'age' => 40]
        ]);

        $rows = $this->db->getAllRows($this->connection, 'users');
        $userId = $rows[0]['id'];

        $prepared = $this->persist->update(
            'users',
            ['age' => null],
            'id',
            $userId,
            'sqlite'
        );

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        $updatedRows = $this->db->getAllRows($this->connection, 'users');
        $this->assertNull($updatedRows[0]['age']);
    }

    // ==================== DELETE Tests ====================

    public function testDeleteByPrimaryKey(): void
    {
        // Insert test data
        $this->db->insertTestData($this->connection, 'users', [
            ['name' => 'John Doe', 'email' => 'john@example.com', 'age' => 30],
            ['name' => 'Jane Doe', 'email' => 'jane@example.com', 'age' => 25]
        ]);

        $rows = $this->db->getAllRows($this->connection, 'users');
        $userIdToDelete = $rows[0]['id'];

        // Delete first user
        $prepared = $this->persist->delete('users', 'id', $userIdToDelete, 'sqlite');

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        // Verify deletion
        $this->assertEquals(1, $this->db->countRows($this->connection, 'users'));

        $remainingRows = $this->db->getAllRows($this->connection, 'users');
        $this->assertEquals('Jane Doe', $remainingRows[0]['name']);
    }

    public function testDeleteNonExistentRecord(): void
    {
        $prepared = $this->persist->delete('users', 'id', 99999, 'sqlite');

        $stmt = $this->connection->prepare($prepared->sql());
        $stmt->execute($prepared->bindings());

        $this->assertEquals(0, $stmt->rowCount());
    }

    // ==================== CRUD Cycle ====================

    public function testCompleteCrudCycle(): void
    {
        // 1. INSERT
        $insertPrepared = $this->persist->insert(
            'users',
            ['name' => 'CRUD User', 'email' => 'crud@example.com', 'age' => 28],
            'id',
            true,
            'sqlite'
        );
        $stmt = $this->connection->prepare($insertPrepared->sql());
        $stmt->execute($insertPrepared->bindings());

        $this->assertEquals(1, $this->db->countRows($this->connection, 'users'));

        // Get inserted ID
        $rows = $this->db->getAllRows($this->connection, 'users');
        $userId = $rows[0]['id'];

        // 2. UPDATE
        $updatePrepared = $this->persist->update(
            'users',
            ['name' => 'CRUD User Updated', 'age' => 29],
            'id',
            $userId,
            'sqlite'
        );
        $stmt = $this->connection->prepare($updatePrepared->sql());
        $stmt->execute($updatePrepared->bindings());

        $updatedRows = $this->db->getAllRows($this->connection, 'users');
        $this->assertEquals('CRUD User Updated', $updatedRows[0]['name']);
        $this->assertEquals(29, $updatedRows[0]['age']);

        // 3. DELETE
        $deletePrepared = $this->persist->delete('users', 'id', $userId, 'sqlite');
        $stmt = $this->connection->prepare($deletePrepared->sql());
        $stmt->execute($deletePrepared->bindings());

        $this->assertEquals(0, $this->db->countRows($this->connection, 'users'));
    }
}
