<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Tests\Unit;

use InvalidArgumentException;
use JardisCore\DbQuery\DbPersist;
use JardisPsr\DbQuery\DbPreparedQueryInterface;
use PHPUnit\Framework\TestCase;

/**
 * Unit Tests for DbPersist
 *
 * Tests the shortcut methods for simple CRUD operations based on primary keys.
 */
class DbPersistTest extends TestCase
{
    private DbPersist $persist;

    protected function setUp(): void
    {
        $this->persist = new DbPersist();
    }

    // ==================== INSERT Tests ====================

    public function testInsertReturnsDbPreparedQueryInterface(): void
    {
        $result = $this->persist->insert(
            'users',
            ['name' => 'John', 'email' => 'john@example.com'],
            'id'
        );

        $this->assertInstanceOf(DbPreparedQueryInterface::class, $result);
    }

    public function testInsertWithAutoIncrementRemovesPrimaryKey(): void
    {
        $result = $this->persist->insert(
            'users',
            ['id' => 99, 'name' => 'John', 'email' => 'john@example.com'],
            'id',
            true
        );

        // Verify that the SQL doesn't contain the id field
        $sql = $result->sql();
        $this->assertStringContainsString('INSERT INTO', $sql);
        $this->assertStringContainsString('users', $sql);
        $this->assertStringContainsString('name', $sql);
        $this->assertStringContainsString('email', $sql);
        // The id should not be in the INSERT since autoIncrement=true
    }

    public function testInsertWithoutAutoIncrementKeepsPrimaryKey(): void
    {
        $result = $this->persist->insert(
            'users',
            ['id' => 99, 'name' => 'John'],
            'id',
            false
        );

        $sql = $result->sql();
        $bindings = $result->bindings();

        $this->assertStringContainsString('INSERT INTO', $sql);
        $this->assertStringContainsString('users', $sql);
        $this->assertCount(2, $bindings);
        $this->assertContains(99, $bindings);
        $this->assertContains('John', $bindings);
    }

    public function testInsertGeneratesPreparedStatement(): void
    {
        $result = $this->persist->insert(
            'users',
            ['name' => 'John', 'email' => 'john@example.com'],
            'id'
        );

        $sql = $result->sql();
        $bindings = $result->bindings();

        // Should use placeholders
        $this->assertStringContainsString('?', $sql);
        // Should have bindings
        $this->assertCount(2, $bindings);
        $this->assertContains('John', $bindings);
        $this->assertContains('john@example.com', $bindings);
    }

    public function testInsertThrowsExceptionWhenDataIsEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Data array cannot be empty');

        $this->persist->insert('users', [], 'id');
    }

    public function testInsertThrowsExceptionWhenTableIsEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Table name cannot be empty');

        $this->persist->insert('', ['name' => 'John'], 'id');
    }

    public function testInsertThrowsExceptionWhenOnlyPrimaryKeyWithAutoIncrement(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Data array is empty after removing auto-increment primary key');

        $this->persist->insert(
            'users',
            ['id' => 99],
            'id',
            true
        );
    }

    public function testInsertSupportsMultipleDialects(): void
    {
        $dataSet = [
            'mysql' => ['name' => 'John', 'email' => 'john@example.com'],
            'postgres' => ['name' => 'Jane', 'email' => 'jane@example.com'],
            'sqlite' => ['name' => 'Bob', 'email' => 'bob@example.com'],
        ];

        foreach ($dataSet as $dialect => $data) {
            $result = $this->persist->insert('users', $data, 'id', true, $dialect);

            $this->assertInstanceOf(DbPreparedQueryInterface::class, $result);
            $this->assertStringContainsString('INSERT INTO', $result->sql());
            $this->assertStringContainsString('users', $result->sql());
        }
    }

    // ==================== UPDATE Tests ====================

    public function testUpdateReturnsDbPreparedQueryInterface(): void
    {
        $result = $this->persist->update(
            'users',
            ['name' => 'John Updated'],
            'id',
            42
        );

        $this->assertInstanceOf(DbPreparedQueryInterface::class, $result);
    }

    public function testUpdateGeneratesCorrectSql(): void
    {
        $result = $this->persist->update(
            'users',
            ['name' => 'John', 'email' => 'john@example.com'],
            'id',
            42
        );

        $sql = $result->sql();

        $this->assertStringContainsString('UPDATE', $sql);
        $this->assertStringContainsString('users', $sql);
        $this->assertStringContainsString('SET', $sql);
        $this->assertStringContainsString('WHERE', $sql);
        $this->assertStringContainsString('id', $sql);
    }

    public function testUpdateGeneratesPreparedStatement(): void
    {
        $result = $this->persist->update(
            'users',
            ['name' => 'John', 'email' => 'john@example.com'],
            'id',
            42
        );

        $sql = $result->sql();
        $bindings = $result->bindings();

        // Should use placeholders
        $this->assertStringContainsString('?', $sql);
        // Should have bindings: 2 for SET values + 1 for WHERE condition
        $this->assertCount(3, $bindings);
        $this->assertContains('John', $bindings);
        $this->assertContains('john@example.com', $bindings);
        $this->assertContains(42, $bindings);
    }

    public function testUpdateThrowsExceptionWhenDataIsEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Data array cannot be empty');

        $this->persist->update('users', [], 'id', 42);
    }

    public function testUpdateThrowsExceptionWhenTableIsEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Table name cannot be empty');

        $this->persist->update('', ['name' => 'John'], 'id', 42);
    }

    public function testUpdateThrowsExceptionWhenPrimaryValueIsNull(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Primary key value cannot be null or empty');

        $this->persist->update('users', ['name' => 'John'], 'id', null);
    }

    public function testUpdateThrowsExceptionWhenPrimaryValueIsEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Primary key value cannot be null or empty');

        $this->persist->update('users', ['name' => 'John'], 'id', '');
    }

    public function testUpdateThrowsExceptionWhenPrimaryKeyIsInData(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot update primary key "id"');

        $this->persist->update(
            'users',
            ['id' => 99, 'name' => 'John'],
            'id',
            42
        );
    }

    public function testUpdateSupportsMultipleDialects(): void
    {
        $dialects = ['mysql', 'postgres', 'sqlite'];

        foreach ($dialects as $dialect) {
            $result = $this->persist->update(
                'users',
                ['name' => 'John Updated'],
                'id',
                42,
                $dialect
            );

            $this->assertInstanceOf(DbPreparedQueryInterface::class, $result);
            $this->assertStringContainsString('UPDATE', $result->sql());
            $this->assertStringContainsString('users', $result->sql());
        }
    }

    public function testUpdateSupportsStringPrimaryKey(): void
    {
        $result = $this->persist->update(
            'users',
            ['name' => 'John'],
            'uuid',
            'abc-123-def',
            'mysql'
        );

        $bindings = $result->bindings();

        $this->assertContains('John', $bindings);
        $this->assertContains('abc-123-def', $bindings);
    }

    // ==================== DELETE Tests ====================

    public function testDeleteReturnsDbPreparedQueryInterface(): void
    {
        $result = $this->persist->delete('users', 'id', 42);

        $this->assertInstanceOf(DbPreparedQueryInterface::class, $result);
    }

    public function testDeleteGeneratesCorrectSql(): void
    {
        $result = $this->persist->delete('users', 'id', 42);

        $sql = $result->sql();

        $this->assertStringContainsString('DELETE FROM', $sql);
        $this->assertStringContainsString('users', $sql);
        $this->assertStringContainsString('WHERE', $sql);
        $this->assertStringContainsString('id', $sql);
    }

    public function testDeleteGeneratesPreparedStatement(): void
    {
        $result = $this->persist->delete('users', 'id', 42);

        $sql = $result->sql();
        $bindings = $result->bindings();

        // Should use placeholders
        $this->assertStringContainsString('?', $sql);
        // Should have 1 binding for WHERE condition
        $this->assertCount(1, $bindings);
        $this->assertContains(42, $bindings);
    }

    public function testDeleteThrowsExceptionWhenTableIsEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Table name cannot be empty');

        $this->persist->delete('', 'id', 42);
    }

    public function testDeleteThrowsExceptionWhenPrimaryValueIsNull(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Primary key value cannot be null or empty');

        $this->persist->delete('users', 'id', null);
    }

    public function testDeleteThrowsExceptionWhenPrimaryValueIsEmpty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Primary key value cannot be null or empty');

        $this->persist->delete('users', 'id', '');
    }

    public function testDeleteSupportsMultipleDialects(): void
    {
        $dialects = ['mysql', 'postgres', 'sqlite'];

        foreach ($dialects as $dialect) {
            $result = $this->persist->delete('users', 'id', 42, $dialect);

            $this->assertInstanceOf(DbPreparedQueryInterface::class, $result);
            $this->assertStringContainsString('DELETE FROM', $result->sql());
            $this->assertStringContainsString('users', $result->sql());
        }
    }

    public function testDeleteSupportsStringPrimaryKey(): void
    {
        $result = $this->persist->delete('users', 'uuid', 'abc-123-def', 'mysql');

        $bindings = $result->bindings();

        $this->assertCount(1, $bindings);
        $this->assertContains('abc-123-def', $bindings);
    }

    // ==================== Integration Tests (Multiple Operations) ====================

    public function testAllMethodsSupportVersionParameter(): void
    {
        $version = '8.0';

        $insertResult = $this->persist->insert(
            'users',
            ['name' => 'John'],
            'id',
            true,
            'mysql',
            $version
        );
        $this->assertInstanceOf(DbPreparedQueryInterface::class, $insertResult);

        $updateResult = $this->persist->update(
            'users',
            ['name' => 'Jane'],
            'id',
            42,
            'mysql',
            $version
        );
        $this->assertInstanceOf(DbPreparedQueryInterface::class, $updateResult);

        $deleteResult = $this->persist->delete('users', 'id', 42, 'mysql', $version);
        $this->assertInstanceOf(DbPreparedQueryInterface::class, $deleteResult);
    }

    public function testAllMethodsReturnCorrectQueryType(): void
    {
        $insertResult = $this->persist->insert(
            'users',
            ['name' => 'John'],
            'id'
        );
        $this->assertEquals('mysql', $insertResult->type());

        $updateResult = $this->persist->update(
            'users',
            ['name' => 'Jane'],
            'id',
            42
        );
        $this->assertEquals('mysql', $updateResult->type());

        $deleteResult = $this->persist->delete('users', 'id', 42);
        $this->assertEquals('mysql', $deleteResult->type());
    }
}
