<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Tests\Unit\Command\Update;

use JardisCore\DbQuery\command\update\UpdateSqliteSql;
use JardisCore\DbQuery\Data\UpdateState;
use PHPUnit\Framework\TestCase;

/**
 * Unit Tests for UpdateSqliteSql
 *
 * Tests SQLite-specific UPDATE SQL generation.
 */
class UpdateSqliteSqlTest extends TestCase
{
    // ==================== Quote Identifier Tests ====================

    public function testQuoteIdentifierWithSimpleName(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('quoteIdentifier');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'column_name');

        $this->assertEquals('`column_name`', $result);
    }

    public function testQuoteIdentifierEscapesBackticks(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('quoteIdentifier');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'column`name');

        $this->assertEquals('`column``name`', $result);
    }

    public function testQuoteIdentifierWithTableAndColumn(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('quoteIdentifier');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'users.id');

        $this->assertEquals('`users.id`', $result);
    }

    // ==================== Should Skip Join Type Tests ====================

    public function testShouldSkipFullJoin(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'FULL JOIN');

        $this->assertTrue($result);
    }

    public function testShouldSkipFullOuterJoin(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'FULL OUTER JOIN');

        $this->assertTrue($result);
    }

    public function testShouldNotSkipInnerJoin(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'INNER JOIN');

        $this->assertFalse($result);
    }

    public function testShouldNotSkipLeftJoin(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'LEFT JOIN');

        $this->assertFalse($result);
    }

    // ==================== JSON Extract Tests ====================

    public function testBuildJsonExtractSimplePath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonExtract');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', '$.age');

        $this->assertEquals("json_extract(`data`, '$.age')", $result);
    }

    public function testBuildJsonExtractNestedPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonExtract');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'metadata', '$.user.name');

        $this->assertEquals("json_extract(`metadata`, '$.user.name')", $result);
    }

    public function testBuildJsonExtractEscapesQuotes(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonExtract');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', "$.field'test");

        $this->assertStringContainsString("json_extract(`data`,", $result);
    }

    // ==================== JSON Contains Tests ====================

    public function testBuildJsonContainsWithoutPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonContains');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'tags', '?', null);

        $this->assertEquals("EXISTS (SELECT 1 FROM json_each(`tags`) WHERE value = ?)", $result);
    }

    public function testBuildJsonContainsWithPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonContains');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', '?', '$.items');

        $expected = "EXISTS (SELECT 1 FROM json_each(json_extract(`data`, '$.items')) WHERE value = ?)";
        $this->assertEquals($expected, $result);
    }

    // ==================== JSON Not Contains Tests ====================

    public function testBuildJsonNotContainsWithoutPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonNotContains');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'tags', '?', null);

        $this->assertEquals("NOT EXISTS (SELECT 1 FROM json_each(`tags`) WHERE value = ?)", $result);
    }

    public function testBuildJsonNotContainsWithPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonNotContains');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', '?', '$.items');

        $expected = "NOT EXISTS (SELECT 1 FROM json_each(json_extract(`data`, '$.items')) WHERE value = ?)";
        $this->assertEquals($expected, $result);
    }

    // ==================== JSON Length Tests ====================

    public function testBuildJsonLengthWithoutPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonLength');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'items', null);

        $this->assertEquals("json_array_length(`items`)", $result);
    }

    public function testBuildJsonLengthWithPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonLength');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', '$.tags');

        $expected = "json_array_length(json_extract(`data`, '$.tags'))";
        $this->assertEquals($expected, $result);
    }

    public function testBuildJsonLengthWithNestedPath(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonLength');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'metadata', '$.user.roles');

        $expected = "json_array_length(json_extract(`metadata`, '$.user.roles'))";
        $this->assertEquals($expected, $result);
    }

    // ==================== Dialect Tests ====================

    public function testDialectIsSqlite(): void
    {
        $builder = new UpdateSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $property = $reflection->getProperty('dialect');
        $property->setAccessible(true);

        $dialect = $property->getValue($builder);

        $this->assertEquals('sqlite', $dialect);
    }
}
