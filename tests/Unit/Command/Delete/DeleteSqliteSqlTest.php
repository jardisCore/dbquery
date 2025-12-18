<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Tests\Unit\Command\Delete;

use JardisCore\DbQuery\command\Delete\DeleteSqliteSql;
use PHPUnit\Framework\TestCase;

/**
 * Unit Tests for DeleteSqliteSql
 *
 * Tests SQLite-specific DELETE SQL generation.
 */
class DeleteSqliteSqlTest extends TestCase
{
    // ==================== Quote Identifier Tests ====================

    public function testQuoteIdentifierWithSimpleName(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('quoteIdentifier');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'users');

        $this->assertEquals('`users`', $result);
    }

    public function testQuoteIdentifierEscapesBackticks(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('quoteIdentifier');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'table`name');

        $this->assertEquals('`table``name`', $result);
    }

    public function testQuoteIdentifierWithMultipleBackticks(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('quoteIdentifier');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'tab``le');

        $this->assertEquals('`tab````le`', $result);
    }

    // ==================== Should Skip Join Type Tests ====================

    public function testShouldSkipFullJoin(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'FULL JOIN');

        $this->assertTrue($result);
    }

    public function testShouldSkipFullOuterJoin(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'FULL OUTER JOIN');

        $this->assertTrue($result);
    }

    public function testShouldSkipRightJoin(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'RIGHT JOIN');

        $this->assertTrue($result);
    }

    public function testShouldNotSkipInnerJoin(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'INNER JOIN');

        $this->assertFalse($result);
    }

    public function testShouldNotSkipLeftJoin(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('shouldSkipJoinType');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'LEFT JOIN');

        $this->assertFalse($result);
    }

    // ==================== JSON Extract Tests ====================

    public function testBuildJsonExtractSimplePath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonExtract');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', '$.status');

        $this->assertEquals("json_extract(`data`, '$.status')", $result);
    }

    public function testBuildJsonExtractNestedPath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonExtract');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'metadata', '$.user.id');

        $this->assertEquals("json_extract(`metadata`, '$.user.id')", $result);
    }

    public function testBuildJsonExtractArrayIndex(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonExtract');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'items', '$[0].name');

        $this->assertEquals("json_extract(`items`, '$[0].name')", $result);
    }

    // ==================== JSON Contains Tests ====================

    public function testBuildJsonContainsWithoutPath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonContains');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'tags', '?', null);

        $this->assertEquals("`tags` LIKE '%' || ? || '%'", $result);
    }

    public function testBuildJsonContainsWithPath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonContains');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', '?', '$.items');

        $expected = "json_extract(`data`, '$.items') LIKE '%' || ? || '%'";
        $this->assertEquals($expected, $result);
    }

    public function testBuildJsonContainsWithNestedPath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonContains');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'metadata', '?', '$.user.roles');

        $expected = "json_extract(`metadata`, '$.user.roles') LIKE '%' || ? || '%'";
        $this->assertEquals($expected, $result);
    }

    // ==================== JSON Length Tests ====================

    public function testBuildJsonLengthWithoutPath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonLength');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'items', null);

        $this->assertEquals("json_array_length(`items`)", $result);
    }

    public function testBuildJsonLengthWithPath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonLength');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'data', '$.tags');

        $this->assertEquals("json_array_length(`data`, '$.tags')", $result);
    }

    public function testBuildJsonLengthWithNestedPath(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $method = $reflection->getMethod('buildJsonLength');
        $method->setAccessible(true);

        $result = $method->invoke($builder, 'metadata', '$.user.permissions');

        $this->assertEquals("json_array_length(`metadata`, '$.user.permissions')", $result);
    }

    // ==================== Dialect Tests ====================

    public function testDialectIsSqlite(): void
    {
        $builder = new DeleteSqliteSql();
        $reflection = new \ReflectionClass($builder);
        $property = $reflection->getProperty('dialect');
        $property->setAccessible(true);

        $dialect = $property->getValue($builder);

        $this->assertEquals('sqlite', $dialect);
    }
}
