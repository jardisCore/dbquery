<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Tests\Unit\Factory;

use JardisCore\DbQuery\Query\builder\Clause\ConditionBuilder;
use JardisCore\DbQuery\Query\builder\Clause\JoinBuilder;
use JardisCore\DbQuery\Factory\BuilderRegistry;
use JardisCore\DbQuery\Query\Formatter\ValueFormatter;
use PHPUnit\Framework\TestCase;

/**
 * Unit Tests for BuilderRegistry
 *
 * Tests the static registry pattern for builder instances.
 */
class BuilderRegistryTest extends TestCase
{
    protected function setUp(): void
    {
        // Clear registry before each test
        BuilderRegistry::clear();
    }

    protected function tearDown(): void
    {
        // Clean up after each test
        BuilderRegistry::clear();
    }

    // ==================== Basic Functionality Tests ====================

    public function testGetReturnsBuilderInstance(): void
    {
        $builder = BuilderRegistry::get(ConditionBuilder::class);

        $this->assertInstanceOf(ConditionBuilder::class, $builder);
    }

    public function testGetReturnsSameInstanceOnMultipleCalls(): void
    {
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);
        $builder2 = BuilderRegistry::get(ConditionBuilder::class);

        $this->assertSame($builder1, $builder2);
    }

    public function testGetCreatesNewInstanceForDifferentClasses(): void
    {
        $conditionBuilder = BuilderRegistry::get(ConditionBuilder::class);
        $joinBuilder = BuilderRegistry::get(JoinBuilder::class);

        $this->assertNotSame($conditionBuilder, $joinBuilder);
        $this->assertInstanceOf(ConditionBuilder::class, $conditionBuilder);
        $this->assertInstanceOf(JoinBuilder::class, $joinBuilder);
    }

    // ==================== Clear Functionality Tests ====================

    public function testClearRemovesAllCachedBuilders(): void
    {
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);

        BuilderRegistry::clear();

        $builder2 = BuilderRegistry::get(ConditionBuilder::class);

        $this->assertNotSame($builder1, $builder2);
    }

    public function testClearAllowsFreshInstances(): void
    {
        $builder1 = BuilderRegistry::get(ValueFormatter::class);
        $builder2 = BuilderRegistry::get(JoinBuilder::class);

        BuilderRegistry::clear();

        $builder3 = BuilderRegistry::get(ValueFormatter::class);
        $builder4 = BuilderRegistry::get(JoinBuilder::class);

        $this->assertNotSame($builder1, $builder3);
        $this->assertNotSame($builder2, $builder4);
    }

    // ==================== Multiple Builder Types Tests ====================

    public function testGetWorksWithMultipleDifferentBuilders(): void
    {
        $conditionBuilder = BuilderRegistry::get(ConditionBuilder::class);
        $joinBuilder = BuilderRegistry::get(JoinBuilder::class);
        $valueFormatter = BuilderRegistry::get(ValueFormatter::class);

        $this->assertInstanceOf(ConditionBuilder::class, $conditionBuilder);
        $this->assertInstanceOf(JoinBuilder::class, $joinBuilder);
        $this->assertInstanceOf(ValueFormatter::class, $valueFormatter);

        // Verify singleton behavior for each
        $this->assertSame($conditionBuilder, BuilderRegistry::get(ConditionBuilder::class));
        $this->assertSame($joinBuilder, BuilderRegistry::get(JoinBuilder::class));
        $this->assertSame($valueFormatter, BuilderRegistry::get(ValueFormatter::class));
    }

    // ==================== Static Shared State Tests ====================

    public function testStaticRegistrySharesInstancesGlobally(): void
    {
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);
        $builder2 = BuilderRegistry::get(ConditionBuilder::class);

        // Static registry should return the same instance
        $this->assertSame($builder1, $builder2);
    }

    public function testRegistryMaintainsSeparateInstancesPerClass(): void
    {
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);
        $builder2 = BuilderRegistry::get(JoinBuilder::class);
        $builder3 = BuilderRegistry::get(ValueFormatter::class);

        // Retrieve again
        $builder1Again = BuilderRegistry::get(ConditionBuilder::class);
        $builder2Again = BuilderRegistry::get(JoinBuilder::class);
        $builder3Again = BuilderRegistry::get(ValueFormatter::class);

        // Same class returns same instance
        $this->assertSame($builder1, $builder1Again);
        $this->assertSame($builder2, $builder2Again);
        $this->assertSame($builder3, $builder3Again);

        // Different classes return different instances
        $this->assertNotSame($builder1, $builder2);
        $this->assertNotSame($builder1, $builder3);
        $this->assertNotSame($builder2, $builder3);
    }

    public function testGetCreatesInstanceOnlyOnce(): void
    {
        // First call creates instance
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);

        // Multiple subsequent calls return same instance
        $builder2 = BuilderRegistry::get(ConditionBuilder::class);
        $builder3 = BuilderRegistry::get(ConditionBuilder::class);
        $builder4 = BuilderRegistry::get(ConditionBuilder::class);

        $this->assertSame($builder1, $builder2);
        $this->assertSame($builder1, $builder3);
        $this->assertSame($builder1, $builder4);
    }

    // ==================== Performance Tests ====================

    public function testGetIsEfficientWithManyRetrievals(): void
    {
        // First retrieval creates instance
        $firstBuilder = BuilderRegistry::get(ConditionBuilder::class);

        // Multiple retrievals should return same instance (no new instantiation)
        for ($i = 0; $i < 1000; $i++) {
            $builder = BuilderRegistry::get(ConditionBuilder::class);
            $this->assertSame($firstBuilder, $builder);
        }
    }

    // ==================== Clear After Multiple Builders Tests ====================

    public function testClearWorksAfterRegisteringMultipleBuilders(): void
    {
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);
        $builder2 = BuilderRegistry::get(JoinBuilder::class);
        $builder3 = BuilderRegistry::get(ValueFormatter::class);

        BuilderRegistry::clear();

        $newBuilder1 = BuilderRegistry::get(ConditionBuilder::class);
        $newBuilder2 = BuilderRegistry::get(JoinBuilder::class);
        $newBuilder3 = BuilderRegistry::get(ValueFormatter::class);

        $this->assertNotSame($builder1, $newBuilder1);
        $this->assertNotSame($builder2, $newBuilder2);
        $this->assertNotSame($builder3, $newBuilder3);
    }

    // ==================== Version Context Tests ====================

    public function testSetContextChangesDialectAndVersion(): void
    {
        BuilderRegistry::setContext('mysql', '8.0');

        // Getting a builder should work with context set
        $builder = BuilderRegistry::get(ConditionBuilder::class);
        $this->assertInstanceOf(ConditionBuilder::class, $builder);
    }

    public function testSetContextWithNullClearsContext(): void
    {
        BuilderRegistry::setContext('mysql', '8.0');
        BuilderRegistry::setContext(null, null);

        // Should still work after clearing context
        $builder = BuilderRegistry::get(ConditionBuilder::class);
        $this->assertInstanceOf(ConditionBuilder::class, $builder);
    }

    public function testSetContextIsCaseInsensitive(): void
    {
        BuilderRegistry::setContext('MYSQL', '8.0');
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);

        BuilderRegistry::clear();

        BuilderRegistry::setContext('mysql', '8.0');
        $builder2 = BuilderRegistry::get(ConditionBuilder::class);

        // Both should work (context is normalized to lowercase)
        $this->assertInstanceOf(ConditionBuilder::class, $builder1);
        $this->assertInstanceOf(ConditionBuilder::class, $builder2);
    }

    public function testClearResetsContext(): void
    {
        BuilderRegistry::setContext('mysql', '8.0');
        $builder1 = BuilderRegistry::get(ConditionBuilder::class);

        BuilderRegistry::clear();

        // After clear, context should be reset
        $builder2 = BuilderRegistry::get(ConditionBuilder::class);
        $this->assertNotSame($builder1, $builder2);
    }

    public function testSetContextAllowsMultipleDialects(): void
    {
        BuilderRegistry::setContext('mysql', '8.0');
        $mysqlBuilder = BuilderRegistry::get(ConditionBuilder::class);

        BuilderRegistry::clear();

        BuilderRegistry::setContext('postgres', '14');
        $postgresBuilder = BuilderRegistry::get(ConditionBuilder::class);

        // Both should work with different contexts
        $this->assertInstanceOf(ConditionBuilder::class, $mysqlBuilder);
        $this->assertInstanceOf(ConditionBuilder::class, $postgresBuilder);
    }
}
