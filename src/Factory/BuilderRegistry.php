<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Factory;

/**
 * Registry for reusable stateless builders
 *
 * Ensures each builder is instantiated only once and can be reused
 * across multiple SQL generation operations without creating new instances.
 *
 * Supports version-aware builder resolution: if a version-specific override exists,
 * it will be loaded instead of the base implementation.
 *
 * Version overrides are primarily intended for forward compatibility - implementing
 * new database features with fallbacks to older syntax. For example, MySQL 8.4+
 * vector search could have a fallback implementation for MySQL 8.0.
 *
 * Override pattern: namespace\method\FullJoin → namespace\method\mysql\v84\FullJoin
 */
class BuilderRegistry
{
    /** @var array<string, object> */
    private static array $builders = [];

    /** @var array<string, string> Cache for resolved class names */
    private static array $classResolutionCache = [];

    /** @var string|null Current database dialect */
    private static ?string $dialect = null;

    /** @var string|null Current database version */
    private static ?string $version = null;

    /**
     * Set the current database context for version-aware builder resolution
     *
     * @param string|null $dialect Database dialect (mysql, postgres, mariadb, sqlite)
     * @param string|null $version Database version (e.g., '5.7', '8.0')
     */
    public static function setContext(?string $dialect, ?string $version): void
    {
        self::$dialect = $dialect !== null ? strtolower($dialect) : null;
        self::$version = $version;
    }

    /**
     * Get or create a builder instance
     *
     * Supports version-aware resolution: attempts to load version-specific override
     * before falling back to the base class.
     *
     * @template T of object
     * @param class-string<T> $builderClass The fully qualified builder class name
     * @return T The builder instance
     */
    public static function get(string $builderClass): object
    {
        // Fast path: no version context set
        if (self::$dialect === null || self::$version === null) {
            if (!isset(self::$builders[$builderClass])) {
                self::$builders[$builderClass] = new $builderClass();
            }
            /** @var T $builder */
            $builder = self::$builders[$builderClass];
            return $builder;
        }

        // Generate cache key for resolution
        $cacheKey = $builderClass . '|' . self::$dialect . '|' . self::$version;

        // Check resolution cache first (avoids string operations + class_exists)
        if (!isset(self::$classResolutionCache[$cacheKey])) {
            self::$classResolutionCache[$cacheKey] = self::resolveVersionedClass($builderClass);
        }

        $resolvedClass = self::$classResolutionCache[$cacheKey];

        // Get or create builder instance
        if (!isset(self::$builders[$resolvedClass])) {
            self::$builders[$resolvedClass] = new $resolvedClass();
        }

        /** @var T $builder */
        $builder = self::$builders[$resolvedClass];
        return $builder;
    }

    /**
     * Resolve version-specific builder class if available
     *
     * Attempts to find a version-specific override in the pattern:
     * namespace\method\FullJoin → namespace\method\mysql\v57\FullJoin
     *
     * @param string $baseClass Base builder class name
     * @return string Resolved class name (versioned or base)
     */
    private static function resolveVersionedClass(string $baseClass): string
    {
        // This should never be called with null values (checked in get()),
        // but we assert for type safety
        assert(self::$dialect !== null && self::$version !== null);

        // Build version namespace inline for performance
        // 'mysql' + '8.0' → 'mysql\v80'
        $versionNamespace = self::$dialect . '\\v' . str_replace('.', '', self::$version);

        // Find last backslash position (more efficient than explode for single operation)
        $lastBackslash = strrpos($baseClass, '\\');

        if ($lastBackslash === false) {
            // No namespace - unlikely but handle gracefully
            return $baseClass;
        }

        // Inject version namespace before class name
        // JardisCore\DbQuery\query\builder\method + mysql\v80 + FullJoin
        $versionedClass = substr($baseClass, 0, $lastBackslash + 1)
                        . $versionNamespace . '\\'
                        . substr($baseClass, $lastBackslash + 1);

        // Check if version-specific override exists
        if (class_exists($versionedClass, false)) {
            return $versionedClass;
        }

        // Try with autoloader (slower)
        if (class_exists($versionedClass)) {
            return $versionedClass;
        }

        // Fallback to base class
        return $baseClass;
    }

    /**
     * Clear all cached builders (useful for testing)
     */
    public static function clear(): void
    {
        self::$builders = [];
        self::$classResolutionCache = [];
        self::$dialect = null;
        self::$version = null;
    }
}
