<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Exception;

use RuntimeException;

/**
 * Exception thrown when a database feature is not supported in the specified version
 *
 * This exception is used in version-specific builder overrides when a SQL feature
 * is not available in the target database version (e.g., FULL OUTER JOIN in MySQL 5.7).
 */
class UnsupportedFeatureException extends RuntimeException
{
    /**
     * Create and throw exception for an unsupported feature
     *
     * This method uses PHP 8.2's `never` return type to indicate
     * that it will always throw an exception and never return normally.
     *
     * @param string $feature The unsupported feature name
     * @param string $dialect Database dialect
     * @param string $version Database version
     * @param string|null $suggestion Optional suggestion for alternative approach
     * @return never This method always throws and never returns
     */
    public static function forFeature(
        string $feature,
        string $dialect,
        string $version,
        ?string $suggestion = null
    ): never {
        $message = sprintf(
            '%s is not supported in %s %s',
            $feature,
            strtoupper($dialect),
            $version
        );

        if ($suggestion !== null) {
            $message .= '. ' . $suggestion;
        }

        throw new self($message);
    }
}
