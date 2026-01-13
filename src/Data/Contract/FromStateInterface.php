<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Data\Contract;

use JardisPsr\DbQuery\DbQueryBuilderInterface;

/**
 * Interface for states that support FROM clause building
 *
 * Required by FromBuilder to access container and alias information.
 */
interface FromStateInterface
{
    /**
     * Get the table name or subquery for the FROM clause
     *
     * @return string|DbQueryBuilderInterface
     */
    public function getContainer(): string|DbQueryBuilderInterface;

    /**
     * Get the alias for the FROM clause
     *
     * @return string|null
     */
    public function getAlias(): ?string;
}
