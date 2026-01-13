<?php

declare(strict_types=1);

namespace JardisCore\DbQuery\Command\Insert;

use JardisCore\DbQuery\Data\Dialect;

/**
 * MySQL/MariaDB INSERT SQL Generator
 *
 * Uses backtick quoting for identifiers.
 * Supports all MySQL INSERT features.
 */
class InsertMySql extends InsertSqlBuilder
{
    protected string $dialect = Dialect::MySQL->value;

    /**
     * Quote identifier with backticks for MySQL
     *
     * @param string $identifier
     * @return string
     */
    protected function quoteIdentifier(string $identifier): string
    {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    /**
     * Build OR IGNORE for MySQL (INSERT IGNORE)
     *
     * @return string
     */
    protected function buildOrIgnoreInsert(): string
    {
        return 'INSERT IGNORE';
    }

    /**
     * Build REPLACE for MySQL
     *
     * @return string
     */
    protected function buildReplaceInsert(): string
    {
        return 'REPLACE';
    }
}
