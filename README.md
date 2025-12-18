# DbQuery

![Build Status](https://github.com/jardisCore/dbquery/actions/workflows/ci.yml/badge.svg)
[![PHP Version](https://img.shields.io/badge/php-%3E%3D8.2-blue.svg)](https://www.php.net/)
[![License](https://img.shields.io/badge/license-PolyForm%20Noncommercial-blue.svg)](LICENSE)
[![PHPStan Level](https://img.shields.io/badge/PHPStan-Level%208-success.svg)](phpstan.neon)
[![PSR-4](https://img.shields.io/badge/autoload-PSR--4-blue.svg)](https://www.php-fig.org/psr/psr-4/)
[![PSR-12](https://img.shields.io/badge/code%20style-PSR--12-orange.svg)](phpcs.xml)
[![Coverage](https://img.shields.io/badge/coverage->96%25-brightgreen)](https://github.com/jardiscore/dbquery)

**Enterprise-Grade SQL Query Builder for PHP 8.2+**

Production-proven, type-safe, and truly database-agnostic. Write once, deploy anywhere: MySQL/MariaDB, PostgreSQL, and SQLite.

---

## 🏆 Enterprise-Level Capabilities

DbQuery is not just another query builder – it's an enterprise-grade solution that handles the complexity modern applications demand:

### Advanced SQL Features Out-of-the-Box
- **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD with partitioning and ordering
- **Common Table Expressions (CTEs)** - WITH and WITH RECURSIVE for complex hierarchical queries
- **Subqueries Everywhere** - Nested queries in SELECT, FROM, WHERE, and HAVING clauses
- **Complex JOINs** - All join types (INNER, LEFT, RIGHT, FULL, CROSS) with subquery support
- **JSON Operations** - Database-agnostic JSON querying with automatic dialect translation
- **UNION/UNION ALL** - Combine multiple result sets seamlessly
- **Conflict Handling** - ON DUPLICATE KEY UPDATE (MySQL), ON CONFLICT (PostgreSQL), OR IGNORE/REPLACE (SQLite)
- **Aggregate Functions** - GROUP BY, HAVING with full support for complex aggregations

### 🎯 **True Database Independence**
Switch between database systems **without changing a single line of code**. DbQuery automatically translates your queries into the correct SQL dialect – including advanced features like JSON operations, window functions, and CTEs. Deploy to MySQL today, PostgreSQL tomorrow, SQLite for testing – **one codebase for all**.

This isn't just basic CRUD portability – we handle the hard stuff:
- JSON path differences: `JSON_EXTRACT()` (MySQL) ↔ `->` / `->>` (PostgreSQL)
- Conflict resolution: `ON DUPLICATE KEY` (MySQL) ↔ `ON CONFLICT` (PostgreSQL)
- Window function syntax variations across all dialects
- Data type conversions and function name mappings

### 🔒 **Security by Design**
Prepared statements are **standard, not optional**. Every query is automatically protected against SQL injection attacks. No manual validation, no forgotten sanitization – **secure by default**. Built for enterprises that can't afford security vulnerabilities.

### 📊 **Database-Agnostic JSON Support**
Work with JSON data across all databases with a unified API. The drastically different JSON syntax between MySQL (`JSON_EXTRACT()`), PostgreSQL (`->` / `->>` operators), and SQLite is completely abstracted:

```php
// One code, three dialects
->whereJson('data')->extract('$.user.email')->equals('user@example.com')
```

### ✨ **Modern PHP Excellence**
- **PHPStan Level 8 certified** – highest static analysis level
- **Full PHP 8.2+ type safety** – strict types, enums, readonly properties
- **PSR-12 compliant** – industry-standard coding style
- **Zero runtime dependencies** – only requires PDO (included in PHP)

### 🚀 **Production-Ready Architecture**
- **Builder Pattern** – Clean separation of query building and SQL generation
- **State Objects** – Enable efficient caching and query reuse
- **Factory Pattern** – Automatic dialect-specific builder selection
- **Registry Pattern** – Prevents unnecessary object instantiation
- **Battle-tested** – Proven in demanding production environments

## The Difference in Practice

**Before** (Raw SQL with manual dialect handling):
```php
// MySQL
$sql = "SELECT * FROM users WHERE JSON_EXTRACT(settings, '$.theme') = ? AND age > ?";
// PostgreSQL
$sql = "SELECT * FROM users WHERE settings->>'theme' = ? AND age > ?";
// Manual binding, error-prone
$stmt = $pdo->prepare($sql);
$stmt->execute(['dark', 18]);
```

**After** (DbQuery - one code for all DBs):
```php
$query = new DbQuery();
$query->select('*')
      ->from('users')
      ->whereJson('settings')->extract('$.theme')->equals('dark')
      ->and('age')->greater(18);

$result = $query->sql('mysql');  // Or 'postgres' or 'sqlite'
// Prepared statements automatic, correct dialect syntax guaranteed
```

---

## Feature Overview

### Query Types
- **SELECT** - Complex queries with subqueries, CTEs, window functions
- **INSERT** - Single/multi-row inserts, INSERT...SELECT, conflict handling
- **UPDATE** - Conditional updates with complex WHERE clauses
- **DELETE** - Safe deletion with flexible filtering
- **DbPersist** - Shortcut methods for simple CRUD operations by primary key

### Advanced SQL Features
- **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG OVER
- **CTEs** - WITH and WITH RECURSIVE for hierarchical data
- **Subqueries** - In SELECT, FROM, WHERE, HAVING, and JOIN clauses
- **All JOIN Types** - INNER, LEFT, RIGHT, FULL OUTER, CROSS
- **UNION/UNION ALL** - Combine result sets
- **GROUP BY & HAVING** - Aggregations with filtering
- **Complex Conditions** - Nested brackets, OR/AND logic, EXISTS, IN

### JSON Operations
- **JSON Extraction** - `extract()` for nested paths
- **JSON Array Operations** - `contains()`, `length()`
- **Cross-Database** - Same API for MySQL, PostgreSQL, SQLite

### Conflict Handling
- **MySQL/MariaDB** - ON DUPLICATE KEY UPDATE
- **PostgreSQL** - ON CONFLICT DO UPDATE/DO NOTHING
- **SQLite** - OR IGNORE, OR REPLACE

---

## Requirements

- PHP 8.2 or higher
- PDO extension
- Docker & Docker Compose (for development)
- One of the supported databases:
  - MySQL 8.0+ (default version: 8.0)
  - MariaDB 10.11+ (default version: 10.11)
  - PostgreSQL 14+ (default version: 14)
  - SQLite 3.35+ (default version: 3.35)

## Installation

```bash
composer require jardiscore/dbquery
```

## Quick Start

### Basic SELECT Query

```php
use JardisCore\DbQuery\DbQuery;

$query = new DbQuery();
$query->select('id, name, email')
      ->from('users')
      ->where('age')->greater(18)
      ->and('status')->equals('active')
      ->orderBy('name', 'ASC')
      ->limit(10);

// Generate SQL for specific dialect
$prepared = $query->sql('mysql', prepared: true);

echo $prepared->sql();          // SQL with placeholders
print_r($prepared->bindings()); // Values for prepared statement
echo $prepared->type();         // Query type: 'SELECT'

// Optional: Specify database version for forward compatibility
$prepared = $query->sql('mysql', prepared: true, '8.4');
```

### INSERT Operations

```php
use JardisCore\DbQuery\DbInsert;

// Option 1: fields() + values() for multiple rows
$insert = new DbInsert();
$insert->into('users')
       ->fields('name', 'email', 'created_at')
       ->values('John Doe', 'john@example.com', '2024-01-01')
       ->values('Jane Smith', 'jane@example.com', '2024-01-02');

$sql = $insert->sql('mysql');

// Option 2: set() for single row with associative array
$insert = new DbInsert();
$insert->into('users')
       ->set([
           'name' => 'John Doe',
           'email' => 'john@example.com',
           'created_at' => '2024-01-01'
       ]);

$sql = $insert->sql('mysql');

// Option 3: INSERT ... SELECT
$select = new DbQuery();
$select->select('name, email')->from('temp_users');

$insert = new DbInsert();
$insert->into('users')
       ->fields('name', 'email')
       ->fromSelect($select);

$sql = $insert->sql('mysql');
```

### UPDATE Operations

```php
use JardisCore\DbQuery\DbUpdate;

$update = new DbUpdate();
$update->table('users')
       ->set('status', 'inactive')
       ->set('updated_at', 'NOW()')
       ->where('last_login')->lower('2023-01-01');

$sql = $update->sql('mysql');

// Set multiple columns at once
$update = new DbUpdate();
$update->table('users')
       ->setMultiple([
           'status' => 'inactive',
           'updated_at' => '2024-01-01'
       ])
       ->where('id')->equals(123);

$sql = $update->sql('mysql');
```

### DELETE Operations

```php
use JardisCore\DbQuery\DbDelete;

$delete = new DbDelete();
$delete->from('users')
       ->where('status')->equals('deleted')
       ->and('created_at')->lower('2020-01-01');

$sql = $delete->sql('mysql');
```

### CRUD Shortcuts with DbPersist

For simple CRUD operations based on primary keys, use the `DbPersist` class for convenient shortcuts:

```php
use JardisCore\DbQuery\DbPersist;

$persist = new DbPersist();

// INSERT with auto-increment primary key
$query = $persist->insert(
    table: 'users',
    data: ['name' => 'John Doe', 'email' => 'john@example.com'],
    primaryKey: 'id',
    autoIncrement: true,
    dialect: 'mysql'
);

// Execute the query
$stmt = $pdo->prepare($query->sql());
$stmt->execute($query->bindings());
$lastInsertId = $pdo->lastInsertId();

// UPDATE by primary key
$query = $persist->update(
    table: 'users',
    data: ['name' => 'Jane Updated', 'status' => 'active'],
    primaryKey: 'id',
    primaryValue: 42,
    dialect: 'mysql'
);

$stmt = $pdo->prepare($query->sql());
$stmt->execute($query->bindings());

// DELETE by primary key
$query = $persist->delete(
    table: 'users',
    primaryKey: 'id',
    primaryValue: 42,
    dialect: 'mysql'
);

$stmt = $pdo->prepare($query->sql());
$stmt->execute($query->bindings());
```

**DbPersist Features:**
- Automatic primary key handling (removes auto-increment keys from INSERT)
- Prepared statements for all operations
- Works across all supported dialects (MySQL, PostgreSQL, SQLite)
- Simple API for common single-record operations
- Type-safe with full validation

## Advanced Examples

### Complex WHERE Conditions with Brackets

```php
$query = new DbQuery();
$query->select('*')
      ->from('products')
      ->where('category')->equals('electronics')
      ->and('price')->between(100, 500)
      ->and('brand', '(')->equals('Sony')        // Opening bracket in $openBracket parameter
          ->or('brand')->equals('Samsung', ')'); // Closing bracket in $closeBracket parameter

// Generates: WHERE category = ? AND price BETWEEN ? AND ? AND (brand = ? OR brand = ?)
```

### JSON Operations

DbQuery provides database-agnostic JSON operations that work across MySQL, PostgreSQL, and SQLite:

```php
// Basic JSON extraction
$query = new DbQuery();
$query->select('*')
      ->from('users')
      ->whereJson('settings')->extract('$.theme')->equals('dark')
      ->andJson('metadata')->extract('$.age')->greater(25);

// JSON array operations
$query = new DbQuery();
$query->select('*')
      ->from('items')
      ->whereJson('tags')->contains('php')            // Array contains value
      ->andJson('data')->length()->greaterEquals(3);  // Array length check

// JSON with NULL checks
$query = new DbQuery();
$query->select('*')
      ->from('users')
      ->whereJson('settings')->extract('$.active')->isNotNull();

// Generates dialect-specific JSON SQL automatically
$sql = $query->sql('postgres'); // Uses -> operator
$sql = $query->sql('mysql');    // Uses JSON_EXTRACT()
```

### JOINs

```php
$query = new DbQuery();
$query->select('u.name, o.total, a.city')
      ->from('users', 'u')
      ->innerJoin('orders', 'u.id = o.user_id', 'o')
      ->leftJoin('addresses', 'u.id = a.user_id', 'a')
      ->where('o.status')->equals('completed');

// All join types supported: innerJoin, leftJoin, rightJoin, fullJoin, crossJoin
```

### Subqueries

```php
// Subquery in WHERE clause
$subquery = new DbQuery();
$subquery->select('user_id')->from('orders')->where('total')->greater(1000);

$query = new DbQuery();
$query->select('*')
      ->from('users')
      ->where('id')->in($subquery);

// Subquery in FROM clause
$query = new DbQuery();
$query->select('*')
      ->from($subquery, 'high_value_orders')
      ->where('status')->equals('active');
```

### Window Functions

```php
// ROW_NUMBER() with partitioning and ordering
$query = new DbQuery();
$query->selectWindow('ROW_NUMBER()', 'row_num')
      ->partitionBy('department')
      ->windowOrderBy('salary', 'DESC')
      ->select('name, department, salary')
      ->from('employees');

// Named window specifications
$query = new DbQuery();
$query->select('name, salary')
      ->selectWindowRef('ROW_NUMBER()', 'w', 'row_num')
      ->window('w')
        ->partitionBy('department')
        ->windowOrderBy('salary', 'DESC')
      ->from('employees');
```

### Common Table Expressions (CTEs)

```php
// Simple CTE
$cte = new DbQuery();
$cte->select('department, AVG(salary) as avg_salary')
    ->from('employees')
    ->groupBy('department');

$query = new DbQuery();
$query->with('dept_avg', $cte)
      ->select('e.name, e.salary, d.avg_salary')
      ->from('employees', 'e')
      ->innerJoin('dept_avg', 'e.department = d.department', 'd');

// Recursive CTE (e.g., for hierarchical data)
$recursiveCte = new DbQuery();
$recursiveCte->select('id, name, parent_id, 1 as level')
             ->from('categories')
             ->where('parent_id')->isNull()
             ->unionAll(/* recursive part */);

$query = new DbQuery();
$query->withRecursive('category_tree', $recursiveCte)
      ->select('*')
      ->from('category_tree');
```

### INSERT with Conflict Handling

```php
// MySQL: ON DUPLICATE KEY UPDATE
$insert = new DbInsert();
$insert->into('users')
       ->set(['id' => 1, 'name' => 'John', 'email' => 'john@example.com'])
       ->onDuplicateKeyUpdate('name', 'John Updated')
       ->onDuplicateKeyUpdate('email', 'john.updated@example.com');

$sql = $insert->sql('mysql');

// PostgreSQL: ON CONFLICT
$insert = new DbInsert();
$insert->into('users')
       ->set(['id' => 1, 'name' => 'John', 'email' => 'john@example.com'])
       ->onConflict('email')
       ->doUpdate(['name' => 'John Updated']);

$sql = $insert->sql('postgres');

// SQLite: OR IGNORE / REPLACE
$insert = new DbInsert();
$insert->into('users')
       ->set(['id' => 1, 'name' => 'John'])
       ->orIgnore();

$sql = $insert->sql('sqlite');
```

## Development

All development work is done inside Docker containers. **Never run composer or vendor/bin commands directly on the host.** Always use `make` targets.

### Setup

```bash
# Start database containers (REQUIRED before any other commands)
make start

# Install dependencies
make install

# Stop and remove all containers
make stop
```

### Running Tests

```bash
# Run all tests
make phpunit

# Run unit tests only (no database required)
make phpunit-unit

# Run integration tests only (tests against MySQL, MariaDB, PostgreSQL)
make phpunit-integration

# Run tests with coverage report
make phpunit-coverage

# Run specific test file
docker compose run --rm phpcli vendor/bin/phpunit --bootstrap ./tests/bootstrap.php tests/unit/DbQueryTest.php

# Run specific test method
docker compose run --rm phpcli vendor/bin/phpunit --bootstrap ./tests/bootstrap.php --filter testMethodName
```

### Code Quality

```bash
# Static analysis (PHPStan Level 8)
make phpstan

# Code style checks (PSR-12)
make phpcs

# Open shell in PHP container
make shell
```

### Docker Compose Configuration

The Makefile references `support/docker-compose.yml` which provides:
- `phpcli` service (PHP 8.3 with Xdebug support)
- `mysql` service (port 3396)
- `mariadb` service (port 3397)
- `postgres` service (port 5499)

All services use tmpfs for database storage (data doesn't persist between runs).

## Architecture

### Builder Pattern with State Separation

The library separates query building from SQL generation using a state-based pattern:

```
Builder Classes → State Objects → SQL Generators → SQL Output
```

**Query Building Flow**:
```
User Code → Builder Methods → State Updates → .sql($dialect) → Factory → SQL Generator → SQL String/PreparedQuery
```

Example:
```php
$query = new DbQuery();
$query->select('id, name')
      ->from('users')
      ->where('age')->greater(18);

// Generates dialect-specific SQL:
$sql = $query->sql('mysql', prepared: true);
// Returns DbPreparedQuery with SQL and bindings

// DbPreparedQuery provides:
$sql->sql();       // string: SQL with placeholders
$sql->bindings();  // array: Values for placeholders
$sql->type();      // string: Query type ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
```

### Core Components

1. **Builders** (`DbQuery`, `DbInsert`, `DbUpdate`, `DbDelete`): Fluent interface that users interact with
2. **DbPersist**: Convenience wrapper for simple CRUD operations based on primary keys
3. **State Objects** (`QueryState`, `InsertState`, `UpdateState`, `DeleteState`): Store all query configuration
4. **SQL Generators**: Database-specific classes that read state and generate SQL
5. **Factory**: `SqlBuilderFactory` creates the appropriate SQL generator based on dialect

### Directory Structure

```
src/
├── DbQuery.php          # SELECT Query Builder
├── DbInsert.php         # INSERT Builder
├── DbUpdate.php         # UPDATE Builder
├── DbDelete.php         # DELETE Builder
├── DbPersist.php        # CRUD Shortcuts for Primary Key Operations
├── data/                # State Objects & Data Structures
│   ├── QueryState.php
│   ├── InsertState.php, UpdateState.php, DeleteState.php
│   ├── QueryConditionCollector.php  # WHERE/HAVING conditions
│   ├── Expression.php               # Raw SQL expressions
│   ├── WindowFunction.php, WindowSpec.php, WindowReference.php
│   ├── DbPreparedQuery.php          # Prepared query result
│   ├── Dialect.php                  # Enum for database dialects
│   └── contract/                    # State interfaces
├── query/               # SELECT SQL Generators
│   ├── SqlBuilder.php   # Base SELECT builder
│   ├── MySql.php, PostgresSql.php, SqliteSql.php  # Dialect implementations
│   ├── builder/method/  # Builder methods (Where, Join, OrderBy, etc.)
│   ├── builder/clause/  # SQL clause builders (SELECT, FROM, JOIN, etc.)
│   ├── builder/condition/  # Condition operators (Equals, GreaterThan, etc.)
│   ├── builder/window/  # Window function builders
│   ├── condition/       # QueryCondition and QueryJsonCondition
│   ├── validator/       # QueryBracketValidator, SqlInjectionValidator
│   ├── formatter/       # ValueFormatter, PlaceholderReplacer
│   └── processor/       # JsonPlaceholderProcessor
├── command/             # INSERT/UPDATE/DELETE Generators
│   ├── insert/          # InsertSqlBuilder + dialect implementations
│   │   └── method/      # Conflict handling (OnConflict, DoUpdate, etc.)
│   ├── update/          # UpdateSqlBuilder + dialect implementations
│   │   └── method/      # Update modifiers (Ignore, etc.)
│   └── delete/          # DeleteSqlBuilder + dialect implementations
└── factory/             # Factories and Registries
    ├── SqlBuilderFactory.php  # Creates dialect-specific builders
    └── BuilderRegistry.php    # Singleton registry for builders
```

### Key Design Patterns

**BuilderRegistry Pattern**: A critical singleton pattern used throughout the codebase:
- Caches reusable stateless builder instances
- Accessed via `BuilderRegistry::get(ClassName::class)`
- Prevents duplicate instantiation of helper classes
- Supports version-aware builder resolution for forward compatibility
- **Important for testing**: Call `BuilderRegistry::clear()` in setUp/tearDown

Version-aware resolution pattern:
- Base class: `namespace\method\FullJoin`
- Version override: `namespace\method\mysql\v84\FullJoin`
- Set via: `BuilderRegistry::setContext('mysql', '8.4')`
- Used by SqlBuilderFactory for dialect/version specific implementations

**Method Delegation Pattern**: Complex builder methods delegate to dedicated classes in `src/query/builder/method/`:

```php
// DbQuery::where() delegates to:
BuilderRegistry::get(method\Where::class)($collector, $queryCondition, $field, $openBracket);
```

This keeps builder classes focused on the fluent interface while delegating implementation details.

## Dialect Support

The library uses a `Dialect` enum (`src/data/Dialect.php`) for type-safe dialect handling:

- **MySQL** (`Dialect::MySQL`) - Default version: 8.0
- **MariaDB** (`Dialect::MariaDB`) - Default version: 10.11
- **PostgreSQL** (`Dialect::PostgreSQL`) - Default version: 14
- **SQLite** (`Dialect::SQLite`) - Default version: 3.35

String-based dialect arguments are accepted and converted via `Dialect::tryFromString()`.

### Key Dialect Differences Handled

- **JSON path syntax**: MySQL uses `JSON_EXTRACT('$.path')`, PostgreSQL uses `->` / `->>` operators
- **INSERT conflict handling**: MySQL `ON DUPLICATE KEY UPDATE`, PostgreSQL `ON CONFLICT`, SQLite `OR IGNORE`/`REPLACE`
- **Window functions**: Dialect-specific implementations and availability by version
- **LIMIT/OFFSET syntax**: Variations between databases
- **Data type conversions**: Database-specific type handling
- **Function name mappings**: e.g., `CONCAT` vs `||` for string concatenation

When adding features, always test against all three dialects using the integration tests.

## Available Condition Operators

- `equals()`, `notEquals()`
- `greater()`, `greaterEquals()`
- `lower()`, `lowerEquals()`
- `between()`, `notBetween()`
- `in()`, `notIn()`
- `like()`, `notLike()`
- `isNull()`, `isNotNull()`
- `exists()`, `notExists()`

## Code Quality Standards

- **PHP 8.2+** with strict types: `declare(strict_types=1)` required in all files
- **PSR-12** coding standard (enforced via PHPCS)
- **PHPStan Level 8** static analysis (strictest level)
- **100% test coverage** target
- All public methods require PHPDoc with `@param` and `@return` tags
- Maximum line length: 120 characters (absolute max: 150)

### Type Safety

- All parameters and return types must be explicitly typed
- Use PHPDoc for complex array types: `@var array<int, string>`
- Leverage PHP 8.2+ features: enums, match expressions, typed properties, readonly properties

### Testing

Two test suites:
- **Unit tests** (`tests/unit/`): Test classes in isolation, no database required
- **Integration tests** (`tests/integration/`): Test against real databases (MySQL, MariaDB, PostgreSQL)

Test structure mirrors `src/` directory.

**Important**: Call `BuilderRegistry::clear()` in test setUp/tearDown to reset singleton state.

---

## Licensing

**Noncommercial**: Licensed under **PolyForm Noncommercial License 1.0.0** for personal use, research, education, nonprofits, and open source projects.

**Commercial**: For commercial use, contact **jardiscore@headgent.dev** for a license with priority support and custom features.

---

## Support

- **Issues**: [GitHub Issues](https://github.com/jardiscore/DbQuery/issues)
- **Email**: jardiscore@headgent.dev
- **Enterprise Support**: Priority bug fixes, custom dialects, migration assistance
