# Contributing to sql-tool

## Development Setup

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (package manager)
- PostgreSQL 15+ (for integration tests)
- TimescaleDB extension (optional, for TimescaleDB command tests)

### Install Dependencies

```bash
git clone https://github.com/vladistan/sql-tool.git
cd sql-tool
uv sync --all-extras
```

### Configure a Test Database Profile

Integration tests require a running PostgreSQL instance. Create a config file at `~/.config/sql-tool/config.toml`:

```toml
[profiles.test_db]
host = "localhost"
port = 5432
dbname = "postgres"
user = "postgres"
password = "your_password"
default_schema = "public"
```

See `config.example.toml` for more configuration options including DSN-style connections.

### Integration Test Environment Variables

Integration tests use environment variables to locate your test database objects. Set these to match your local setup:

```bash
export SQL_TOOL_TEST_PROFILE=test_db              # Config profile name
export SQL_TOOL_TEST_SCHEMA=public                 # Schema with test tables
export SQL_TOOL_TEST_TABLE=my_hypertable           # A TimescaleDB hypertable
export SQL_TOOL_TEST_DATABASE=postgres              # Database name
export SQL_TOOL_TEST_PLAIN_TABLE=some_regular_table # A non-hypertable table
```

If not set, tests use generic defaults (`myschema`, `sensor_data`, etc.) that you would need to create in your database.

## Development Workflow

### Running Tests

```bash
# All tests
uv run pytest

# Unit tests only (no database required)
uv run pytest -m unit

# Integration tests only (requires PostgreSQL)
uv run pytest -m integration

# Skip slow tests
uv run pytest -m "not slow"
```

### Linting

```bash
# Check for issues
uv run ruff check src/ tests/

# Auto-fix issues
uv run ruff check --fix src/ tests/

# Format code
uv run ruff format src/ tests/
```

### Type Checking

```bash
uv run mypy src/
```

### Pre-commit Hooks

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

## Code Style

- Line length: 88 characters
- Formatting: ruff (Black-compatible)
- Import sorting: ruff isort
- Type annotations: required for all public functions (mypy strict mode)
- Test coverage: minimum 80%

## Project Structure

```
src/sql_tool/
├── cli/           # CLI commands (typer)
│   ├── commands/  # Subcommand groups (pg, ts, query)
│   ├── helpers.py # CLI utilities
│   ├── main.py    # Entry point
│   └── output.py  # Output formatting
├── core/          # Core logic
│   ├── client.py  # PostgreSQL client
│   ├── config.py  # Configuration loading
│   ├── models.py  # Data models
│   ├── postgres.py      # PostgreSQL admin queries
│   └── timescaledb.py   # TimescaleDB admin queries
└── formatters/    # Output formatters (table, json, csv)

tests/
├── cli/           # CLI integration tests
├── fixtures/      # SQL fixtures
├── test_client.py
├── test_config.py
└── ...
```

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Make your changes
4. Run the full check suite: `uv run ruff check src/ tests/ && uv run mypy src/ && uv run pytest`
5. Commit with a descriptive message
6. Open a pull request
