# sql-tool

PostgreSQL and TimescaleDB query and administration CLI tool.

Provides multiple output formats (table, JSON, CSV), flexible connection management (CLI flags, DSN, PG* env vars, config profiles), and admin commands for database inspection and maintenance.

## Installation

```bash
git clone <repository-url>
cd sql-tool
uv sync
```

Run commands with `uv run sql-tool` or install globally:

```bash
uv tool install .
```

## Quick Start

```bash
# Check connectivity
sql-tool service check

# Run a query
sql-tool query -e "SELECT now()"

# List databases
sql-tool databases

# List tables in a schema
sql-tool -s myschema table

# Show table columns and preview data
sql-tool table myschema.sensor_data --head 5
```

## Commands

### Query Execution

```bash
# Inline query
sql-tool query -e "SELECT * FROM users LIMIT 10"

# From file
sql-tool query queries/report.sql

# From stdin
echo "SELECT 1" | sql-tool query

# With timeout
sql-tool query -e "SELECT pg_sleep(5)" -t 3
```

### Database Overview

```bash
# List all databases (sorted by size)
sql-tool databases

# List schemas with space usage
sql-tool schema

# Schemas across all databases
sql-tool schema --all-databases
```

### Table Inspection

```bash
# List all tables with sizes
sql-tool table

# Filter by schema
sql-tool -s myschema table

# Show column definitions
sql-tool table myschema.sensor_data

# Preview data
sql-tool table myschema.sensor_data --head 10
sql-tool table myschema.sensor_data --tail 5
sql-tool table myschema.sensor_data --sample 3

# Show timestamp range for hypertables
sql-tool table myschema.sensor_data --range
```

### Connections

```bash
# Active connections (non-idle)
sql-tool connections

# All connections including idle
sql-tool connections --all

# Connection summary with memory config
sql-tool connections --summary

# Filter by user, database, or state
sql-tool connections --filter-user appuser --filter-db mydb
sql-tool connections --min-duration 5
```

### Service & Maintenance

```bash
# Check server connectivity and version
sql-tool service check

# Vacuum a table
sql-tool service vacuum my_table
sql-tool service vacuum my_table --full
sql-tool service vacuum --all

# Kill a backend process
sql-tool service kill 12345
sql-tool service kill 12345 --cancel  # cancel query only
```

### TimescaleDB

```bash
# List hypertables
sql-tool ts hypertables

# Show chunks for a hypertable
sql-tool ts chunks myschema.sensor_data

# Compression statistics
sql-tool ts compression
sql-tool ts compression myschema.sensor_data

# Compression settings and policies
sql-tool ts compression-settings -s myschema

# Configure compression
sql-tool ts compression-set myschema.sensor_data --segmentby "device_id, sensor" --orderby "timestamp DESC"
sql-tool ts compression-set myschema.sensor_data --policy "4 hours"

# Compress chunks
sql-tool ts compress myschema.sensor_data
sql-tool ts compress myschema.sensor_data --chunk 11420

# Continuous aggregates
sql-tool ts caggs

# Retention policies
sql-tool ts retention

# Background jobs
sql-tool ts jobs
sql-tool ts jobs --history
sql-tool ts jobs --history --job 1005
```

### Configuration

```bash
# Show resolved config with source attribution
sql-tool config show

# List available profiles
sql-tool config profiles
```

## Output Formats

All listing commands support multiple output formats:

```bash
# Table format (default in terminal)
sql-tool -f table databases

# JSON format
sql-tool -f json databases

# Compact JSON
sql-tool --compact -f json databases

# CSV format (default when piped)
sql-tool databases | head

# CSV without header
sql-tool --no-header databases
```

Auto-detection: table format for TTY, CSV when piped.

## Connection Options

Global flags (must appear before the subcommand):

```bash
sql-tool -H myhost -p 5432 -d mydb -U myuser query -e "SELECT 1"
sql-tool --dsn "postgresql://user@host/db" databases
sql-tool -P production table
```

| Flag | Env Var | Description |
|------|---------|-------------|
| `-H, --host` | `PGHOST` | PostgreSQL host |
| `-p, --port` | `PGPORT` | PostgreSQL port |
| `-d, --database` | `PGDATABASE` | Database name |
| `-U, --user` | `PGUSER` | User name |
| `-W, --password` | `PGPASSWORD` | Password |
| `--dsn` | | Connection DSN |
| `-P, --profile` | `SQL_PROFILE` | Named profile |

## Configuration File

Location: `~/.config/sql-tool/config.toml`

```toml
default_timeout = 30
default_format = "table"

[profiles.local]
host = "localhost"
port = 5432
dbname = "postgres"
user = "postgres"

[profiles.production]
dsn = "postgresql://readonly@prod-db.example.com:5432/myapp?sslmode=verify-full"

[profiles.staging]
dsn = "postgresql://user@staging-db.example.com/myapp"
sslmode = "verify-full"
connect_timeout = 5
```

See `config.example.toml` for a full example.

### Precedence (highest to lowest)

1. CLI flags (`--host`, `--port`, etc.)
2. `--dsn` flag
3. Environment variables (`PGHOST`, `PGPORT`, etc.)
4. Named profile (`--profile` or `SQL_PROFILE`)
5. Config file defaults
6. Built-in defaults (`localhost:5432/postgres`)

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (SQL syntax, permission denied) |
| 2 | Usage error (invalid arguments) |
| 3 | Input error (file not found, invalid parameters) |
| 5 | Network error (connection failed, authentication) |
| 6 | Timeout (query timeout, connection timeout) |
| 7 | Configuration error (malformed config, unknown profile) |

## Development

```bash
# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=sql_tool

# Lint
uv run ruff check src/

# Type check
uv run mypy src/
```
