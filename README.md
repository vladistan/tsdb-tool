# tsdb-tool

PostgreSQL and TimescaleDB query and administration CLI tool.

Provides multiple output formats (table, JSON, CSV), flexible connection management (CLI flags, DSN, PG* env vars, config profiles), and admin commands for database inspection and maintenance.

## Installation

```bash
git clone <repository-url>
cd tsdb-tool
uv sync
```

Run commands with `uv run tsdb-tool` or install globally:

```bash
uv tool install .
```

## Quick Start

```bash
# Check connectivity
tsdb-tool service check

# Run a query
tsdb-tool query -e "SELECT now()"

# List databases
tsdb-tool databases

# List tables in a schema
tsdb-tool -s myschema table

# Show table columns and preview data
tsdb-tool table myschema.sensor_data --head 5
```

## Commands

### Query Execution

```bash
# Inline query
tsdb-tool query -e "SELECT * FROM users LIMIT 10"

# From file
tsdb-tool query queries/report.sql

# From stdin
echo "SELECT 1" | tsdb-tool query

# With timeout
tsdb-tool query -e "SELECT pg_sleep(5)" -t 3
```

### Database Overview

```bash
# List all databases (sorted by size)
tsdb-tool databases

# List schemas with space usage
tsdb-tool schema

# Schemas across all databases
tsdb-tool schema --all-databases
```

### Table Inspection

```bash
# List all tables with sizes
tsdb-tool table

# Filter by schema
tsdb-tool -s myschema table

# Show column definitions
tsdb-tool table myschema.sensor_data

# Preview data
tsdb-tool table myschema.sensor_data --head 10
tsdb-tool table myschema.sensor_data --tail 5
tsdb-tool table myschema.sensor_data --sample 3

# Show timestamp range for hypertables
tsdb-tool table myschema.sensor_data --range
```

### Connections

```bash
# Active connections (non-idle)
tsdb-tool connections

# All connections including idle
tsdb-tool connections --all

# Connection summary with memory config
tsdb-tool connections --summary

# Filter by user, database, or state
tsdb-tool connections --filter-user appuser --filter-db mydb
tsdb-tool connections --min-duration 5
```

### Service & Maintenance

```bash
# Check server connectivity and version
tsdb-tool service check

# Vacuum a table
tsdb-tool service vacuum my_table
tsdb-tool service vacuum my_table --full
tsdb-tool service vacuum --all

# Kill a backend process
tsdb-tool service kill 12345
tsdb-tool service kill 12345 --cancel  # cancel query only
```

### TimescaleDB

```bash
# List hypertables
tsdb-tool ts hypertables

# Show chunks for a hypertable
tsdb-tool ts chunks myschema.sensor_data

# Compression statistics
tsdb-tool ts compression
tsdb-tool ts compression myschema.sensor_data

# Compression settings and policies
tsdb-tool ts compression-settings -s myschema

# Configure compression
tsdb-tool ts compression-set myschema.sensor_data --segmentby "device_id, sensor" --orderby "timestamp DESC"
tsdb-tool ts compression-set myschema.sensor_data --policy "4 hours"

# Compress chunks
tsdb-tool ts compress myschema.sensor_data
tsdb-tool ts compress myschema.sensor_data --chunk 11420

# Continuous aggregates
tsdb-tool ts caggs

# Retention policies
tsdb-tool ts retention

# Background jobs
tsdb-tool ts jobs
tsdb-tool ts jobs --history
tsdb-tool ts jobs --history --job 1005
```

### Configuration

```bash
# Show resolved config with source attribution
tsdb-tool config show

# List available profiles
tsdb-tool config profiles
```

## Output Formats

All listing commands support multiple output formats:

```bash
# Table format (default in terminal)
tsdb-tool -f table databases

# JSON format
tsdb-tool -f json databases

# Compact JSON
tsdb-tool --compact -f json databases

# CSV format (default when piped)
tsdb-tool databases | head

# CSV without header
tsdb-tool --no-header databases
```

Auto-detection: table format for TTY, CSV when piped.

## Connection Options

Global flags (must appear before the subcommand):

```bash
tsdb-tool -H myhost -p 5432 -d mydb -U myuser query -e "SELECT 1"
tsdb-tool --dsn "postgresql://user@host/db" databases
tsdb-tool -P production table
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

Location: `~/.config/tsdb-tool/config.toml`

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
uv run pytest --cov=tsdb_tool

# Lint
uv run ruff check src/

# Type check
uv run mypy src/
```
