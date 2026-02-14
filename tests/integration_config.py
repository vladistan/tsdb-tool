"""Configuration for integration tests.

Override these values via environment variables to match your local database setup.
Defaults use generic names; set env vars to point to your actual test database.

Example:
    export SQL_TOOL_TEST_PROFILE=my_local_db
    export SQL_TOOL_TEST_SCHEMA=magnum
    export SQL_TOOL_TEST_TABLE=magnum_data
    export SQL_TOOL_TEST_DATABASE=piccolo_dev
    export SQL_TOOL_TEST_PLAIN_TABLE=flyway_schema_history
"""

import os

# Profile name configured in ~/.config/sql-tool/config.toml
TEST_PROFILE = os.environ.get("SQL_TOOL_TEST_PROFILE", "test_db")

# Schema containing the hypertable used for integration tests
TEST_SCHEMA = os.environ.get("SQL_TOOL_TEST_SCHEMA", "myschema")

# Hypertable name used for TimescaleDB integration tests
TEST_TABLE = os.environ.get("SQL_TOOL_TEST_TABLE", "sensor_data")

# Database name the test profile connects to
TEST_DATABASE = os.environ.get("SQL_TOOL_TEST_DATABASE", "mydb")

# A plain (non-hypertable) table for preview tests
TEST_PLAIN_TABLE = os.environ.get("SQL_TOOL_TEST_PLAIN_TABLE", "flyway_schema_history")

# Convenience: fully qualified table references
TEST_TABLE_REF = f"{TEST_SCHEMA}.{TEST_TABLE}"
TEST_PLAIN_TABLE_REF = f"{TEST_SCHEMA}.{TEST_PLAIN_TABLE}"

# CLI profile arguments
PROFILE_ARGS = ["--profile", TEST_PROFILE]
