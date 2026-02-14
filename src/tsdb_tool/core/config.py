"""Configuration management for SQL Tool.

Handles TOML config files, environment variables, named profiles,
and configuration precedence resolution.

Precedence order (highest to lowest):
1. CLI flags (--host, --port, etc.)
2. --dsn flag (parsed into components)
3. Environment variables (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
4. Named profile (--profile or SQL_PROFILE env var)
5. Config file defaults
6. Built-in defaults
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from pydantic import BaseModel, computed_field, field_validator, model_validator

from tsdb_tool.core.exceptions import ConfigError

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "tsdb-tool" / "config.toml"

_PG_ENV_VARS: dict[str, str] = {
    "PGHOST": "host",
    "PGPORT": "port",
    "PGDATABASE": "dbname",
    "PGUSER": "user",
    "PGPASSWORD": "password",  # pragma: allowlist secret
}

_PROFILE_DEFAULTS: dict[str, Any] = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": None,
    "password": None,
    "sslmode": "prefer",
    "connect_timeout": 10,
    "application_name": "tsdb-tool",
}


def parse_dsn(dsn: str) -> dict[str, Any]:
    """Supports postgresql:// and postgres:// schemes with query params."""
    parsed = urlparse(dsn)
    if parsed.scheme not in ("postgresql", "postgres"):
        msg = f"Invalid DSN scheme: '{parsed.scheme}'. Expected 'postgresql' or 'postgres'"
        raise ConfigError(msg)

    result: dict[str, Any] = {}
    if parsed.hostname:
        result["host"] = parsed.hostname
    if parsed.port:
        result["port"] = parsed.port
    if parsed.path and parsed.path.strip("/"):
        result["dbname"] = parsed.path.strip("/")
    if parsed.username:
        result["user"] = parsed.username
    if parsed.password:
        result["password"] = parsed.password
    query_params = parse_qs(parsed.query)
    if "sslmode" in query_params:
        result["sslmode"] = query_params["sslmode"][0]
    if "connect_timeout" in query_params:
        result["connect_timeout"] = int(query_params["connect_timeout"][0])
    if "application_name" in query_params:
        result["application_name"] = query_params["application_name"][0]
    return result


class PgProfile(BaseModel):
    dsn: str | None = None
    host: str = "localhost"
    port: int = 5432
    dbname: str = "postgres"
    user: str | None = None
    password: str | None = None
    sslmode: str = "prefer"
    connect_timeout: int = 10
    application_name: str = "tsdb-tool"
    default_schema: str | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_dsn_into_components(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("dsn"):
            dsn_fields = parse_dsn(data["dsn"])
            for key, value in dsn_fields.items():
                if key not in data:
                    data[key] = value
        return data

    @field_validator("sslmode")
    @classmethod
    def validate_sslmode(cls, v: str) -> str:
        valid_modes = {
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        }
        if v not in valid_modes:
            msg = f"Invalid sslmode: '{v}'. Must be one of: {', '.join(sorted(valid_modes))}"
            raise ValueError(msg)
        return v

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            msg = f"Invalid port: {v}. Must be 1-65535"
            raise ValueError(msg)
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def connection_url(self) -> str:
        userinfo = ""
        if self.user:
            if self.password:
                userinfo = f"{self.user}:{self.password}@"
            else:
                userinfo = f"{self.user}@"
        return (
            f"postgresql://{userinfo}{self.host}:{self.port}"
            f"/{self.dbname}?sslmode={self.sslmode}"
        )


class AppConfig(BaseModel):
    default_timeout: float = 30.0
    default_format: str = "table"
    default_profile: str | None = None
    profiles: dict[str, PgProfile] = {}


class ResolvedConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    dbname: str = "postgres"
    user: str | None = None
    password: str | None = None
    sslmode: str = "prefer"
    connect_timeout: int = 10
    application_name: str = "tsdb-tool"
    default_timeout: float = 30.0
    default_format: str = "table"
    default_schema: str | None = None
    active_profile: str | None = None
    sources: dict[str, str] = {}


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load configuration from TOML file.

    Returns default AppConfig if file doesn't exist.
    Raises ConfigError on malformed TOML or invalid config.
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH

    if not config_path.exists():
        return AppConfig()

    try:
        with open(config_path, "rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        msg = f"Malformed TOML in {config_path}: {e}"
        raise ConfigError(msg) from e

    try:
        return AppConfig.model_validate(data)
    except Exception as e:
        msg = f"Invalid configuration in {config_path}: {e}"
        raise ConfigError(msg) from e


def resolve_config(
    config: AppConfig,
    profile_name: str | None = None,
    dsn: str | None = None,
    **cli_overrides: Any,
) -> ResolvedConfig:
    """Resolve configuration using precedence chain.

    CLI > DSN > env > profile > config defaults > built-in defaults.
    """
    sources: dict[str, str] = {}
    resolved: dict[str, Any] = {}

    # Layer 1: Built-in defaults
    resolved.update(_PROFILE_DEFAULTS)
    resolved["default_timeout"] = 30.0
    resolved["default_format"] = "table"
    for key in resolved:
        sources[key] = "default"

    # Layer 2: Config file global defaults
    if config.default_timeout != 30.0:
        resolved["default_timeout"] = config.default_timeout
        sources["default_timeout"] = "config"
    if config.default_format != "table":
        resolved["default_format"] = config.default_format
        sources["default_format"] = "config"

    # Layer 3: Named profile
    effective_profile = profile_name
    if not effective_profile:
        effective_profile = os.environ.get("SQL_PROFILE")
    if not effective_profile:
        effective_profile = config.default_profile

    if effective_profile:
        if effective_profile not in config.profiles:
            available = (
                ", ".join(sorted(config.profiles.keys())) if config.profiles else "none"
            )
            msg = f"Unknown profile: '{effective_profile}'. Available profiles: {available}"
            raise ConfigError(msg)
        profile = config.profiles[effective_profile]
        for key in profile.model_fields_set:
            if key in ("dsn",):
                continue
            if key in resolved:
                resolved[key] = getattr(profile, key)
                sources[key] = f"profile: {effective_profile}"

    # Layer 4: Environment variables
    for env_var, field_name in _PG_ENV_VARS.items():
        value = os.environ.get(env_var)
        if value is not None:
            if field_name == "port":
                try:
                    resolved[field_name] = int(value)
                except ValueError:
                    msg = f"Invalid {env_var} value: '{value}'. Must be an integer"
                    raise ConfigError(msg) from None
            else:
                resolved[field_name] = value
            sources[field_name] = f"env: {env_var}"

    # Layer 5: DSN flag
    if dsn:
        dsn_fields = parse_dsn(dsn)
        for key, value in dsn_fields.items():
            if key in resolved:
                resolved[key] = value
                sources[key] = "dsn"

    # Layer 6: CLI flags (highest priority)
    cli_to_field = {
        "host": "host",
        "port": "port",
        "database": "dbname",
        "user": "user",
        "password": "password",  # pragma: allowlist secret
        "sslmode": "sslmode",
        "timeout": "default_timeout",
        "schema": "default_schema",
    }
    for cli_name, field_name in cli_to_field.items():
        value = cli_overrides.get(cli_name)
        if value is not None:
            resolved[field_name] = value
            sources[field_name] = f"cli: --{cli_name}"

    resolved["active_profile"] = effective_profile
    resolved["sources"] = sources
    return ResolvedConfig(**resolved)
