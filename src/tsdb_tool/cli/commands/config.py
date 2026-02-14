"""Configuration management CLI commands."""

from __future__ import annotations

from typing import TYPE_CHECKING

import typer

from tsdb_tool.core.config import DEFAULT_CONFIG_PATH, load_config, resolve_config

if TYPE_CHECKING:
    from pathlib import Path

    from tsdb_tool.core.config import ResolvedConfig

config_app = typer.Typer(help="Configuration management commands")


@config_app.callback(invoke_without_command=True)
def config_callback(ctx: typer.Context) -> None:
    if not ctx.invoked_subcommand:
        typer.echo(ctx.get_help())
        raise typer.Exit()


def _get_resolved_config(ctx: typer.Context) -> tuple[ResolvedConfig, Path | None]:
    config_path: Path | None = ctx.obj.get("config_file")
    app_config = load_config(config_path)
    resolved = resolve_config(
        app_config,
        profile_name=ctx.obj.get("profile"),
        dsn=ctx.obj.get("dsn"),
        host=ctx.obj.get("host"),
        port=ctx.obj.get("port"),
        database=ctx.obj.get("database"),
        user=ctx.obj.get("user"),
        password=ctx.obj.get("password"),
    )
    return resolved, config_path


def _mask_password(value: str | None) -> str:
    if value is None:
        return "not set"
    return "***"


@config_app.command("show")
def config_show(ctx: typer.Context) -> None:
    """Display resolved configuration with source attribution."""
    resolved, config_path = _get_resolved_config(ctx)
    sources = resolved.sources

    typer.echo("Connection Settings (resolved):")
    connection_fields = [
        ("host", resolved.host),
        ("port", str(resolved.port)),
        ("database", resolved.dbname),
        ("user", resolved.user or "not set"),
        ("password", _mask_password(resolved.password)),
        ("sslmode", resolved.sslmode),
    ]
    for field_name, value in connection_fields:
        source_key = "dbname" if field_name == "database" else field_name
        source = sources.get(source_key, "default")
        typer.echo(f"  {field_name}: {value} ({source})")

    typer.echo("")
    typer.echo("General:")
    timeout_source = sources.get("default_timeout", "default")
    typer.echo(f"  timeout: {resolved.default_timeout}s ({timeout_source})")
    format_source = sources.get("default_format", "default")
    typer.echo(f"  format: {resolved.default_format} ({format_source})")

    typer.echo("")
    if resolved.active_profile:
        typer.echo(f"Active Profile: {resolved.active_profile}")
    else:
        typer.echo("Active Profile: none")

    display_path = config_path or DEFAULT_CONFIG_PATH
    typer.echo(f"Config File: {display_path}")


@config_app.command("profiles")
def config_profiles(ctx: typer.Context) -> None:
    """List available connection profiles."""
    config_path: Path | None = ctx.obj.get("config_file")
    app_config = load_config(config_path)
    active_profile = ctx.obj.get("profile") or None

    if not app_config.profiles:
        typer.echo("No profiles configured.")
        display_path = config_path or DEFAULT_CONFIG_PATH
        typer.echo(f"Add profiles to: {display_path}")
        return

    typer.echo("Available Profiles:")
    typer.echo("")
    for name, profile in sorted(app_config.profiles.items()):
        is_active = name == active_profile
        marker = "* " if is_active else "  "
        label = " (active)" if is_active else ""
        typer.echo(f"{marker}{name}{label}")

        display_fields = [
            ("host", profile.host),
            ("port", str(profile.port)),
            ("database", profile.dbname),
        ]
        if profile.user:
            display_fields.append(("user", profile.user))
        if profile.sslmode != "prefer":
            display_fields.append(("sslmode", profile.sslmode))

        for field_name, value in display_fields:
            typer.echo(f"      {field_name}: {value}")
        typer.echo("")
