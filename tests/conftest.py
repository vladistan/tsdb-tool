"""Shared test fixtures for SQL Tool."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from typer.testing import CliRunner

from sql_tool.cli.main import app


@pytest.fixture
def runner():
    """Typer CLI test runner."""
    return CliRunner()


@pytest.fixture
def cli_runner(runner):
    """Invoke the CLI app with the given arguments."""

    def invoke(*args: str, **kwargs):
        return runner.invoke(app, list(args), **kwargs)

    return invoke


@pytest.fixture
def temp_dir():
    """Temporary directory for test files."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)
