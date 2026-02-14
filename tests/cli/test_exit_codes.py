"""Tests for exit code mapping through run() (Phase 3, Step 3.5)."""

from unittest.mock import patch

import pytest

from sql_tool.core.exceptions import (
    ConfigError,
    InputError,
    NetworkError,
    SqlToolError,
    TimeoutError,
)
from sql_tool.core.exit_codes import ExitCode

# -- Exception exit_code attributes --


@pytest.mark.unit
def test_sql_tool_error_general():
    e = SqlToolError("something failed")
    assert e.exit_code == ExitCode.GENERAL_ERROR


@pytest.mark.unit
def test_network_error_code():
    e = NetworkError("connection refused")
    assert e.exit_code == ExitCode.NETWORK_ERROR


@pytest.mark.unit
def test_timeout_error_code():
    e = TimeoutError("query timed out")
    assert e.exit_code == ExitCode.TIMEOUT


@pytest.mark.unit
def test_input_error_code():
    e = InputError("file not found")
    assert e.exit_code == ExitCode.INPUT_ERROR


@pytest.mark.unit
def test_config_error_code():
    e = ConfigError("bad config")
    assert e.exit_code == ExitCode.CONFIG_ERROR


# -- run() exit code mapping --


@pytest.mark.unit
def test_run_network_error_maps_to_exit_code():
    with patch("sql_tool.cli.main.app", side_effect=NetworkError("fail")):
        with pytest.raises(SystemExit) as exc_info:
            from sql_tool.cli.main import run

            run()
        assert exc_info.value.code == ExitCode.NETWORK_ERROR


@pytest.mark.unit
def test_run_input_error_maps_to_exit_code():
    with patch("sql_tool.cli.main.app", side_effect=InputError("bad input")):
        with pytest.raises(SystemExit) as exc_info:
            from sql_tool.cli.main import run

            run()
        assert exc_info.value.code == ExitCode.INPUT_ERROR


@pytest.mark.unit
def test_run_config_error_maps_to_exit_code():
    with patch("sql_tool.cli.main.app", side_effect=ConfigError("bad config")):
        with pytest.raises(SystemExit) as exc_info:
            from sql_tool.cli.main import run

            run()
        assert exc_info.value.code == ExitCode.CONFIG_ERROR


@pytest.mark.unit
def test_run_timeout_error_maps_to_exit_code():
    with patch("sql_tool.cli.main.app", side_effect=TimeoutError("timed out")):
        with pytest.raises(SystemExit) as exc_info:
            from sql_tool.cli.main import run

            run()
        assert exc_info.value.code == ExitCode.TIMEOUT


@pytest.mark.unit
def test_run_keyboard_interrupt_maps_to_130():
    with patch("sql_tool.cli.main.app", side_effect=KeyboardInterrupt):
        with pytest.raises(SystemExit) as exc_info:
            from sql_tool.cli.main import run

            run()
        assert exc_info.value.code == 130


@pytest.mark.unit
def test_run_unexpected_exception_maps_to_1():
    with patch("sql_tool.cli.main.app", side_effect=RuntimeError("boom")):
        with pytest.raises(SystemExit) as exc_info:
            from sql_tool.cli.main import run

            run()
        assert exc_info.value.code == 1


@pytest.mark.unit
def test_run_system_exit_passes_through():
    with patch("sql_tool.cli.main.app", side_effect=SystemExit(42)):
        with pytest.raises(SystemExit) as exc_info:
            from sql_tool.cli.main import run

            run()
        assert exc_info.value.code == 42
