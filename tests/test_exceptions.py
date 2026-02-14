"""Tests for exception hierarchy (Step 1.2)."""

import pytest

from tsdb_tool.core.exceptions import (
    ConfigError,
    InputError,
    NetworkError,
    SqlToolError,
    TimeoutError,
)
from tsdb_tool.core.exit_codes import ExitCode


@pytest.mark.unit
class TestExitCodes:
    def test_exit_code_values(self):
        assert ExitCode.SUCCESS == 0
        assert ExitCode.GENERAL_ERROR == 1
        assert ExitCode.USAGE_ERROR == 2
        assert ExitCode.INPUT_ERROR == 3
        assert ExitCode.OUTPUT_ERROR == 4
        assert ExitCode.NETWORK_ERROR == 5
        assert ExitCode.TIMEOUT == 6
        assert ExitCode.CONFIG_ERROR == 7

    def test_exit_code_is_int(self):
        for code in ExitCode:
            assert isinstance(code, int)


@pytest.mark.unit
class TestSqlToolError:
    def test_base_exception(self):
        err = SqlToolError("test error")
        assert str(err) == "test error"
        assert err.message == "test error"
        assert err.exit_code == ExitCode.GENERAL_ERROR

    def test_is_exception(self):
        assert issubclass(SqlToolError, Exception)


@pytest.mark.unit
class TestNetworkError:
    def test_exit_code(self):
        err = NetworkError("connection failed")
        assert err.exit_code == ExitCode.NETWORK_ERROR

    def test_inherits_from_base(self):
        err = NetworkError("connection failed")
        assert isinstance(err, SqlToolError)

    def test_message(self):
        err = NetworkError("host unreachable")
        assert err.message == "host unreachable"


@pytest.mark.unit
class TestTimeoutError:
    def test_exit_code(self):
        err = TimeoutError("query timed out")
        assert err.exit_code == ExitCode.TIMEOUT

    def test_inherits_from_network(self):
        err = TimeoutError("query timed out")
        assert isinstance(err, NetworkError)
        assert isinstance(err, SqlToolError)


@pytest.mark.unit
class TestInputError:
    def test_exit_code(self):
        err = InputError("file not found")
        assert err.exit_code == ExitCode.INPUT_ERROR

    def test_inherits_from_base(self):
        err = InputError("file not found")
        assert isinstance(err, SqlToolError)


@pytest.mark.unit
class TestConfigError:
    def test_exit_code(self):
        err = ConfigError("missing profile")
        assert err.exit_code == ExitCode.CONFIG_ERROR

    def test_inherits_from_base(self):
        err = ConfigError("missing profile")
        assert isinstance(err, SqlToolError)


@pytest.mark.unit
class TestExceptionCatching:
    def test_catch_all_by_base(self):
        """All specific exceptions are caught by SqlToolError."""
        for exc_class in [NetworkError, TimeoutError, InputError, ConfigError]:
            with pytest.raises(SqlToolError):
                raise exc_class("test")

    def test_timeout_caught_by_network(self):
        """TimeoutError is caught by NetworkError handler."""
        with pytest.raises(NetworkError):
            raise TimeoutError("timeout")
