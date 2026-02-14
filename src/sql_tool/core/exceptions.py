"""Exception hierarchy for SQL Tool.

All exceptions carry an exit_code for CLI return value mapping.
Exit codes are defined in exit_codes.py and follow CODE-PY-CLI-001 pattern.
"""

from sql_tool.core.exit_codes import ExitCode


class SqlToolError(Exception):
    """Base exception for all SQL Tool errors."""

    exit_code: int = ExitCode.GENERAL_ERROR

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class NetworkError(SqlToolError):
    """Connection failures, unreachable host."""

    exit_code: int = ExitCode.NETWORK_ERROR


class TimeoutError(NetworkError):
    """Query timeout, connection timeout."""

    exit_code: int = ExitCode.TIMEOUT


class InputError(SqlToolError):
    """File not found, invalid parameters."""

    exit_code: int = ExitCode.INPUT_ERROR


class ConfigError(SqlToolError):
    """Malformed config, missing profile."""

    exit_code: int = ExitCode.CONFIG_ERROR
