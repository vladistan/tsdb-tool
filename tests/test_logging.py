"""Tests for logging setup (Step 1.3)."""

import pytest

from tsdb_tool.core.logging import get_logger, setup_logging


@pytest.mark.unit
class TestSetupLogging:
    def test_setup_without_errors(self):
        """Logger initializes without errors."""
        setup_logging()

    def test_setup_verbose(self):
        """Logger initializes with verbose flag."""
        setup_logging(verbose=True)

    def test_setup_not_verbose(self):
        """Logger initializes with default (non-verbose)."""
        setup_logging(verbose=False)


@pytest.mark.unit
class TestGetLogger:
    def test_get_logger_without_name(self):
        """Get logger without binding a name."""
        setup_logging()
        log = get_logger()
        assert log is not None

    def test_get_logger_with_name(self):
        """Get logger with a bound name."""
        setup_logging()
        log = get_logger("test_module")
        assert log is not None


@pytest.mark.unit
class TestLogOutput:
    def test_log_to_stderr(self, capsys):
        """Log output goes to stderr, not stdout."""
        setup_logging(verbose=True)
        log = get_logger()
        log.info("test message")

        captured = capsys.readouterr()
        assert captured.out == ""
