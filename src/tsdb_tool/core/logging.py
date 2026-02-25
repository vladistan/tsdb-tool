"""Logging configuration using structlog.

Logs go to stderr to keep stdout clean for data output (piping).
"""

import logging
import sys
from typing import Any

import structlog

_LOG_LEVELS: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


class _LazyStderrFactory:
    """Resolve sys.stderr at logger creation time, not at configure() time.

    PrintLoggerFactory(file=sys.stderr) captures the file handle once.
    Under CliRunner tests the captured handle becomes stale when stderr
    is closed between invocations.  This factory defers the lookup so
    each logger gets the *current* sys.stderr.
    """

    def __call__(self, *args: Any, **kwargs: Any) -> structlog.PrintLogger:
        return structlog.PrintLogger(file=sys.stderr)


def setup_logging(verbose: bool = False) -> None:
    """Configure structlog for SQL Tool.

    Args:
        verbose: If True, set log level to DEBUG. Otherwise INFO.
    """
    log_level = "debug" if verbose else "info"

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty()),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(_LOG_LEVELS[log_level]),
        context_class=dict,
        logger_factory=_LazyStderrFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str | None = None) -> Any:
    """Get a structlog logger, optionally bound with a name.

    IMPORTANT: Never call this at module level. Always call inside
    functions or __init__() after setup_logging() has been called.
    """
    logger = structlog.get_logger()
    if name:
        logger = logger.bind(logger=name)
    return logger
