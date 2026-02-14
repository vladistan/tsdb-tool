"""Sentry integration for error tracking and performance monitoring.

Sentry is initialized early in main() after logging setup.
"""

import sentry_sdk

from sql_tool.__about__ import __version__

_SENTRY_DSN = "https://36ad37dfb52a69bccdd8cf8d8940c747@o4508594232426496.ingest.us.sentry.io/4510874339966977"


def setup_sentry(environment: str = "local") -> None:
    """Initialize Sentry with public DSN for error tracking."""
    sentry_sdk.init(
        dsn=_SENTRY_DSN,
        traces_sample_rate=0.03,
        environment=environment,
        release=__version__,
        attach_stacktrace=True,
        send_default_pii=False,
    )
