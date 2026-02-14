"""Query source resolution for SQL Tool.

Resolves the SQL query text from one of three sources:
1. Inline (-e flag)  — highest priority
2. File path         — middle priority
3. stdin             — lowest priority
"""

from __future__ import annotations

import sys
from pathlib import Path

from tsdb_tool.core.exceptions import InputError


def resolve_query_source(
    inline: str | None,
    file_path: str | None,
) -> str:
    """Resolve SQL query from inline, file, or stdin.

    Precedence: inline > file > stdin.
    Raises InputError when no source is available.
    """
    if inline is not None:
        return inline

    if file_path is not None:
        p = Path(file_path)
        if not p.exists():
            msg = (
                f"Query file not found: {file_path}\n"
                "Use -e for inline queries or pipe query via stdin."
            )
            raise InputError(msg)
        return p.read_text()

    if not sys.stdin.isatty():
        return sys.stdin.read()

    msg = "No query provided. Use -e, file path, or pipe to stdin."
    raise InputError(msg)
