"""Shared CLI formatting helpers."""

from __future__ import annotations

import re


def normalize_pg_interval(pg_interval: str | None) -> str:
    """Convert PostgreSQL interval strings to human-readable form.

    Examples: "01:00:00" → "1 hour", "00:10:00" → "10 minutes",
    "24:00:00" → "1 day", "1 mon" → "1 month".
    Already-readable strings like "7 days" pass through unchanged.
    """
    if not pg_interval:
        return "-"

    val = pg_interval.strip()

    # Handle HH:MM:SS format
    match = re.match(r"^(\d+):(\d{2}):(\d{2})$", val)
    if match:
        hours, minutes, seconds = (
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3)),
        )
        total_seconds = hours * 3600 + minutes * 60 + seconds
        if total_seconds == 0:
            return "0 seconds"
        parts = []
        days, remainder = divmod(total_seconds, 86400)
        if days:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        hrs, remainder = divmod(remainder, 3600)
        if hrs:
            parts.append(f"{hrs} hour{'s' if hrs != 1 else ''}")
        mins, secs = divmod(remainder, 60)
        if mins:
            parts.append(f"{mins} minute{'s' if mins != 1 else ''}")
        if secs:
            parts.append(f"{secs} second{'s' if secs != 1 else ''}")
        return " ".join(parts)

    # Normalize "mon" → "month"
    val = re.sub(r"\bmon\b", "month", val)
    val = re.sub(r"\bmons\b", "months", val)

    return val


def format_duration_human(seconds: float | None) -> str:
    if seconds is None:
        return ""

    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes}m"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}h"
    else:
        days = int(seconds / 86400)
        return f"{days}d"


def format_relative_time(seconds: float | None) -> str:
    if seconds is None:
        return ""
    return f"{format_duration_human(seconds)} ago"


def fmt_size(b: int | None) -> str:
    """Format bytes as human-readable size for table output."""
    if not b:
        return "-"
    units = [("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)]
    for suffix, threshold in units:
        if b >= threshold:
            value = b / threshold
            return f"{value:.0f} {suffix}" if value >= 10 else f"{value:.1f} {suffix}"
    return f"{b}B"


def format_size_compact(size_bytes: int | None) -> str:
    """Format bytes as compact human-readable: 2KB, 300M, 1.0GB, 3TB."""
    if not size_bytes:
        return "0B"
    units = [("TB", 1 << 40), ("GB", 1 << 30), ("M", 1 << 20), ("KB", 1 << 10)]
    for suffix, threshold in units:
        if size_bytes >= threshold:
            value = size_bytes / threshold
            if value >= 10:
                return f"{value:.0f}{suffix}"
            return f"{value:.1f}{suffix}"
    return f"{size_bytes}B"


def format_size_gb(size_bytes: int | None) -> str:
    """Format bytes with adaptive units for CSV/JSON output."""
    if not size_bytes:
        return "0 bytes"
    units = [("TB", 1 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)]
    for suffix, threshold in units:
        if size_bytes >= threshold:
            return f"{size_bytes / threshold:.2f} {suffix}"
    return f"{size_bytes} bytes"


def format_timestamp(ts: str | None) -> str:
    """Strip microseconds and timezone from PostgreSQL timestamps."""
    if not ts or ts == "-infinity" or ts == "infinity":
        return "-"
    # Strip microseconds and timezone for readability
    if "." in ts:
        ts = ts.split(".")[0]
    elif "+" in ts:
        ts = ts.split("+")[0]
    return ts
