"""Tests for CLI formatting helpers."""

from tsdb_tool.cli.helpers import (
    fmt_size,
    format_duration_human,
    format_relative_time,
    format_size_compact,
    format_size_gb,
    format_timestamp,
    normalize_pg_interval,
)

# -- format_duration_human --


def test_format_duration_human_none_returns_empty():
    assert format_duration_human(None) == ""


def test_format_duration_human_zero_seconds():
    assert format_duration_human(0) == "0s"


def test_format_duration_human_seconds_range():
    assert format_duration_human(30) == "30s"


def test_format_duration_human_just_under_minute():
    assert format_duration_human(59) == "59s"


def test_format_duration_human_one_minute():
    assert format_duration_human(60) == "1m"


def test_format_duration_human_minutes_range():
    assert format_duration_human(300) == "5m"


def test_format_duration_human_just_under_hour():
    assert format_duration_human(3599) == "59m"


def test_format_duration_human_one_hour():
    assert format_duration_human(3600) == "1h"


def test_format_duration_human_hours_range():
    assert format_duration_human(7200) == "2h"


def test_format_duration_human_just_under_day():
    assert format_duration_human(86399) == "23h"


def test_format_duration_human_one_day():
    assert format_duration_human(86400) == "1d"


def test_format_duration_human_multiple_days():
    assert format_duration_human(259200) == "3d"


def test_format_duration_human_fractional_seconds():
    assert format_duration_human(45.7) == "45s"


# -- format_relative_time --


def test_format_relative_time_none_returns_empty():
    assert format_relative_time(None) == ""


def test_format_relative_time_seconds_ago():
    assert format_relative_time(30) == "30s ago"


def test_format_relative_time_minutes_ago():
    assert format_relative_time(120) == "2m ago"


def test_format_relative_time_hours_ago():
    assert format_relative_time(7200) == "2h ago"


def test_format_relative_time_days_ago():
    assert format_relative_time(172800) == "2d ago"


# -- normalize_pg_interval --


def test_normalize_pg_interval_none_returns_dash():
    assert normalize_pg_interval(None) == "-"


def test_normalize_pg_interval_empty_string_returns_dash():
    assert normalize_pg_interval("") == "-"


def test_normalize_pg_interval_hms_one_hour():
    assert normalize_pg_interval("01:00:00") == "1 hour"


def test_normalize_pg_interval_hms_multiple_hours():
    assert normalize_pg_interval("04:00:00") == "4 hours"


def test_normalize_pg_interval_hms_ten_minutes():
    assert normalize_pg_interval("00:10:00") == "10 minutes"


def test_normalize_pg_interval_hms_one_day():
    assert normalize_pg_interval("24:00:00") == "1 day"


def test_normalize_pg_interval_hms_zero():
    assert normalize_pg_interval("00:00:00") == "0 seconds"


def test_normalize_pg_interval_hms_complex():
    assert normalize_pg_interval("25:30:45") == "1 day 1 hour 30 minutes 45 seconds"


def test_normalize_pg_interval_mon_normalized():
    assert normalize_pg_interval("1 mon") == "1 month"


def test_normalize_pg_interval_mons_normalized():
    assert normalize_pg_interval("3 mons") == "3 months"


def test_normalize_pg_interval_passthrough_already_readable():
    assert normalize_pg_interval("7 days") == "7 days"


# -- fmt_size --


def test_fmt_size_none_returns_dash():
    assert fmt_size(None) == "-"


def test_fmt_size_zero_returns_dash():
    assert fmt_size(0) == "-"


def test_fmt_size_bytes():
    assert fmt_size(512) == "512B"


def test_fmt_size_kilobytes():
    assert fmt_size(1024) == "1.0 KB"


def test_fmt_size_kilobytes_large():
    assert fmt_size(15360) == "15 KB"


def test_fmt_size_megabytes():
    assert fmt_size(1048576) == "1.0 MB"


def test_fmt_size_megabytes_large():
    assert fmt_size(52428800) == "50 MB"


def test_fmt_size_gigabytes():
    assert fmt_size(1073741824) == "1.0 GB"


def test_fmt_size_gigabytes_large():
    assert fmt_size(21474836480) == "20 GB"


def test_fmt_size_terabytes():
    assert fmt_size(1099511627776) == "1.0 TB"


# -- format_size_compact --


def test_format_size_compact_none_returns_zero():
    assert format_size_compact(None) == "0B"


def test_format_size_compact_zero_returns_zero():
    assert format_size_compact(0) == "0B"


def test_format_size_compact_small_bytes():
    assert format_size_compact(512) == "512B"


def test_format_size_compact_kilobytes():
    assert format_size_compact(1024) == "1.0KB"


def test_format_size_compact_kilobytes_large():
    assert format_size_compact(15360) == "15KB"


def test_format_size_compact_megabytes():
    assert format_size_compact(1048576) == "1.0M"


def test_format_size_compact_megabytes_large():
    assert format_size_compact(52428800) == "50M"


def test_format_size_compact_gigabytes():
    assert format_size_compact(1073741824) == "1.0GB"


def test_format_size_compact_gigabytes_large():
    assert format_size_compact(21474836480) == "20GB"


def test_format_size_compact_terabytes():
    assert format_size_compact(1099511627776) == "1.0TB"


# -- format_size_gb --


def test_format_size_gb_none_returns_zero_bytes():
    assert format_size_gb(None) == "0 bytes"


def test_format_size_gb_zero_returns_zero_bytes():
    assert format_size_gb(0) == "0 bytes"


def test_format_size_gb_small_bytes():
    assert format_size_gb(512) == "512 bytes"


def test_format_size_gb_kilobytes():
    assert format_size_gb(1024) == "1.00 KB"


def test_format_size_gb_megabytes():
    assert format_size_gb(1048576) == "1.00 MB"


def test_format_size_gb_gigabytes():
    assert format_size_gb(1073741824) == "1.00 GB"


def test_format_size_gb_gigabytes_fractional():
    assert format_size_gb(2684354560) == "2.50 GB"


def test_format_size_gb_terabytes():
    assert format_size_gb(1099511627776) == "1.00 TB"


# -- format_timestamp --


def test_format_timestamp_none_returns_dash():
    assert format_timestamp(None) == "-"


def test_format_timestamp_negative_infinity():
    assert format_timestamp("-infinity") == "-"


def test_format_timestamp_positive_infinity():
    assert format_timestamp("infinity") == "-"


def test_format_timestamp_strips_microseconds():
    assert format_timestamp("2026-02-14 10:30:00.123456+00") == "2026-02-14 10:30:00"


def test_format_timestamp_strips_timezone_only():
    assert format_timestamp("2026-02-14 10:30:00+00") == "2026-02-14 10:30:00"


def test_format_timestamp_passthrough_clean():
    assert format_timestamp("2026-02-14 10:30:00") == "2026-02-14 10:30:00"


def test_format_timestamp_empty_string_returns_dash():
    assert format_timestamp("") == "-"
