"""Tests for package structure and imports (Step 1.1)."""

import pytest


@pytest.mark.unit
def test_package_imports():
    """Package imports without errors."""
    import tsdb_tool

    assert tsdb_tool is not None


@pytest.mark.unit
def test_version_accessible():
    """Version is accessible from package."""
    from tsdb_tool import __version__

    assert __version__ is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


@pytest.mark.unit
def test_version_format():
    """Version follows semver format."""
    from tsdb_tool import __version__

    parts = __version__.split(".")
    assert len(parts) == 3
    for part in parts:
        assert part.isdigit()


@pytest.mark.unit
def test_version_value():
    """Version matches expected initial value."""
    from tsdb_tool import __version__

    assert __version__ == "0.1.0"
