"""Tests for package structure and imports (Step 1.1)."""

import pytest


@pytest.mark.unit
def test_package_imports():
    """Package imports without errors."""
    import sql_tool

    assert sql_tool is not None


@pytest.mark.unit
def test_version_accessible():
    """Version is accessible from package."""
    from sql_tool import __version__

    assert __version__ is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


@pytest.mark.unit
def test_version_format():
    """Version follows semver format."""
    from sql_tool import __version__

    parts = __version__.split(".")
    assert len(parts) == 3
    for part in parts:
        assert part.isdigit()


@pytest.mark.unit
def test_version_value():
    """Version matches expected initial value."""
    from sql_tool import __version__

    assert __version__ == "0.1.0"
