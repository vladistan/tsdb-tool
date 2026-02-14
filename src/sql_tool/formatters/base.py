"""Formatter protocol and registry for output formatting."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sql_tool.core.models import QueryResult


@runtime_checkable
class Formatter(Protocol):
    """Protocol for output formatters.

    Each formatter transforms a QueryResult into lines of formatted text.
    Yielding strings (rather than returning a single string) enables
    streaming output for large result sets without buffering everything
    in memory.
    """

    def format(self, result: QueryResult) -> Iterator[str]:
        """Transform a QueryResult into formatted output lines."""
        ...


class FormatterRegistry:
    """Registry for looking up formatters by name."""

    def __init__(self) -> None:
        self._formatters: dict[str, type[Formatter]] = {}

    def register(self, name: str, formatter_class: type[Formatter]) -> None:
        self._formatters[name] = formatter_class

    def get(self, name: str, **kwargs: object) -> Formatter:
        """Return a formatter instance by name.

        Raises KeyError if the format name is not registered.
        """
        if name not in self._formatters:
            available = ", ".join(sorted(self._formatters))
            msg = f"Unknown format {name!r}. Available: {available}"
            raise KeyError(msg)
        return self._formatters[name](**kwargs)

    @property
    def available(self) -> list[str]:
        return sorted(self._formatters)


# Global registry instance populated by formatter modules.
registry = FormatterRegistry()
