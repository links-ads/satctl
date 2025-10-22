"""Generic registry pattern for managing pluggable implementations.

This module provides a type-safe registry system that allows registering,
retrieving, and instantiating implementations of a given interface. It's
used throughout satctl for data sources, authenticators, downloaders,
and writers.

Example:
    >>> from satctl.registry import Registry
    >>> from satctl.sources import DataSource
    >>>
    >>> source_registry = Registry[DataSource]("source")
    >>> source_registry.register("sentinel2", Sentinel2L2ASource)
    >>> source = source_registry.create("sentinel2", downloader=my_downloader)
"""

from typing import Generic, TypeVar

T = TypeVar("T")


class Registry(Generic[T]):
    """Registry for managing specific class implementations."""

    def __init__(self, name: str):
        self.registry_name = name
        self._items: dict[str, type[T]] = {}

    def get(self, name: str) -> type[T] | None:
        return self._items.get(name)

    def register(self, name: str, source_class: type[T]):
        self._items[name] = source_class

    def create(self, name: str, **kwargs) -> T:
        if name not in self._items:
            raise ValueError(
                (
                    f"{self.registry_name.capitalize()} '{name}' not found. "
                    f"Specify one of the following: ({list(self._items.keys())}), "
                    f"or register your own {self.registry_name}."
                )
            )
        source_class = self._items[name]
        return source_class(**kwargs)

    def list(self) -> list[str]:
        return list(self._items.keys())

    def is_registered(self, name: str) -> bool:
        return name in self._items
