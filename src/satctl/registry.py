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

from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Registry(Generic[T]):
    """Registry for managing specific class implementations."""

    def __init__(self, name: str):
        self.registry_name = name
        self._items: dict[str, type[T]] = {}

    def get(self, name: str) -> type[T] | None:
        """Get a registered class by name.

        Args:
            name (str): Name of the registered class

        Returns:
            type[T] | None: Registered class or None if not found
        """
        return self._items.get(name)

    def register(self, name: str, source_class: type[T]):
        """Register a class implementation.

        Args:
            name (str): Name to register the class under
            source_class (type[T]): Class to register
        """
        self._items[name] = source_class

    def create(self, name: str, **kwargs) -> T:
        """Create an instance of a registered class.

        Args:
            name (str): Name of the registered class to instantiate
            **kwargs: Keyword arguments to pass to the class constructor

        Returns:
            T: Instance of the registered class

        Raises:
            ValueError: If name is not registered
        """
        if name not in self._items:
            available = ", ".join(self._items.keys())
            raise ValueError(
                f"Resource not found: {self.registry_name} '{name}'. "
                f"Available options: {available}. "
                f"To register a custom {self.registry_name}, use {self.registry_name}_registry.register(name, class)."
            )
        source_class = self._items[name]
        return source_class(**kwargs)

    def list(self) -> list[str]:
        """List all registered names.

        Returns:
            list[str]: List of all registered class names
        """
        return list(self._items.keys())

    def is_registered(self, name: str) -> bool:
        """Check if a name is registered.

        Args:
            name (str): Name to check

        Returns:
            bool: True if name is registered, False otherwise
        """
        return name in self._items


class Builder(Generic[T]):
    """
    Builder/factory class definition.
    Provides the means to defer the creation of a named object when we need one.
    """

    def __init__(self, name: str, registry: Registry, **config: dict) -> None:
        self.name = name
        self.registry = registry
        self.config = config

    def __call__(self, **overrides: Any) -> T:
        merged_config = {**self.config, **overrides}
        return self.registry.create(self.name, **merged_config)
