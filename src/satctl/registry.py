from typing import Generic, TypeVar

T = TypeVar("T")


class Registry(Generic[T]):
    """Registry for managing specific class implementations."""

    def __init__(self):
        self._items: dict[str, type[T]] = {}

    def register(self, name: str, source_class: type[T]):
        self._items[name] = source_class

    def create(self, name: str, **kwargs) -> T:
        if name not in self._items:
            raise FileNotFoundError(f"Item '{name}' not found. Available: {list(self._items.keys())}")

        source_class = self._items[name]
        return source_class(**kwargs)

    def list(self) -> list[str]:
        return list(self._items.keys())

    def is_registered(self, name: str) -> bool:
        return name in self._items
