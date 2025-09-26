from abc import ABC, abstractmethod
from typing import Any


class ProgressReporter(ABC):
    @abstractmethod
    def start(self, total_items: int) -> None: ...

    @abstractmethod
    def add_task(self, item_id: str, description: str) -> Any: ...

    @abstractmethod
    def set_task_duration(self, task: Any, total: int) -> None: ...

    @abstractmethod
    def update_progress(self, task: Any, advance: int | None = None, description: str | None = None) -> None: ...

    @abstractmethod
    def end_task(self, task: Any, success: bool, description: str | None = None) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...


class EmptyReporter(ProgressReporter):
    """
    Empty reporter to avoid continuos checks against None
    """

    def start(self, total_items: int) -> None:
        pass

    def add_task(self, item_id: str, description: str) -> Any:
        pass

    def set_task_duration(self, task: Any, total: int) -> None:
        pass

    def update_progress(self, task: Any, advance: int | None = None, description: str | None = None) -> None:
        pass

    def end_task(self, task: Any, success: bool, description: str | None = None) -> None:
        pass

    def stop(self) -> None:
        pass
