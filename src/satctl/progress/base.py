from abc import ABC, abstractmethod
from contextvars import ContextVar
from typing import Any

_current_reporter: ContextVar["ProgressReporter | None"] = ContextVar("progress_reporter", default=None)


def get_reporter() -> "ProgressReporter":
    reporter = _current_reporter.get()
    if reporter is None:
        reporter = EmptyReporter()
        set_reporter(reporter)
    return reporter


def set_reporter(reporter: "ProgressReporter") -> None:
    _current_reporter.set(reporter)


class ProgressReporter(ABC):
    @abstractmethod
    def start(self, total_items: int) -> None: ...

    @abstractmethod
    def add_task(self, item_id: str, description: str) -> Any: ...

    @abstractmethod
    def set_task_duration(self, item_id: str, total: int) -> None: ...

    @abstractmethod
    def update_progress(self, item_id: str, advance: int | None = None, description: str | None = None) -> None: ...

    @abstractmethod
    def end_task(self, item_id: str, success: bool, description: str | None = None) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...

    def cleanup(self) -> None:
        self.stop()
        _current_reporter.set(None)


class EmptyReporter(ProgressReporter):
    """
    Empty reporter to avoid continuos checks against None
    """

    def start(self, total_items: int) -> None:
        pass

    def add_task(self, item_id: str, description: str) -> Any:
        pass

    def set_task_duration(self, item_id: str, total: int) -> None:
        pass

    def update_progress(self, item_id: str, advance: int | None = None, description: str | None = None) -> None:
        pass

    def end_task(self, item_id: str, success: bool, description: str | None = None) -> None:
        pass

    def stop(self) -> None:
        pass
