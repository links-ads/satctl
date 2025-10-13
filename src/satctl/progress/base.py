import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import Handler

from satctl.model import ProgressEvent
from satctl.progress.events import get_bus

log = logging.getLogger(__name__)


@dataclass
class LoggingConfig:
    handlers: list[Handler] | None
    format: str


class ProgressReporter(ABC):
    @classmethod
    def logging_config(cls) -> LoggingConfig:
        """Provide a custom configuration for the logging module.
        The current implementation allows for barebone logs, subclasses
        should extend this method to customize the output (e.g., optional
        rich logs).

        Returns:
            LoggingConfig: logging configuration parameters.
        """
        return LoggingConfig(handlers=None, format="%(message)s")

    @abstractmethod
    def start(self) -> None:
        get_bus().subscribe(self.handle_event)

    @abstractmethod
    def stop(self) -> None:
        get_bus().unsubscribe(self.handle_event)

    def handle_event(self, event: ProgressEvent) -> None:
        """Dispatch-like function that tries to find a specific function to handle the given event enum.
        The base reporter intentionally implements empty handlers to allow for overriding only the necessary function.

        Args:
            event (ProgressEvent): generic event derived from the event bus.

        Raises:
            ValueError: when to handler has been found. Should happen only in case of event type customization.
        """
        event_handler_name = f"on_{event.type.value}"
        event_handler_fn = getattr(self, event_handler_name, None)
        if event_handler_fn is None:
            raise ValueError(f"No handler for event: {event.type.value}")
        if event.type.value != "task_progress":
            log.debug("Handling event: %s", event)
        event_handler_fn(event)

    def on_batch_started(self, event: ProgressEvent): ...

    def on_batch_completed(self, event: ProgressEvent): ...

    def on_task_created(self, event: ProgressEvent): ...

    def on_task_duration(self, event: ProgressEvent): ...

    def on_progress(self, event: ProgressEvent): ...

    def task_completed(self, event: ProgressEvent): ...


class EmptyProgressReporter(ProgressReporter):
    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass
