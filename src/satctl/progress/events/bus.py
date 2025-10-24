import logging
import threading
from contextvars import ContextVar
from typing import Callable

from satctl.model import ProgressEvent, ProgressEventType

log = logging.getLogger(__name__)


class EventBus:
    """
    thread-safe event bus for progress events.
    """

    def __init__(self):
        self._handlers: list[Callable[[ProgressEvent], None]] = []
        self._lock = threading.Lock()

    def subscribe(self, handler: Callable[[ProgressEvent], None]):
        """Subscribe a handler to receive progress events.

        Args:
            handler (Callable[[ProgressEvent], None]): Event handler function
        """
        with self._lock:
            self._handlers.append(handler)

    def unsubscribe(self, handler: Callable[[ProgressEvent], None]):
        """Unsubscribe a handler from receiving progress events.

        Args:
            handler (Callable[[ProgressEvent], None]): Event handler function to remove
        """
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)

    def emit(self, event: ProgressEvent):
        """Emit a progress event to all subscribed handlers.

        Args:
            event (ProgressEvent): Event to emit
        """
        with self._lock:
            handlers = self._handlers.copy()

        for handler in handlers:
            handler(event)


# global bus instance, thread-safe singleton
_global_bus = EventBus()
# context-aware bus for nested contexts (optional advanced usage)
_current_bus: ContextVar[EventBus | None] = ContextVar("bus", default=None)


def get_bus() -> EventBus:
    """
    Get the current event bus (from context or global).

    Returns:
        EventBus: current event bus, either global or local.
    """
    return _current_bus.get() or _global_bus


def emit_event(event_type: ProgressEventType, task_id: str, **data):
    """
    Convenience function to emit events.

    Args:
        event_type (ProgressEventType): event type.
        task_id (str): ID of the task to be tracked.
    """
    event = ProgressEvent(type=event_type, task_id=task_id, data=data)
    get_bus().emit(event)
