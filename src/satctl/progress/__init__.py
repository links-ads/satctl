from typing import Any

from satctl.progress.base import EmptyProgressReporter, LoggingConfig, ProgressReporter
from satctl.progress.rich import RichProgressReporter
from satctl.progress.simple import SimpleProgressReporter
from satctl.registry import Registry

registry = Registry[ProgressReporter](name="reporter")
registry.register("empty", EmptyProgressReporter)
registry.register("simple", SimpleProgressReporter)
registry.register("rich", RichProgressReporter)

__all__ = [
    "ProgressReporter",
    "EmptyProgressReporter",
    "SimpleProgressReporter",
    "RichProgressReporter",
    "LoggingConfig",
]


def create_reporter(reporter_name: str, **kwargs: dict[str, Any]) -> ProgressReporter | None:
    config = kwargs or {}
    return registry.create(reporter_name, **config)
