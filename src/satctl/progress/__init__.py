from typing import Any

from satctl.progress.base import EmptyReporter, ProgressReporter, get_reporter, set_reporter
from satctl.progress.rich import RichProgressReporter
from satctl.progress.simple import SimpleProgressReporter
from satctl.registry import Registry

registry = Registry[ProgressReporter](name="reporter")
registry.register("empty", EmptyReporter)
registry.register("simple", SimpleProgressReporter)
registry.register("rich", RichProgressReporter)

__all__ = [
    "ProgressReporter",
    "EmptyReporter",
    "SimpleProgressReporter",
    "RichProgressReporter",
    "get_reporter",
    "set_reporter",
]


def create_reporter(reporter_name: str | None, **kwargs: dict[str, Any]) -> ProgressReporter:
    reporter_name = reporter_name or "empty"
    config = kwargs or {}
    reporter = registry.create(reporter_name, **config)
    set_reporter(reporter)
    return reporter
