"""Progress reporting implementations for satellite data operations.

This package provides progress reporters for tracking download and processing:
- EmptyProgressReporter: No-op reporter for silent operation
- SimpleProgressReporter: Basic text-based progress output
- RichProgressReporter: Enhanced terminal UI with progress bars

All reporters implement the ProgressReporter interface and can be configured
via the registry system.
"""

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
