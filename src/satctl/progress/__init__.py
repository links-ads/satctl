from satctl.progress.base import EmptyReporter, ProgressReporter
from satctl.progress.rich import RichProgressReporter
from satctl.progress.simple import SimpleProgressReporter
from satctl.registry import Registry

registry = Registry[ProgressReporter]()
registry.register("empty", EmptyReporter)
registry.register("simple", SimpleProgressReporter)
registry.register("rich", RichProgressReporter)

__all__ = [
    "ProgressReporter",
    "EmptyReporter",
    "SimpleProgressReporter",
    "RichProgressReporter",
]


def create_reporter(reporter_name: str | None, config: dict | None = None) -> ProgressReporter:
    if reporter_name is None:
        reporter_name = "empty"
    if not registry.is_registered(reporter_name):
        raise ValueError(f"Unknown reporter: {reporter_name}. Available reporters: {registry.list()}")
    config = config or {}
    return registry.create(reporter_name, **config)
