import logging
from collections import namedtuple
from typing import Any

from satctl.model import ProgressEvent
from satctl.progress import LoggingConfig, ProgressReporter

TaskInfo = namedtuple("TaskInfo", ("task_id", "description"))


class RichProgressReporter(ProgressReporter):
    """Rich-based progress reporter with fancy progress bars."""

    def __init__(self):
        try:
            from rich.progress import (
                BarColumn,
                DownloadColumn,
                Progress,
                TextColumn,
                TimeRemainingColumn,
                TransferSpeedColumn,
            )

            self.progress = Progress(
                TextColumn("[bold green]{task.description}", justify="right"),
                TextColumn("[blue]{task.fields[item_id]}", justify="right"),
                BarColumn(bar_width=None),
                "[progress.percentage]{task.percentage:>3.1f}%",
                "•",
                DownloadColumn(),
                "•",
                TransferSpeedColumn(),
                "•",
                TimeRemainingColumn(),
            )
        except ImportError:
            raise ImportError(
                "rich is not installed. Either run `pip install satctl[console]`, "
                "install it manually, or choose another progress reporter"
            )

        self.log = logging.getLogger(__name__)
        self.active = False
        self.task_info: dict[str, Any] = {}

    @classmethod
    def logging_config(cls) -> LoggingConfig:
        """Provide logging configuration with Rich handler.

        Returns:
            LoggingConfig: Logging configuration with Rich handler
        """
        from rich.logging import RichHandler

        return LoggingConfig(
            handlers=[
                RichHandler(
                    show_time=True,
                    show_path=False,
                    rich_tracebacks=False,
                    tracebacks_suppress=["typer", "click"],
                )
            ],
            format="%(message)s",
        )

    def start(self) -> None:
        """Start the rich progress reporter."""
        self.progress.start()
        self.active = True
        super().start()

    def stop(self) -> None:
        """Stop the rich progress reporter."""
        if not self.active:
            return
        self.progress.stop()
        self.active = False
        self.task_info.clear()
        super().stop()

    def on_batch_started(self, event: ProgressEvent):
        """Handle batch started event.

        Args:
            event (ProgressEvent): Progress event with batch information
        """
        description = event.data.get("description")
        total_items = event.data.get("total_items")
        self.log.info("Starting batch: %s - (%s items)", description, total_items or "NA")

    def on_batch_completed(self, event: ProgressEvent) -> None:
        """Handle batch completed event.

        Args:
            event (ProgressEvent): Progress event with completion counts
        """
        success_count = event.data.get("success_count")
        failure_count = event.data.get("failure_count")
        success = str(success_count) or "NA"
        failure = str(failure_count) or "NA"
        self.log.info("Batch complete: %s succeeded, %s failed", success, failure)

    def on_task_created(self, event: ProgressEvent) -> Any:
        """Handle task created event.

        Args:
            event (ProgressEvent): Progress event with task information

        Returns:
            Any: Rich progress task ID
        """
        description = event.data.get("description", "")
        task_id = self.progress.add_task(
            description=description,
            item_id=event.task_id,
            start=False,
            total=None,  # will be set when we know file size
        )
        self.task_info[event.task_id] = TaskInfo(task_id=task_id, description=description)
        return task_id

    def on_task_duration(self, event: ProgressEvent) -> None:
        """Handle task duration event.

        Args:
            event (ProgressEvent): Progress event with duration information
        """
        task_id = self.task_info[event.task_id].task_id
        self.progress.update(task_id=task_id, total=event.data.get("duration"))
        self.progress.start_task(task_id=task_id)

    def on_task_progress(self, event: ProgressEvent) -> None:
        """Handle task progress event.

        Args:
            event (ProgressEvent): Progress event with progress update
        """
        task_info = self.task_info[event.task_id]
        description = event.data.get("description", task_info.description)
        # update description if necessary
        if description != task_info.description:
            task_info = TaskInfo(task_id=task_info.task_id, description=description)
            self.task_info[event.task_id] = task_info
        self.progress.update(
            task_id=task_info.task_id,
            advance=event.data.get("advance"),
            description=task_info.description,
        )

    def on_task_completed(self, event: ProgressEvent) -> None:
        """Handle task completed event.

        Args:
            event (ProgressEvent): Progress event with completion status
        """
        status = "✓" if event.data.get("success") else "✗"
        task_info = self.task_info[event.task_id]
        description = event.data.get("description", task_info.description)
        self.progress.update(task_id=task_info.task_id, description=f"{status} {description}")
        del self.task_info[event.task_id]
