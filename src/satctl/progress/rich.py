from collections import namedtuple
from typing import Any

from satctl.progress import ProgressReporter

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
        except ImportError:
            raise ImportError(
                "rich is not installed, please ensure to install it manually or include the extra `eokit[console]`"
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
        self._active = False

    def start(self, total_items: int) -> None:
        self.progress.start()
        self._active = True
        self._task_info = {}

    def add_task(self, item_id: str, description: str) -> Any:
        task_id = self.progress.add_task(
            description=description,
            item_id=item_id,
            start=False,
            total=None,  # will be set when we know file size
        )
        self._task_info[item_id] = TaskInfo(task_id=task_id, description=description)
        return task_id

    def set_task_duration(self, item_id: str, total: int) -> None:
        """Set total size for a task (when we get Content-Length)."""
        if self._active:
            task_id = self._task_info[item_id].task_id
            self.progress.update(task_id=task_id, total=total)
            self.progress.start_task(task_id=task_id)

    def update_progress(self, item_id: str, advance: int | None = None, description: str | None = None) -> None:
        if self._active:
            task_info = self._task_info[item_id]
            if description and description != task_info.description:
                task_info = TaskInfo(task_id=task_info.task_id, description=description)
                self._task_info[item_id] = task_info
            self.progress.update(task_id=task_info.task_id, advance=advance, description=task_info.description)

    def end_task(self, item_id: str, success: bool, description: str | None = None) -> None:
        if self._active:
            status = "✓" if success else "✗"
            task_info = self._task_info[item_id]
            description = description or task_info.description
            self.progress.update(task_id=task_info.task_id, description=f"{status} {description}")

    def stop(self) -> None:
        if self._active:
            self.progress.stop()
            self._active = False
            self._task_info.clear()
