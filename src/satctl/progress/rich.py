from typing import Any

from satctl.progress import ProgressReporter


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
        self._tasks = {}

    def add_task(self, item_id: str, description: str) -> Any:
        task_id = self.progress.add_task(
            description=description,
            item_id=item_id,
            start=False,
            total=None,  # Will be set when we know file size
        )
        self._tasks[task_id] = dict(
            description=description,
            item_id=item_id,
        )
        return task_id

    def set_task_duration(self, task: Any, total: int) -> None:
        """Set total size for a task (when we get Content-Length)."""
        if self._active:
            self.progress.update(task, total=total)
            self.progress.start_task(task)

    def update_progress(self, task: Any, advance: int | None = None, description: str | None = None) -> None:
        if self._active:
            self.progress.update(task, advance=advance, description=description)
            if description is not None:
                self._tasks[task]["description"] = description

    def end_task(self, task: Any, success: bool, description: str | None = None) -> None:
        if self._active:
            status = "✓" if success else "✗"
            description = description or self._tasks[task]["description"]
            self.progress.update(task, description=f"{status} {description}")

    def stop(self) -> None:
        if self._active:
            self.progress.stop()
            self._active = False
            self._tasks.clear()
