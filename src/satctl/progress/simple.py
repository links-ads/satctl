from satctl.progress.base import ProgressReporter


class SimpleProgressReporter(ProgressReporter):
    """Simple text-based progress reporter using logging."""

    def __init__(self):
        import logging

        self.log = logging.getLogger(__name__)
        self.total_items = 0
        self.completed = 0
        self.failed = 0

    def start(self, total_items: int) -> None:
        self.total_items = total_items
        self.completed = 0
        self.failed = 0
        self.log.info(f"Tracking progress for {total_items} items")

    def add_task(self, item_id: str, description: str) -> dict:
        self.log.info("Started %s - %s", description, item_id)
        return {"item_id": item_id, "description": description}

    def set_task_duration(self, item_id: str, total: int) -> None:
        # we do not track task duration in simple reporter
        pass

    def update_progress(self, item_id: str, advance: int | None = None, description: str | None = None) -> None:
        # no byte-level progress
        pass

    def end_task(self, item_id: str, success: bool, description: str | None = None) -> None:
        if success:
            self.completed += 1
        else:
            self.failed += 1
        remaining = self.total_items - self.completed - self.failed
        description = description or ""
        status = f"✓ {description}" if success else f"✗ {description}"

        self.log.info(
            "%s - %s (%d/%d, %d remaining)",
            status,
            item_id,
            self.completed + self.failed,
            self.total_items,
            remaining,
        )

    def stop(self) -> None:
        self.log.info(
            "Tracking completed: %d successful, %d failed, %d total",
            self.completed,
            self.failed,
            self.total_items,
        )
