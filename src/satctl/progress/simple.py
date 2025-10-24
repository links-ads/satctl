from satctl.model import ProgressEvent
from satctl.progress import LoggingConfig, ProgressReporter


class SimpleProgressReporter(ProgressReporter):
    """Simple text-based progress reporter using logging."""

    def __init__(self):
        import logging

        self.log = logging.getLogger(__name__)
        self.total_items = 0
        self.completed = 0
        self.failed = 0

    @classmethod
    def logging_config(cls) -> LoggingConfig:
        """Provide logging configuration for simple reporter.

        Returns:
            LoggingConfig: Logging configuration with timestamp format
        """
        return LoggingConfig(
            handlers=None,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    def start(self) -> None:
        """Start the simple progress reporter and reset counters."""
        super().start()
        self.total_items = 0
        self.completed = 0
        self.failed = 0

    def stop(self) -> None:
        """Stop the simple progress reporter."""
        return super().stop()

    def on_batch_started(self, event: ProgressEvent) -> None:
        """Handle batch started event.

        Args:
            event (ProgressEvent): Progress event with batch information
        """
        total_items = event.data.get("total_items", "NA")
        self.total_items = total_items
        self.log.info("Tracking progress for %s items", total_items)

    def on_batch_completed(self, event: ProgressEvent):
        """Handle batch completed event.

        Args:
            event (ProgressEvent): Progress event with completion counts
        """
        success_count = event.data.get("success_count")
        failure_count = event.data.get("failure_count")
        success = str(success_count) or "NA"
        failure = str(failure_count) or "NA"
        self.log.info("Batch complete: %s succeeded, %s failed", success, failure)

    def on_task_created(self, event: ProgressEvent):
        """Handle task created event.

        Args:
            event (ProgressEvent): Progress event with task information
        """
        self.log.info("Started %s - %s", event.task_id, event.data.get("description", ""))

    def on_task_duration(self, event: ProgressEvent):
        """Handle task duration event (not tracked in simple reporter).

        Args:
            event (ProgressEvent): Progress event with duration
        """
        # task duration not yet tracked in simple reporter
        pass

    def on_task_progress(self, event: ProgressEvent):
        """Handle task progress event (not tracked in simple reporter).

        Args:
            event (ProgressEvent): Progress event with progress update
        """
        # no byte-level progress
        pass

    def on_task_completed(self, event: ProgressEvent):
        """Handle task completed event.

        Args:
            event (ProgressEvent): Progress event with completion status
        """
        success = event.data.get("success", False)
        if success:
            self.completed += 1
        else:
            self.failed += 1
        remaining = self.total_items - self.completed - self.failed
        description = event.data.get("description", "")
        status = f"✓ {description}" if success else f"✗ {description}"
        self.log.info(
            "%s - %s (%d/%d, %d remaining)",
            status,
            event.task_id,
            self.completed + self.failed,
            self.total_items,
            remaining,
        )
