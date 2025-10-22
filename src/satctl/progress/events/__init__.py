"""Event bus system for progress reporting.

This module provides a simple event bus for publishing and subscribing to
progress events throughout satctl. It enables decoupled progress reporting
where different components can emit events without knowing about the reporters.
"""

from satctl.progress.events.bus import emit_event, get_bus

__all__ = ["get_bus", "emit_event"]
