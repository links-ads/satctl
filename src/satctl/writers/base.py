from abc import ABC, abstractmethod
from pathlib import Path

from satpy.scene import Scene


class Writer(ABC):
    """Base class for writing processed satellite data."""

    @abstractmethod
    def write(self, scene: Scene, output_path: Path, composite: str, **kwargs) -> bool:
        """Write scene data to file.

        Args:
            scene: Resampled satpy Scene
            output_path: Output file path
            composite: Name of composite to write
            **kwargs: Additional writer-specific options

        Returns:
            bool: Success status
        """
        pass
