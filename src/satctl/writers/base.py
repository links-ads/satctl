from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Mapping

from xarray import DataArray


class Writer(ABC):
    """Base class for writing processed satellite data."""

    def __init__(self, extension: str) -> None:
        super().__init__()
        self.extension = extension

    def parse_datasets(self, datasets: str | list[str] | dict[str, str]) -> dict[str, str]:
        """Parse datasets into normalized dict format.

        Args:
            datasets: Dataset specification (name, list of names, or name->filename mapping)

        Returns:
            Dictionary mapping dataset names to output filenames

        Raises:
            TypeError: If datasets type is not supported
        """
        if isinstance(datasets, str):
            return {datasets: datasets}
        elif isinstance(datasets, Mapping):
            return dict(datasets)
        elif isinstance(datasets, Iterable):
            return {name: name for name in datasets}
        else:
            raise TypeError(f"Unsupported dataset format: {type(datasets)}")

    @abstractmethod
    def write(
        self,
        dataset: DataArray,
        output_path: Path,
        **kwargs,
    ) -> None:
        """Write dataset to file in the specific format.

        Args:
            dataset: Xarray DataArray with satellite data and metadata
            output_path: Path where the output file will be written
            **kwargs: Writer-specific options (compression, dtype, etc.)

        Raises:
            FileNotFoundError: If output_path parent directory doesn't exist
        """
