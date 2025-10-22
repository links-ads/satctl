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
        result = {}
        if isinstance(datasets, str):
            result = {datasets: datasets}
        # if dict, already ok
        elif isinstance(datasets, Mapping):
            pass
        # test lists later, dict is also iterable
        elif isinstance(datasets, Iterable):
            result = {s: s for s in datasets}
        else:
            raise TypeError(f"Dataset format ({type(datasets)}) not supported")
        return result

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
