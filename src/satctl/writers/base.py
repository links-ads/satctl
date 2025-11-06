from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
from xarray import DataArray


class Writer(ABC):
    """Base class for writing processed satellite data."""

    def __init__(self, extension: str) -> None:
        """Initialize writer with file extension.

        Args:
            extension (str): File extension for output files (e.g., ".tif", ".nc")
        """
        super().__init__()
        self.extension = extension

    def parse_datasets(self, datasets: str | list[str] | dict[str, str]) -> dict[str, str]:
        """Parse datasets into normalized dict format.

        Args:
            datasets (str | list[str] | dict[str, str]): Dataset specification

        Returns:
            dict[str, str]: Dictionary mapping dataset names to output filenames

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
        dtype: type | np.dtype[Any] | None = None,
        **kwargs: Any,
    ) -> Path | None:
        """Write dataset to file in the specific format.

        Args:
            dataset (DataArray): Xarray DataArray with satellite data and metadata
            output_path (Path): Path where the output file will be written
            **kwargs: Writer-specific options (compression, dtype, etc.)

        Raises:
            FileNotFoundError: If output_path parent directory doesn't exist
        """
