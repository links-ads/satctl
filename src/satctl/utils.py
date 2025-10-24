"""Utility functions for satctl operations.

This module provides helper functions for:
- Progress-tracked I/O operations
- Logging configuration
- ZIP file extraction with progress reporting
- Geometric area definition creation
"""

import logging
import zipfile
from functools import partial
from pathlib import Path
from shutil import copyfileobj
from typing import IO, Callable

from pyproj import CRS, Transformer
from pyresample import create_area_def
from pyresample.geometry import AreaDefinition, DynamicAreaDefinition
from shapely import Polygon

from satctl.model import ProgressEventType
from satctl.progress import ProgressReporter
from satctl.progress.events import emit_event


class IOProgressWrapper:
    """
    Derived from the magnificent `tqdm.CallbackIOWrapper`
    """

    def __init__(self, callback: Callable, stream: IO[bytes]):
        """Wrap a file-like object to report read/write progress.

        Args:
            callback (Callable): Callback function to report progress
            stream (IO[bytes]): File-like stream to wrap
        """
        self.callback = callback
        self.stream = stream

    def write(self, data, *args, **kwargs):
        """Write data and report progress.

        Args:
            data: Data to write
            *args: Additional positional arguments for stream.write
            **kwargs: Additional keyword arguments for stream.write

        Returns:
            Any: Result from stream.write
        """
        res = self.stream.write(data, *args, **kwargs)
        self.callback(advance=len(data))
        return res

    def read(self, *args, **kwargs):
        """Read data and report progress.

        Args:
            *args: Positional arguments for stream.read
            **kwargs: Keyword arguments for stream.read

        Returns:
            Any: Data read from stream
        """
        data = self.stream.read(*args, **kwargs)
        self.callback(advance=len(data))
        return data


def setup_logging(
    log_level: str,
    reporter_cls: type[ProgressReporter] | None,
    suppressions: dict[str, list[str]] | None = None,
) -> None:
    """Configure logging, optionally using the reporter's configuration.

    Args:
        log_level (str): which log level (e.g., DEBUG, INFO, WARNING).
        reporter_cls (type[ProgressReporter] | None): Optional reporter class to get the config from.
        suppressions (dict[str, list[str]] | None, optional): Additional user-provided suppressions. Defaults to None.
    """
    config = reporter_cls.logging_config() if reporter_cls else ProgressReporter.logging_config()
    suppressions = suppressions or {}
    # apply config
    logging.basicConfig(
        level=log_level.upper(),
        format=config.format,
        handlers=config.handlers,
        force=True,  # reconfigure if already configured
    )
    # apply suppressions by level
    for level_name, loggers in suppressions.items():
        suppress_level = getattr(logging, level_name.upper())
        for logger_name in loggers:
            logging.getLogger(logger_name).setLevel(suppress_level)


def extract_zip(
    zip_path: Path,
    extract_to: Path,
    item_id: str,
    expected_dir: str | None = None,
) -> Path:
    """Extract zip file and return path to extracted directory.

    Args:
        zip_path (Path): Path to zip file
        extract_to (Path): Directory to extract to
        item_id (str): Identifier for progress tracking
        expected_dir (str | None): Expected directory name. Defaults to None.

    Returns:
        Path: Path to extracted directory

    Raises:
        ValueError: If expected directory not found after extraction
    """
    task_id = f"extract_{item_id}"

    emit_event(ProgressEventType.TASK_CREATED, task_id=task_id, description="extract")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        total_size = sum(f.file_size for f in zip_ref.infolist() if not f.is_dir())
        emit_event(ProgressEventType.TASK_DURATION, task_id=task_id, duration=total_size)

        for info in zip_ref.infolist():
            if info.is_dir():
                zip_ref.extract(info, extract_to)
            else:
                file_path = extract_to / info.filename
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with zip_ref.open(info) as in_file, open(str(file_path), "wb") as out_file:
                    copyfileobj(
                        IOProgressWrapper(
                            callback=partial(emit_event, ProgressEventType.TASK_PROGRESS, task_id),
                            stream=in_file,
                        ),
                        out_file,
                    )

    if expected_dir:
        extracted_dir = extract_to / expected_dir
        if not extracted_dir.exists():
            raise ValueError(
                f"Invalid archive structure: expected directory '{expected_dir}' not found after extraction"
            )
        emit_event(ProgressEventType.TASK_COMPLETED, task_id=task_id, success=True)
        return extracted_dir
    else:
        # Return the extract_to directory
        emit_event(ProgressEventType.TASK_COMPLETED, task_id=task_id, success=True)
        return extract_to


def area_def_from_geometry(
    name: str,
    area: Polygon,
    resolution: int,
    target_crs: CRS,
    source_crs: CRS | None = None,
    description: str | None = None,
) -> AreaDefinition | DynamicAreaDefinition:
    """Generate a pyresample AreaDefinition from a given polygon/multipolygon.

    Args:
        name (str): name to be assigned to the definition.
        area (Polygon): area defining the extents of the resampled output.
        resolution (int): spatial resolution, unit is defined by the target CRS
        target_crs (pyproj.CRS): CRS to use as destination for projection.
        source_crs (pyproj.CRS, optional): CRS of the input polygon. Defaults to "EPSG:4326".
        description (str | None, optional): Optional description for the definition. Defaults to None.

    Returns:
        AreaDefinition | DynamicAreaDefinition: pyresample definition for satpy
    """
    bounds = area.bounds
    source_crs = source_crs or CRS.from_epsg(4326)
    projector = Transformer.from_crs(source_crs, target_crs, always_xy=True)
    # Transform corner coordinates
    min_x, min_y = projector.transform(bounds[0], bounds[1])  # SW corner
    max_x, max_y = projector.transform(bounds[2], bounds[3])  # NE corner

    # Create area definition with transformed bounds
    area_def = create_area_def(
        name,
        target_crs,
        resolution=resolution,
        area_extent=[min_x, min_y, max_x, max_y],
        units=f"{target_crs.axis_info[0].unit_name}s",  # pyresample is plural (metres, degrees)
        description=description,
    )
    return area_def
