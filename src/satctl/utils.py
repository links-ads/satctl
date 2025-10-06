import zipfile
from functools import partial
from pathlib import Path
from shutil import copyfileobj
from typing import IO, Any, Callable

from pyproj import CRS, Transformer
from pyresample import create_area_def
from pyresample.geometry import AreaDefinition, DynamicAreaDefinition
from shapely import Polygon

from satctl.progress import ProgressReporter


class IOProgressWrapper:
    """
    Derived from the magnificent `tqdm.CallbackIOWrapper`
    """

    def __init__(self, callback: Callable, stream: IO[bytes]):
        """
        Wrap a given `file`-like object's `read()` or `write()` to report
        lengths to the given `callback`
        """
        self.callback = callback
        self.stream = stream

    def write(self, data, *args, **kwargs):
        res = self.stream.write(data, *args, **kwargs)
        self.callback(advance=len(data))
        return res

    def read(self, *args, **kwargs):
        data = self.stream.read(*args, **kwargs)
        self.callback(advance=len(data))
        return data


def convert_crs(value: str | CRS) -> CRS:
    if not isinstance(value, CRS):
        return CRS.from_string(value)
    return value


def extract_zip(
    zip_path: Path,
    extract_to: Path,
    expected_dir: str | None = None,
    progress: ProgressReporter | None = None,
    task_id: Any | None = None,
) -> Path:
    """Extract zip file and return path to extracted directory.

    Args:
        zip_path: Path to zip file
        extract_to: Directory to extract to
        expected_dir: Expected directory name (e.g., "{zip_stem}.SEN3")

    Returns:
        Path to extracted directory or None if failed
    """
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        # zip_ref.extractall(extract_to)

        total_size = sum(f.file_size for f in zip_ref.infolist() if not f.is_dir())
        if progress is not None and task_id is not None:
            progress.set_task_duration(item_id=task_id, total=total_size)

        for info in zip_ref.infolist():
            if info.is_dir() or progress is None or task_id is None:
                zip_ref.extract(info, extract_to)
            else:
                file_path = extract_to / info.filename
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with zip_ref.open(info) as in_file, open(str(file_path), "wb") as out_file:
                    copyfileobj(
                        IOProgressWrapper(callback=partial(progress.update_progress, task_id), stream=in_file),
                        out_file,
                    )

    if expected_dir:
        extracted_dir = extract_to / expected_dir
        if not extracted_dir.exists():
            raise ValueError(f"Expected directory {expected_dir} not found")
        return extracted_dir
    else:
        # Return the extract_to directory
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
