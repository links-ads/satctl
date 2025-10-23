import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, cast

from geojson_pydantic import Feature, FeatureCollection
from pydantic import BaseModel, BeforeValidator, model_validator
from pyproj import CRS
from pyproj.exceptions import CRSError
from shapely import GeometryCollection, Polygon, from_geojson
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

# Constants
GRANULE_METADATA_FILENAME = "_granule.json"


def convert_to_geojson(value: Any) -> Any:
    # shapely -> geojson before validating
    if isinstance(value, BaseGeometry):
        return value.__geo_interface__
    # otherwise validate as is. Hopefully it is already a geojson
    return value


def validate_crs(value: Any) -> Any:
    if value is None:
        return value
    # if already a CRS instance, dump it
    if isinstance(value, CRS):
        return value.to_string()
    # check it's a valid CRS
    try:
        CRS.from_string(value)
        return value
    except CRSError:
        raise ValueError(
            f"Invalid CRS: '{value}' (expected EPSG code like 'EPSG:4326' or valid proj string)"
        )


class AreaParams(BaseModel):
    """Store the actual geometry, not the path to it."""

    area: Annotated[Feature | FeatureCollection | None, BeforeValidator(convert_to_geojson)] = None

    @classmethod
    def _load_geometry(cls, path: Path) -> dict:
        if path is None:
            raise ValueError("Invalid configuration: area file path is required for from_file()")
        if not path.exists() or not path.is_file():
            raise ValueError(f"Resource not found: area file '{path}' does not exist or is not a file")
        data = json.loads(path.read_text())
        return data

    @classmethod
    def from_file(cls, path: Path, **kwargs) -> "AreaParams":
        return cls(area=cls._load_geometry(path))  # type:ignore

    @property
    def area_geometry(self) -> Polygon | None:
        if self.area is None:
            return None
        # this is absurd, but it's the only way to validate inputs
        # and convert any geojson back to shapely
        geometry = from_geojson(self.area.model_dump_json())
        # if not already a polygon, use convex hull
        if hasattr(geometry, "geoms"):
            geometry = cast(GeometryCollection, geometry)
            geometry = unary_union(list(geometry.geoms))
        if not isinstance(geometry, Polygon):
            return cast(Polygon, geometry.convex_hull)
        return geometry


class SearchParams(AreaParams):
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_dates(self):
        if self.start >= self.end:
            raise ValueError(
                f"Invalid date range: start ({self.start}) must be before end ({self.end})"
            )
        return self

    @classmethod
    def from_file(cls, path: Path, *, start: datetime, end: datetime, **kwargs) -> "SearchParams":
        return cls(area=cls._load_geometry(path), start=start, end=end)  # type:ignore


class ConversionParams(AreaParams):
    target_crs: Annotated[str, BeforeValidator(validate_crs)]  # Store as string, convert on demand
    source_crs: Annotated[str | None, BeforeValidator(validate_crs)] = None
    datasets: list[str] | None = None
    resolution: int | None = None

    @classmethod
    def from_file(
        cls,
        path: Path,
        *,
        target_crs: str | CRS,
        source_crs: str | CRS | None = None,
        datasets: list[str] | None = None,
        resolution: int | None = None,
        **kwargs,
    ) -> "ConversionParams":
        if isinstance(target_crs, CRS):
            target_crs = target_crs.to_string()
        if source_crs and isinstance(source_crs, CRS):
            source_crs = source_crs.to_string()
        return cls(
            area=cls._load_geometry(path),  # type: ignore
            target_crs=target_crs,
            source_crs=source_crs,
            datasets=datasets,
            resolution=resolution,
        )

    @property
    def target_crs_obj(self) -> CRS:
        # forced to string by validator
        return CRS.from_string(cast(str, self.target_crs))

    @property
    def source_crs_obj(self) -> CRS | None:
        # forced to string by validator
        return CRS.from_string(cast(str, self.source_crs)) if self.source_crs else None


class ProductInfo(BaseModel):
    instrument: str
    level: str
    product_type: str
    acquisition_time: datetime


class Granule(BaseModel):
    granule_id: str
    source: str
    assets: dict[str, Any]  # 'Any' must be serializable
    info: ProductInfo
    local_path: Path | None = None

    @classmethod
    def from_file(cls, path: Path) -> "Granule":
        file_path = path / GRANULE_METADATA_FILENAME
        with open(file_path, "r") as f:
            return cls.model_validate_json(f.read())

    def to_file(self, path: Path) -> None:
        file_path = path / GRANULE_METADATA_FILENAME
        with open(file_path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    def __str__(self) -> str:
        return f"Granule(id={self.granule_id})"


class ProgressEventType(Enum):
    TASK_CREATED = "task_created"
    TASK_DURATION = "task_duration"
    TASK_PROGRESS = "task_progress"
    TASK_COMPLETED = "task_completed"
    BATCH_STARTED = "batch_started"
    BATCH_COMPLETED = "batch_completed"


class ProgressEvent(BaseModel):
    type: ProgressEventType
    task_id: str
    data: dict[str, Any]
