from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from pyproj import CRS
from pyproj.exceptions import CRSError
from shapely import MultiPolygon, Polygon, from_geojson

from satctl.utils import convert_crs


@dataclass
class AreaParams:
    area: str | Path | Polygon | MultiPolygon

    def __post_init__(self):
        if isinstance(self.area, str):
            self.area = Path(self.area)
        if isinstance(self.area, Path):
            if not self.area.exists() or not self.area.is_file():
                raise ValueError(f"Invalid area file: {self.area}")
        elif not isinstance(self.area, (Polygon, MultiPolygon)):
            raise ValueError("Area must be either a path to a GeoJSON file or a valid [Multi]Polygon")

    @property
    def area_geometry(self) -> Polygon | MultiPolygon:
        if isinstance(self.area, Path):
            return from_geojson(self.area.read_text())  # type: ignore
        return cast(Polygon, self.area)


@dataclass
class SearchParams(AreaParams):
    start: datetime
    end: datetime

    def __post_init__(self):
        super().__post_init__()
        if self.start >= self.end:
            raise ValueError(f"Start date {self.start} comes after end date {self.end}")


@dataclass
class ConversionParams(AreaParams):
    crs_data: str | CRS

    def __post_init__(self):
        try:
            self.crs_data = convert_crs(self.crs_data)
        except CRSError:
            raise ValueError(f"The provided string is not a valid CRS: {self.crs_data}")

    @property
    def crs(self):
        return cast(CRS, self.crs_data)


@dataclass
class ProductInfo:
    instrument: str
    level: str
    product_type: str
    acquisition_time: datetime


@dataclass
class Granule:
    granule_id: str
    source: str
    assets: dict[str, Any]
    info: ProductInfo

    def __str__(self) -> str:
        return f"Granule(id={self.granule_id})"
