from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, cast

from pyproj import CRS, Transformer
from pyresample import create_area_def
from pyresample.geometry import AreaDefinition
from satpy.scene import Scene
from shapely import Polygon

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, SearchParams
from satctl.writers import Writer


class DataSource(ABC):
    """
    Abstract base class for all satellite data sources.
    """

    def __init__(
        self,
        name: str,
        downloader: Downloader,
        default_resolution: int | None = None,
        default_composite: str | None = None,
    ):
        self.source_name = name
        self.downloader = downloader
        self.default_composite = default_composite
        self.default_resolution = default_resolution
        self.reader = None

    @property
    def collections(self) -> list[str]:
        return [self.source_name]

    @abstractmethod
    def search(self, params: SearchParams) -> list[Granule]: ...

    @abstractmethod
    def get_by_id(self, item_id: str) -> Granule: ...

    @abstractmethod
    def get_files(self, item: Granule) -> list[Path | str]: ...

    @abstractmethod
    def validate(self, item: Granule) -> None: ...

    @abstractmethod
    def download_item(
        self,
        item: Granule,
        destination: Path,
    ) -> bool: ...

    @abstractmethod
    def download(
        self,
        items: Granule | list[Granule],
        destination: Path,
        num_workers: int | None = None,
    ) -> tuple[list, list]: ...

    def load_scene(
        self,
        item: Granule,
        datasets: list[str] | None = None,
        **scene_options: dict[str, Any],
    ) -> Scene:
        if not datasets:
            if self.default_composite is None:
                raise ValueError("Please provide the source with a default composite, or provide custom composites")
            datasets = [self.default_composite]
        scene = Scene(
            filenames=self.get_files(item),
            reader=self.reader,
            reader_kwargs=scene_options,
        )
        scene.load(datasets)
        return scene

    def resample(
        self,
        scene: Scene,
        area_def: AreaDefinition | None = None,
        datasets: list[str] | None = None,
        resolution: int | None = None,
        **resample_options,
    ) -> Scene:
        resolution = resolution or self.default_resolution
        area_def = area_def or self.define_area(
            target_crs=CRS.from_epsg(4326),
            scene=scene,
            resolution=resolution,
            name=f"{self.source_name}-area",
        )
        return scene.resample(destination=area_def, datasets=datasets, **resample_options)

    def define_area(
        self,
        target_crs: CRS,
        area: Polygon | None = None,
        scene: Scene | None = None,
        source_crs: CRS | None = None,
        resolution: int | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> AreaDefinition:
        """Generate a pyresample AreaDefinition from a given polygon/multipolygon.

        Args:
            area (Polygon, | None, optional): area defining custom extents of the resampled output, if provided.
            scene (Scene | None, optional): alternative to custom area, gets the maximum extents from a scene.
            target_crs (pyproj.CRS): CRS to use as destination for projection.
            source_crs (pyproj.CRS, optional): CRS of the input polygon. Assumed to be `EPSG:4326` when none.
            resolution (int): custom spatial resolution (overrides default resolution if set),
                              unit is defined by the target CRS. Defaults to None.
            name (str | None, optional): name to be assigned to the definition. Defaults to None.
            description (str | None, optional): Optional description for the definition. Defaults to None.

        Returns:
            AreaDefinition: pyresample definition for satpy
        """
        if area:
            bounds = area.bounds
        elif scene:
            definition = scene.finest_area()
            lons, lats = definition.get_latlons()  # type: ignore (this is not a list)
            bounds = (lons.min(), lats.min(), lons.max(), lats.max())
        else:
            raise ValueError("Provide at least one between 'area' and 'scene'")

        resolution = resolution or self.default_resolution
        source_crs = source_crs or CRS.from_epsg(4326)
        projector = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        min_x, min_y = projector.transform(bounds[0], bounds[1])  # SW corner
        max_x, max_y = projector.transform(bounds[2], bounds[3])  # NE corner

        area_def = create_area_def(
            name,
            target_crs,
            resolution=resolution,
            area_extent=[min_x, min_y, max_x, max_y],
            units=f"{target_crs.axis_info[0].unit_name}s",  # pyresample is plural (metres, degrees)
            description=description,
        )
        return cast(AreaDefinition, area_def)

    @abstractmethod
    def save(
        self,
        items: Granule | list[Granule],
        params: ConversionParams,
        destination: Path,
        writer: Writer,
        num_workers: int | None = None,
        force: bool = False,
    ) -> tuple[list, list]: ...
