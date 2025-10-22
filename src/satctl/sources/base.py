import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, cast

from pyproj import CRS, Transformer
from pyresample import create_area_def
from pyresample.geometry import AreaDefinition, SwathDefinition
from satpy.scene import Scene
from shapely import Polygon

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, ProgressEventType, SearchParams
from satctl.progress.events import emit_event
from satctl.writers import Writer

log = logging.getLogger(__name__)


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
    def get_by_id(self, item_id: str, **kwargs) -> Granule: ...

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
    def save_item(
        self,
        item: Granule,
        destination: Path,
        writer: Writer,
        params: ConversionParams,
        force: bool = False,
    ) -> dict[str, list]: ...

    def get_downloader_init_kwargs(self) -> dict:
        """Hook method for subclasses to provide downloader initialization arguments.

        Override this method in subclasses to pass custom arguments to downloader.init().

        Returns:
            dict: Keyword arguments to pass to downloader.init()
        """
        return {}

    def download(
        self,
        items: Granule | list[Granule],
        destination: Path,
        num_workers: int | None = None,
    ) -> tuple[list, list]:
        # check output folder exists, make sure items is iterable
        destination.mkdir(parents=True, exist_ok=True)
        if not isinstance(items, Iterable):
            items = [items]
        items = cast(list, items)

        success = []
        failure = []
        num_workers = num_workers or 1
        batch_id = str(uuid.uuid4())
        emit_event(
            ProgressEventType.BATCH_STARTED,
            task_id=batch_id,
            total_items=len(items),
            description=self.collections[0],
        )
        self.downloader.init(**self.get_downloader_init_kwargs())
        executor = None
        try:
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                future2item = {executor.submit(self.download_item, item, destination): item for item in items}
                for future in as_completed(future2item):
                    item = future2item[future]
                    result = future.result()
                    if result:
                        success.append(item)
                    else:
                        failure.append(item)
            emit_event(
                ProgressEventType.BATCH_COMPLETED,
                task_id=batch_id,
                success_count=len(success),
                failure_count=len(failure),
            )
            return success, failure
        except KeyboardInterrupt:
            log.info("Interrupted, cleaning up...")
            if executor:
                executor.shutdown(wait=False, cancel_futures=True)
        finally:
            emit_event(
                ProgressEventType.BATCH_COMPLETED,
                task_id=batch_id,
                success_count=len(success),
                failure_count=len(failure),
            )
            return success, failure

    def load_scene(
        self,
        item: Granule,
        datasets: list[str] | None = None,
        generate: bool = False,
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

    def get_finest_resolution(self, scene: Scene) -> int:
        """
        Scan all datasets and return smallest resolution.
        """
        resolutions = [ds.attrs.get("resolution") for ds in scene.values()]
        resolutions = [r for r in resolutions if r is not None]
        return min(resolutions)

    def define_area(
        self,
        *,
        area: Polygon | None = None,
        scene: Scene | None = None,
        target_crs: CRS,
        source_crs: CRS | None = None,
        resolution: int | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> AreaDefinition:
        """Create area definition for resampling.

        When area is None and scene is provided, creates area covering full
        scene extent at finest available resolution.

        Args:
            area(Polygon, optional): Optional polygon defining custom extents
            scene (Scene, optional): Optional scene to extract extent from
            target_crs (CRS): Target coordinate reference system
            source_crs (CRS, optional): Source CRS (defaults to EPSG:4326)
            resolution (int, optional): Resolution in CRS units (defaults to finest available)
            name (str, optional): Area name
            description (str, optional): Area description

        Returns:
            AreaDefinition: area required for resampling
        """
        if area:
            bounds = area.bounds
        elif scene:
            area_def = scene.finest_area()
            if isinstance(area_def, SwathDefinition):
                import dask.array as da

                # extract bounds from swath lon/lat arrays
                lons, lats = area_def.lons, area_def.lats
                if isinstance(lons.data, da.Array):
                    lon_min = float(lons.min().compute())
                    lon_max = float(lons.max().compute())
                    lat_min = float(lats.min().compute())
                    lat_max = float(lats.max().compute())
                else:
                    lon_min = float(lons.min())
                    lon_max = float(lons.max())
                    lat_min = float(lats.min())
                    lat_max = float(lats.max())
                bounds = (lon_min, lat_min, lon_max, lat_max)
            elif isinstance(area_def, AreaDefinition):
                # use area extent directly
                bounds = area_def.area_extent
            else:
                raise ValueError(f"Unsupported area type: {type(area_def)}")
        else:
            raise ValueError("Provide either 'area' or 'scene'")

        # determine resolution (use finest if not specified)
        if resolution is None:
            if scene:
                resolution = self.get_finest_resolution(scene)
            elif self.default_resolution:
                resolution = self.default_resolution
            else:
                raise ValueError("Cannot determine resolution, please provide it manually.")

        # transform bounds to target CRS
        source_crs = source_crs or CRS.from_epsg(4326)
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        min_x, min_y = transformer.transform(bounds[0], bounds[1])
        max_x, max_y = transformer.transform(bounds[2], bounds[3])

        if target_crs.is_geographic:
            # Resolution is in meters, but CRS is degrees
            # Approximate: 1 degree ≈ 111km at equator
            units = "degrees"
            resolution_degrees = resolution / 111000.0
            width = int(round((max_x - min_x) / resolution_degrees))
            height = int(round((max_y - min_y) / resolution_degrees))
        else:
            # Projected CRS - resolution already in correct units
            units = "metres"
            width = int(round((max_x - min_x) / resolution))
            height = int(round((max_y - min_y) / resolution))
        width = max(1, width)
        height = max(1, height)

        # Create concrete AreaDefinition
        area_def = create_area_def(
            name or f"{self.source_name}-area",
            target_crs,
            area_extent=[min_x, min_y, max_x, max_y],
            width=width,
            height=height,
            units=units,
            description=description,
        )
        return cast(AreaDefinition, area_def)

    def save(
        self,
        items: Granule | list[Granule],
        params: ConversionParams,
        destination: Path,
        writer: Writer,
        num_workers: int | None = None,
        force: bool = False,
    ) -> tuple[list, list]:
        if not isinstance(items, Iterable):
            items = [items]
        items = cast(list, items)

        success = []
        failure = []
        num_workers = num_workers or 1
        batch_id = str(uuid.uuid4())

        emit_event(
            ProgressEventType.BATCH_STARTED,
            task_id=batch_id,
            total_items=len(items),
            description=self.source_name,
        )

        executor = None
        try:
            with ProcessPoolExecutor(max_workers=num_workers) as executor:
                future2item = {
                    executor.submit(
                        self.save_item,
                        item,
                        destination,
                        writer,
                        params,
                        force,
                    ): item
                    for item in items
                }
                for future in as_completed(future2item):
                    item = future2item[future]
                    if future.result():
                        success.append(item)
                    else:
                        failure.append(item)

            emit_event(
                ProgressEventType.BATCH_COMPLETED,
                task_id=batch_id,
                success_count=len(success),
                failure_count=len(failure),
            )
        except KeyboardInterrupt:
            log.info("Interrupted, cleaning up...")
            if executor:
                executor.shutdown(wait=False, cancel_futures=True)
        finally:
            emit_event(
                ProgressEventType.BATCH_COMPLETED,
                task_id=batch_id,
                success_count=len(success),
                failure_count=len(failure),
            )
            if executor:
                executor.shutdown()

        return success, failure

    def _validate_save_inputs(self, item: Granule, params: ConversionParams) -> None:
        """Validate inputs for save_item operation.

        Args:
            item: Granule to process
            params: Conversion parameters

        Raises:
            FileNotFoundError: If item.local_path is None or doesn't exist
            ValueError: If both params.datasets and default_composite are None
        """
        if item.local_path is None or not item.local_path.exists():
            raise FileNotFoundError(f"Invalid source file or directory: {item.local_path}")
        if params.datasets is None and self.default_composite is None:
            raise ValueError("Missing datasets or default composite for storage")

    def _prepare_datasets(self, writer: Writer, params: ConversionParams) -> dict[str, str]:
        """Parse and prepare datasets dictionary from params or defaults.

        Args:
            writer: Writer instance for parsing datasets
            params: Conversion parameters

        Returns:
            Dictionary mapping dataset names to file names
        """
        datasets_dict = writer.parse_datasets(params.datasets or self.default_composite)
        log.debug("Attempting to save the following datasets: %s", datasets_dict)
        return datasets_dict

    def _filter_existing_files(
        self,
        datasets_dict: dict[str, str],
        destination: Path,
        granule_id: str,
        writer: Writer,
        force: bool,
    ) -> dict[str, str]:
        """Remove datasets that already exist unless force=True.

        Args:
            datasets_dict: Dictionary of dataset names to file names
            destination: Base destination directory
            granule_id: Granule identifier for subdirectory
            writer: Writer instance for file extension
            force: If True, don't filter existing files

        Returns:
            Filtered dictionary of datasets to process
        """
        if force:
            return datasets_dict

        filtered = {}
        for dataset_name, file_name in datasets_dict.items():
            output_path = destination / granule_id / f"{file_name}.{writer.extension}"
            if not output_path.exists():
                filtered[dataset_name] = file_name
        return filtered

    def _create_area_from_params(
        self,
        params: ConversionParams,
        scene: Scene | None = None,
    ) -> AreaDefinition:
        """Create area definition from conversion params, using scene extent if no geometry.

        Args:
            params: Conversion parameters with CRS and optional geometry
            scene: Optional scene for extracting extent when no geometry provided

        Returns:
            AreaDefinition for resampling

        Raises:
            ValueError: If scene is None when area_geometry is also None
        """
        if params.area_geometry is not None:
            return self.define_area(
                target_crs=params.target_crs_obj,
                area=params.area_geometry,
                source_crs=params.source_crs_obj,
                resolution=params.resolution,
            )
        else:
            if scene is None:
                raise ValueError("Scene required when area_geometry is None")
            return self.define_area(
                target_crs=params.target_crs_obj,
                scene=scene,
                source_crs=params.source_crs_obj,
                resolution=params.resolution,
            )

    def _write_scene_datasets(
        self,
        scene: Scene,
        datasets_dict: dict[str, str],
        destination: Path,
        granule_id: str,
        writer: Writer,
    ) -> dict[str, list]:
        """Write all datasets from scene to output files.

        Args:
            scene: Scene containing loaded datasets
            datasets_dict: Dictionary mapping dataset names to file names
            destination: Base destination directory
            granule_id: Granule identifier for subdirectory
            writer: Writer instance for output

        Returns:
            Dictionary mapping granule_id to list of output paths
        """
        from collections import defaultdict

        from xarray import DataArray

        paths: dict[str, list] = defaultdict(list)
        output_dir = destination / granule_id
        output_dir.mkdir(exist_ok=True, parents=True)

        for dataset_name, file_name in datasets_dict.items():
            output_path = output_dir / f"{file_name}.{writer.extension}"
            paths[granule_id].append(
                writer.write(
                    dataset=cast(DataArray, scene[dataset_name]),
                    output_path=output_path,
                )
            )
        return paths
