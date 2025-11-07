import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, cast

import numpy as np
from pyproj import CRS, Transformer
from pyresample import create_area_def
from pyresample.geometry import AreaDefinition, SwathDefinition
from satpy.scene import Scene
from shapely import Polygon

from satctl.auth import AuthBuilder
from satctl.auth.base import Authenticator
from satctl.downloaders import DownloadBuilder, Downloader
from satctl.model import ConversionParams, Granule, ProgressEventType, SearchParams
from satctl.progress.events import emit_event
from satctl.writers import Writer

log = logging.getLogger(__name__)


class DataSource(ABC):
    """Abstract base class for all satellite data sources."""

    def __init__(
        self,
        name: str,
        auth_builder: AuthBuilder | None,
        down_builder: DownloadBuilder | None,
        default_authenticator: str | None = None,
        default_downloader: str | None = None,
        default_resolution: int | None = None,
        default_composite: str | None = None,
    ):
        """Initialize data source.

        Args:
            name (str): Source name identifier
            auth_builder (AuthBuilder): Factory that creates an authenticator object on demand.
            down_builder (DownloadBuilder): Factory that creates a downloader object on demand.
            default_resolution (int | None): Default resolution in meters. Defaults to None.
            default_composite (str | None): Default composite/dataset to load. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None. Defaults to None.
            default_downloader (str | None): Default downloader name to use when down_builder is None. Defaults to None.

        """
        self.source_name = name
        self.default_composite = default_composite
        self.default_resolution = default_resolution
        self.reader = None

        if auth_builder is None:
            from satctl.auth import configure_authenticator

            if default_authenticator is None:
                raise ValueError(f"Authentication not configured for source: {name}")
            auth_builder = configure_authenticator(default_authenticator)

        if down_builder is None:
            from satctl.downloaders import configure_downloader

            if default_downloader is None:
                raise ValueError(f"Downloader not configured for source: {name}")
            down_builder = configure_downloader(default_downloader)

        self.auth_builder = auth_builder
        self.down_builder = down_builder
        # lazy init to avoid attribute errors
        self._authenticator: Authenticator | None = None
        self._downloader: Downloader | None = None

    @property
    def collections(self) -> list[str]:
        """Get list of collection identifiers.

        Returns:
            list[str]: List containing the source name
        """
        return [self.source_name]

    @abstractmethod
    def search(self, params: SearchParams) -> list[Granule]:
        """Search for granules matching search parameters.

        Args:
            params (SearchParams): Search parameters including time range and area

        Returns:
            list[Granule]: List of matching granules
        """
        ...

    @abstractmethod
    def get_by_id(self, item_id: str, **kwargs: Any) -> Granule:
        """Retrieve a specific granule by its identifier.

        Args:
            item_id (str): Granule identifier
            **kwargs (Any): Additional keyword arguments

        Returns:
            Granule: The requested granule
        """
        ...

    @abstractmethod
    def get_files(self, item: Granule) -> list[Path | str]:
        """Get list of files for a granule.

        Args:
            item (Granule): Granule to get files for

        Returns:
            list[Path | str]: List of file paths
        """
        ...

    @abstractmethod
    def validate(self, item: Granule) -> None:
        """Validate a granule's structure and assets.

        Args:
            item (Granule): Granule to validate

        Raises:
            ValidationError: If granule is invalid
        """
        ...

    @abstractmethod
    def download_item(
        self,
        item: Granule,
        destination: Path,
        downloader: Downloader,
    ) -> bool:
        """Download a single granule.

        Args:
            item (Granule): Granule to download
            destination (Path): Base destination directory
            downloader (Downloader): Downloader instance to use for downloading

        Returns:
            bool: True if download succeeded, False otherwise
        """
        ...

    @abstractmethod
    def save_item(
        self,
        item: Granule,
        destination: Path,
        writer: Writer,
        params: ConversionParams,
        force: bool = False,
    ) -> dict[str, list]:
        """Save granule item to output files after processing.

        Args:
            item (Granule): Granule to process
            destination (Path): Base destination directory
            writer (Writer): Writer instance for output
            params (ConversionParams): Conversion parameters
            force (bool): If True, overwrite existing files. Defaults to False.

        Returns:
            dict[str, list]: Dictionary mapping granule_id to list of output paths
        """
        ...

    def authenticator(self, **overrides: dict) -> Authenticator:
        if not self._authenticator:
            self._authenticator = self.auth_builder(**overrides)
        return cast(Authenticator, self._authenticator)

    def downloader(self, **overrides: dict) -> Downloader:
        if not self._downloader:
            self._downloader = self.down_builder(**overrides)
        return cast(Downloader, self._downloader)

    def download(
        self,
        items: Granule | list[Granule],
        destination: Path,
        num_workers: int | None = None,
    ) -> tuple[list, list]:
        """Download one or more granules with parallel processing.

        Args:
            items (Granule | list[Granule]): Single granule or list of granules to download
            destination (Path): Base destination directory
            num_workers (int | None): Number of parallel workers. Defaults to 1.

        Returns:
            tuple[list, list]: Tuple of (successful_items, failed_items)
        """
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
        # Initialize downloader
        downloader = self.downloader()
        downloader.init(self.authenticator())
        executor = None
        try:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_item_map = {
                    executor.submit(
                        self.download_item,
                        item,
                        destination,
                        downloader,
                    ): item
                    for item in items
                }
                for future in as_completed(future_to_item_map):
                    item = future_to_item_map[future]
                    result = future.result()
                    if result:
                        success.append(item)
                    else:
                        failure.append(item)
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
        **scene_options: Any,
    ) -> Scene:
        """Load a satpy Scene from granule files.

        Args:
            item (Granule): Granule to load
            datasets (list[str] | None): List of datasets/composites to load. Defaults to None (uses default_composite).
            generate (bool): Whether to generate composites. Defaults to False.
            **scene_options (Any): Additional keyword arguments passed to Scene reader

        Returns:
            Scene: Loaded satpy Scene object

        Raises:
            ValueError: If datasets is None and no default_composite is set
        """
        if not datasets:
            if self.default_composite is None:
                raise ValueError(
                    "Invalid configuration: datasets parameter is required when no default composite is set"
                )
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
        **resample_options: Any,
    ) -> Scene:
        """Resample a Scene to a target area definition.

        Args:
            scene (Scene): Scene to resample
            area_def (AreaDefinition | None): Target area definition. Defaults to None (auto-generated).
            datasets (list[str] | None): Specific datasets to resample. Defaults to None (all datasets).
            resolution (int | None): Resolution in meters. Defaults to None (uses default_resolution).
            **resample_options (Any): Additional keyword arguments passed to scene.resample()

        Returns:
            Scene: Resampled scene
        """
        resolution = resolution or self.default_resolution
        area_def = area_def or self.define_area(
            target_crs=CRS.from_epsg(4326),
            scene=scene,
            resolution=resolution,
            name=f"{self.source_name}-area",
        )
        return scene.resample(destination=area_def, datasets=datasets, **resample_options)

    def get_finest_resolution(self, scene: Scene) -> int:
        """Scan all datasets and return smallest resolution.

        Args:
            scene (Scene): Scene to analyze

        Returns:
            int: Finest (minimum) resolution in meters across all datasets
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
            area (Polygon | None): Optional polygon defining custom extents. Defaults to None.
            scene (Scene | None): Optional scene to extract extent from. Defaults to None.
            target_crs (CRS): Target coordinate reference system
            source_crs (CRS | None): Source CRS. Defaults to None (EPSG:4326).
            resolution (int | None): Resolution in meters. Defaults to None (finest available).
            name (str | None): Area name. Defaults to None.
            description (str | None): Area description. Defaults to None.

        Returns:
            AreaDefinition: Area definition configured for resampling

        Raises:
            ValueError: If both area and scene are None or resolution cannot be determined
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
                bounds = area_def.area_extent
            else:
                raise ValueError(f"Unsupported area type: {type(area_def).__name__}")
        else:
            raise ValueError("Invalid configuration: either 'area' or 'scene' parameter must be provided")

        # determine resolution (use finest if not specified)
        if resolution is None:
            if scene:
                resolution = self.get_finest_resolution(scene)
            elif self.default_resolution:
                resolution = self.default_resolution
            else:
                raise ValueError(
                    "Invalid configuration: resolution parameter is required (cannot determine from scene or defaults)"
                )

        # transform bounds to target CRS
        source_crs = source_crs or CRS.from_epsg(4326)
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        min_x, min_y = transformer.transform(bounds[0], bounds[1])
        max_x, max_y = transformer.transform(bounds[2], bounds[3])

        if target_crs.is_geographic:
            # Geographic CRS (lat/lon): coordinates are in degrees, but resolution parameter is in meters
            # Convert meters to degrees using approximation: 1 degree ≈ 111km at equator
            # This simplification works reasonably well for moderate latitudes
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
        """Process and save one or more granules with parallel processing.

        Args:
            items (Granule | list[Granule]): Single granule or list of granules to process
            params (ConversionParams): Conversion parameters
            destination (Path): Base destination directory
            writer (Writer): Writer instance for output
            num_workers (int | None): Number of parallel workers. Defaults to 1.
            force (bool): If True, overwrite existing files. Defaults to False.

        Returns:
            tuple[list, list]: Tuple of (successful_items, failed_items)
        """
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
                future_to_item_map = {
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
                for future in as_completed(future_to_item_map):
                    item = future_to_item_map[future]
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
            item (Granule): Granule to process
            params (ConversionParams): Conversion parameters

        Raises:
            FileNotFoundError: If item.local_path is None or doesn't exist
            ValueError: If both params.datasets and default_composite are None
        """
        if item.local_path is None or not item.local_path.exists():
            raise FileNotFoundError(
                f"Resource not found: granule data at '{item.local_path}' "
                "(download the granule first using download_item())"
            )
        if params.datasets is None and self.default_composite is None:
            raise ValueError("Invalid configuration: datasets parameter is required when no default composite is set")

    def _prepare_datasets(self, writer: Writer, params: ConversionParams) -> dict[str, str]:
        """Parse and prepare datasets dictionary from params or defaults.

        Args:
            writer (Writer): Writer instance for parsing datasets
            params (ConversionParams): Conversion parameters

        Returns:
            dict[str, str]: Dictionary mapping dataset names to file names
        """
        datasets = params.datasets if params.datasets is not None else self.default_composite
        if datasets is None:
            raise ValueError("No datasets specified and no default composite configured")

        datasets_dict = writer.parse_datasets(datasets)
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
            datasets_dict (dict[str, str]): Dictionary of dataset names to file names
            destination (Path): Base destination directory
            granule_id (str): Granule identifier for subdirectory
            writer (Writer): Writer instance for file extension
            force (bool): If True, don't filter existing files

        Returns:
            dict[str, str]: Filtered dictionary of datasets to process
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
            params (ConversionParams): Conversion parameters with CRS and optional geometry
            scene (Scene | None): Optional scene for extracting extent when no geometry provided. Defaults to None.

        Returns:
            AreaDefinition: AreaDefinition for resampling

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
                raise ValueError(
                    "Invalid configuration: scene parameter is required when area_geometry is not provided"
                )
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
        dtype: type | np.dtype[Any] | None = None,
    ) -> dict[str, list]:
        """Write all datasets from scene to output files.

        Args:
            scene (Scene): Scene containing loaded datasets
            datasets_dict (dict[str, str]): Dictionary mapping dataset names to file names
            destination (Path): Base destination directory
            granule_id (str): Granule identifier for subdirectory
            writer (Writer): Writer instance for output

        Returns:
            dict[str, list]: Dictionary mapping granule_id to list of output paths
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
                    dtype=dtype,
                )
            )
        return paths
