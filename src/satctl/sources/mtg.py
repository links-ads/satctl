import logging
import re
import threading
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import dask.config
import numpy as np
from eumdac.datastore import DataStore
from pydantic import BaseModel
from satpy.scene import Scene

from satctl.auth import AuthBuilder
from satctl.downloaders import DownloadBuilder, Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.utils import extract_zip
from satctl.writers import Writer

log = logging.getLogger(__name__)

# Constants
DEFAULT_SEARCH_LIMIT = 100


class MTGAsset(BaseModel):
    href: str


class MTGSource(DataSource):
    """Source for EUMETSAT MTG product"""

    _netcdf_lock = threading.Lock()

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str = "eumetsat",
        default_downloader: str = "http",
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ):
        """Initialize MTG data source.

        Args:
            collection_name (str): Name of the MTG collection
            reader (str): Satpy reader name for this product type
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str): Default authenticator name to use when auth_builder is None. Defaults to "eumetsat".
            default_downloader (str): Default downloader name to use when down_builder is None. Defaults to "s3".
            default_composite (str | None): Default composite/band to load. Defaults to None.
            default_resolution (int | None): Default resolution in meters. Defaults to None.
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
        """
        super().__init__(
            collection_name,
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
        )
        self.reader = reader
        self.search_limit = search_limit
        warnings.filterwarnings(action="ignore", category=UserWarning)

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse MTG item name into product information.

        Args:
            name (str): MTG item identifier

        Returns:
            ProductInfo: Parsed product metadata

        Raises:
            ValueError: If name format is invalid
        """
        pattern = r"S3([AB])_OL_(\d)_(\w+)____(\d{8}T\d{6})"
        match = re.match(pattern, name)
        if not match:
            raise ValueError(
                f"Invalid filename format: '{name}' does not match expected pattern (S3X_OL_L_XXX____YYYYMMDDTHHMMSS)"
            )

        groups = match.groups()
        acquisition_time = datetime.strptime(groups[3], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return ProductInfo(
            instrument="olci",
            level=groups[1],
            product_type=groups[2],
            acquisition_time=acquisition_time,
        )

    def search(self, params: SearchParams) -> list[Granule]:
        """Search for MTG data using EUMETSAT DataStore.

        Args:
            params (SearchParams): Search parameters including time range

        Returns:
            list[Granule]: List of matching granules with metadata and assets
        """
        # Ensure authentication before searching
        log.debug("Setting up the DataStore client")
        self.authenticator().ensure_authenticated()
        catalogue = DataStore(self.authenticator().auth_session)

        log.debug("Searching catalog")
        collections = [catalogue.get_collection(c) for c in self.collections]
        items = []
        for collection in collections:
            results = collection.search(dtstart=params.start, dtend=params.end)
            items.extend(
                [
                    Granule(
                        granule_id=str(eumdac_result),
                        source=str(eumdac_result.collection),
                        assets={"product": MTGAsset(href=eumdac_result.url)},
                        info=ProductInfo(
                            instrument=eumdac_result.instrument,
                            level="",
                            product_type=eumdac_result.product_type,
                            acquisition_time=eumdac_result.sensing_end,
                        ),
                    )
                    for eumdac_result in results
                ]
            )
        log.debug("Found %d items", len(items))
        return items[: self.search_limit]

    def get_by_id(self, item_id: str, **kwargs) -> Granule:
        """Get specific MTG granule by ID.

        Args:
            item_id (str): Product identifier
            **kwargs: Additional keyword arguments (unused)

        Returns:
            Granule: Requested granule with metadata

        Raises:
            ValueError: If granule not found
        """
        # Ensure authentication before accessing DataStore
        log.debug("Fetching MTG granule by ID: %s", item_id)
        self.authenticator().ensure_authenticated()
        catalogue = DataStore(self.authenticator().auth_session)

        try:
            product = catalogue.get_product(self.collections[0], item_id)
        except Exception as e:
            log.error("Failed to fetch granule %s: %s", item_id, e)
            raise ValueError(f"No granule found with id: {item_id}") from e

        return Granule(
            granule_id=str(product),
            source=str(product.collection),
            assets={"product": MTGAsset(href=product.url)},
            info=ProductInfo(
                instrument=product.instrument,
                level="",
                product_type=product.product_type,
                acquisition_time=product.sensing_end,
            ),
        )

    def get_files(self, item: Granule) -> list[Path | str]:
        """Get list of files for a downloaded MTG granule.

        Args:
            item (Granule): Granule with local_path set

        Returns:
            list[Path | str]: List of all files in the granule directory

        Raises:
            ValueError: If local_path is not set (granule not downloaded)
        """
        if item.local_path is None:
            raise ValueError(
                f"Resource not found: granule '{item.granule_id}' has no local_path "
                "(download the granule first using download_item())"
            )
        return list(item.local_path.glob("*"))

    def load_scene(
        self,
        item: Granule,
        datasets: list[str] | None = None,
        generate: bool = False,
        **scene_options: dict[str, Any],
    ) -> Scene:
        """Load MTG data into a Satpy Scene.

        Args:
            item (Granule): Granule to load
            datasets (list[str] | None): List of dataset names to load. Defaults to None.
            generate (bool): Whether to generate composites. Defaults to False.
            **scene_options (dict[str, Any]): Additional scene options

        Returns:
            Scene: Loaded Satpy scene with requested datasets

        Raises:
            ValueError: If datasets is None and no default composite is configured
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
        # note: the data inside the FCI files is stored upside down.
        # The upper_right_corner='NE' argument flips it automatically in upright position
        scene.load(datasets, upper_right_corner="NE")
        return scene

    def validate(self, item: Granule) -> None:
        """Validates a MTG Product item.

        Args:
            item (Granule): Product item to validate
        """
        for name, asset in item.assets.items():
            asset = cast(MTGAsset, asset)
            assert "access_token=" in asset.href, "The URL does not contain the 'access_token' query parameter."

    def download_item(self, item: Granule, destination: Path, downloader: Downloader) -> bool:
        """Download single MTG item and extract to destination.

        Downloads the product ZIP file, extracts it, saves metadata, and removes the ZIP.

        Args:
            item (Granule): Granule to download
            destination (Path): Directory to save extracted files

        Returns:
            bool: True if download succeeded, False otherwise
        """
        self.validate(item)
        zip_asset = cast(MTGAsset, item.assets["product"])
        local_file = destination / f"{item.granule_id}.zip"

        if result := downloader.download(
            uri=zip_asset.href,
            destination=local_file,
            item_id=item.granule_id,
        ):
            # extract to uniform with other sources
            local_path = extract_zip(
                zip_path=local_file, extract_to=destination / f"{item.granule_id}.MTG", item_id=item.granule_id
            )
            item.local_path = local_path
            log.debug("Saving granule metadata to: %s", local_path)
            item.to_file(local_path)
            local_file.unlink()  # delete redundant zip
        else:
            log.warning("Failed to download: %s", item.granule_id)
        return result

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
        # Use synchronous dask scheduler for processing
        dask.config.set(scheduler="synchronous")
        self._validate_save_inputs(item, params)
        datasets_dict = self._prepare_datasets(writer, params)
        datasets_dict = self._filter_existing_files(datasets_dict, destination, item.granule_id, writer, force)

        with self._netcdf_lock:
            # Load and resample scene
            log.debug("Loading and resampling scene")
            scene = self.load_scene(item, datasets=list(datasets_dict.values()))

            # Define area using base class helper
            area_def = self._create_area_from_params(params, scene)
            scene = scene.compute()
            scene = self.resample(scene, area_def=area_def)

            # Write datasets using base class helper
            res = self._write_scene_datasets(scene, datasets_dict, destination, item.granule_id, writer)

        return res

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
            if "mask" in dataset_name:
                dtype = np.uint8
            else:
                dtype = np.float32
            output_path = output_dir / f"{file_name}.{writer.extension}"
            paths[granule_id].append(
                writer.write(
                    dataset=cast(DataArray, scene[dataset_name]),
                    output_path=output_path,
                    dtype=dtype,
                )
            )
        return paths
