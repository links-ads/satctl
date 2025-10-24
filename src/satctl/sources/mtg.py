import logging
import re
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from eumdac.datastore import DataStore
from pydantic import BaseModel
from satpy.scene import Scene

from satctl.downloaders import Downloader
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

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        downloader: Downloader,
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
        download_pool_conns: int = 10,
        download_pool_size: int = 2,
    ):
        super().__init__(
            collection_name,
            downloader=downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
        )
        self.reader = reader
        self.search_limit = search_limit
        self.download_pool_conns = download_pool_conns
        self.download_pool_size = download_pool_size
        warnings.filterwarnings(action="ignore", category=UserWarning)

    def _parse_item_name(self, name: str) -> ProductInfo:
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
        log.debug("Setting up the DataStore client")
        catalogue = DataStore(self.downloader.auth.auth_session)

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
        return items

    def get_by_id(self, item_id: str) -> Granule:
        raise NotImplementedError()

    def get_files(self, item: Granule) -> list[Path | str]:
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

    def download_item(self, item: Granule, destination: Path) -> bool:
        self.validate(item)
        zip_asset = cast(MTGAsset, item.assets["product"])
        local_file = destination / f"{item.granule_id}.zip"
        if result := self.downloader.download(
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
            item: Granule to process
            destination: Base destination directory
            writer: Writer instance for output
            params: Conversion parameters
            force: If True, overwrite existing files

        Returns:
            Dictionary mapping granule_id to list of output paths
        """
        # Validate inputs using base class helper
        self._validate_save_inputs(item, params)

        # Parse datasets using base class helper
        datasets_dict = self._prepare_datasets(writer, params)

        # Filter existing files using base class helper
        datasets_dict = self._filter_existing_files(datasets_dict, destination, item.granule_id, writer, force)

        # Load and resample scene
        log.debug("Loading and resampling scene")
        scene = self.load_scene(item, datasets=list(datasets_dict.values()))

        # Define area using base class helper
        area_def = self._create_area_from_params(params, scene)
        scene = self.resample(scene, area_def=area_def)

        # Write datasets using base class helper
        return self._write_scene_datasets(scene, datasets_dict, destination, item.granule_id, writer)
