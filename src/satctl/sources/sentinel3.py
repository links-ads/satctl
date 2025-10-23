import logging
import re
import warnings
from abc import abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from pydantic import BaseModel
from pystac_client import Client

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.utils import extract_zip
from satctl.writers import Writer

log = logging.getLogger(__name__)

# Constants
DEFAULT_SEARCH_LIMIT = 100


class S3Asset(BaseModel):
    href: str
    media_type: str | None


class Sentinel3Source(DataSource):
    """Base source for Sentinel-3 products"""

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        downloader: Downloader,
        stac_url: str,
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
        self.stac_url = stac_url
        self.search_limit = search_limit
        self.download_pool_conns = download_pool_conns
        self.download_pool_size = download_pool_size
        warnings.filterwarnings(action="ignore", category=UserWarning)

    # ============================================================================
    # Abstract methods
    # ============================================================================

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo: ...

    # ============================================================================
    # Search operations
    # ============================================================================

    def search(self, params: SearchParams) -> list[Granule]:
        log.debug("Setting up the STAC client")
        catalogue = Client.open(self.stac_url)

        log.debug("Searching catalog")
        search = catalogue.search(
            collections=self.collections,
            intersects=params.area_geometry,
            datetime=(params.start, params.end),
            max_items=self.search_limit,
        )
        items = [
            Granule(
                granule_id=i.id,
                source=self.collections[0],
                assets={k: S3Asset(href=v.href, media_type=v.media_type) for k, v in i.assets.items()},
                info=self._parse_item_name(i.id),
            )
            for i in search.items()
        ]
        log.debug("Found %d items", len(items))
        return items

    # ============================================================================
    # Retrieval operations
    # ============================================================================

    def get_by_id(self, item_id: str, **kwargs) -> Granule:
        raise NotImplementedError()

    def get_files(self, item: Granule) -> list[Path | str]:
        if item.local_path is None:
            raise ValueError(
                f"Resource not found: granule '{item.granule_id}' has no local_path "
                "(download the granule first using download_item())"
            )
        return list(item.local_path.glob("*"))

    # ============================================================================
    # Validation operations
    # ============================================================================

    def validate(self, item: Granule) -> None:
        """Validates a Sentinel3 STAC item.

        Args:
            item (Granule): STAC item to validate
        """
        for name, asset in item.assets.items():
            asset = cast(S3Asset, asset)
            # We expect zips, netcdfs, xfdumanifest.xml and thumbnail.jpg
            assert asset.media_type in ("application/netcdf", "application/zip", "image/jpeg", "application/xml")
            # The zip is our main interest
            if asset.media_type == "application/zip":
                assert name == "product"
            # Check that we have a manifest file
            if asset.media_type == "application/xml":
                assert name == "xfdumanifest"

    # ============================================================================
    # Download operations
    # ============================================================================

    def download_item(self, item: Granule, destination: Path) -> bool:
        """Download single item - can be called in thread pool."""
        self.validate(item)
        zip_asset = cast(S3Asset, item.assets["product"])
        local_file = destination / f"{item.granule_id}.zip"
        if result := self.downloader.download(
            uri=zip_asset.href,
            destination=local_file,
            item_id=item.granule_id,
        ):
            # extract to uniform with other sources
            local_path = extract_zip(
                zip_path=local_file,
                extract_to=destination,
                item_id=item.granule_id,
                expected_dir=f"{item.granule_id}.SEN3",
            )
            item.local_path = local_path
            log.debug("Saving granule metadata to: %s", local_path)
            item.to_file(local_path)
            local_file.unlink()  # delete redundant zip
        else:
            log.warning("Failed to download: %s", item.granule_id)
        return result

    # ============================================================================
    # Processing operations
    # ============================================================================

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


class SLSTRSource(Sentinel3Source):
    """Source for Sentinel-3 SLSTR L1B product."""

    def __init__(
        self,
        *,
        downloader: Downloader,
        stac_url: str,
        default_composite: str = "all_bands",
        default_resolution: int = 1000,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
        download_pool_conns: int = 10,
        download_pool_size: int = 2,
    ):
        super().__init__(
            "sentinel-3-sl-1-rbt-ntc",
            reader="slstr_l1b",
            default_composite=default_composite,
            default_resolution=default_resolution,
            downloader=downloader,
            stac_url=stac_url,
            search_limit=search_limit,
            download_pool_conns=download_pool_conns,
            download_pool_size=download_pool_size,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        pattern = r"S3([AB])_SL_(\d)_(\w+)____(\d{8}T\d{6})"
        match = re.match(pattern, name)
        if not match:
            raise ValueError(
                f"Invalid filename format: '{name}' does not match SLSTR pattern (S3X_SL_L_XXX____YYYYMMDDTHHMMSS)"
            )

        groups = match.groups()
        acquisition_time = datetime.strptime(groups[3], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return ProductInfo(
            instrument="slstr",
            level=groups[1],
            product_type=groups[2],
            acquisition_time=acquisition_time,
        )


class OLCISource(Sentinel3Source):
    """Source for Sentinel-3 OLCI L1B product."""

    def __init__(
        self,
        *,
        downloader: Downloader,
        stac_url: str,
        default_composite: str = "all_bands",
        default_resolution: int = 300,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
        download_pool_conns: int = 10,
        download_pool_size: int = 2,
    ):
        super().__init__(
            "sentinel-3-olci-1-efr-ntc",
            reader="olci_l1b",
            default_composite=default_composite,
            default_resolution=default_resolution,
            downloader=downloader,
            stac_url=stac_url,
            search_limit=search_limit,
            download_pool_conns=download_pool_conns,
            download_pool_size=download_pool_size,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        pattern = r"S3([AB])_OL_(\d)_(\w+)____(\d{8}T\d{6})"
        match = re.match(pattern, name)
        if not match:
            raise ValueError(
                f"Invalid filename format: '{name}' does not match OLCI pattern (S3X_OL_L_XXX____YYYYMMDDTHHMMSS)"
            )

        groups = match.groups()
        acquisition_time = datetime.strptime(groups[3], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return ProductInfo(
            instrument="olci",
            level=groups[1],
            product_type=groups[2],
            acquisition_time=acquisition_time,
        )
