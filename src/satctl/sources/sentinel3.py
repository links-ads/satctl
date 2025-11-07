import logging
import re
from abc import abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from pydantic import BaseModel
from pystac_client import Client

from satctl.auth import AuthBuilder
from satctl.downloaders import DownloadBuilder, Downloader
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
        stac_url: str,
        auth_builder: AuthBuilder | None,
        down_builder: DownloadBuilder | None,
        default_authenticator: str | None,
        default_downloader: str | None,
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ):
        """Initialize Sentinel-3 data source.

        Args:
            collection_name (str): Name of the Sentinel-3 collection
            reader (str): Satpy reader name for this product type
            stac_url (str): URL of the STAC catalog API endpoint
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None.
            default_downloader (str | None): Default downloader name to use when down_builder is None.
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
        self.stac_url = stac_url
        self.search_limit = search_limit

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse Sentinel-3 item name into product information.

        Args:
            name (str): Sentinel-3 item identifier

        Returns:
            ProductInfo: Parsed product metadata
        """
        ...

    def search(self, params: SearchParams) -> list[Granule]:
        """Search for Sentinel-3 data using STAC catalog.

        Args:
            params (SearchParams): Search parameters including time range and area

        Returns:
            list[Granule]: List of matching granules with metadata and assets
        """
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

    def get_by_id(self, item_id: str, **kwargs) -> Granule:
        """Get specific Sentinel-3 granule by ID.

        Args:
            item_id (str): Granule identifier
            **kwargs: Additional keyword arguments (unused)

        Returns:
            Granule: Requested granule with metadata

        Raises:
            ValueError: If granule not found
        """
        log.debug("Fetching Sentinel-3 granule by ID: %s", item_id)
        catalogue = Client.open(self.stac_url)

        try:
            collection = catalogue.get_collection(self.collections[0])
            stac_item = collection.get_item(item_id)
            if stac_item is None:
                raise ValueError(f"No granule found with id: {item_id}")
        except Exception as e:
            log.error("Failed to fetch granule %s: %s", item_id, e)
            raise ValueError(f"No granule found with id: {item_id}") from e

        return Granule(
            granule_id=stac_item.id,
            source=self.collections[0],
            assets={k: S3Asset(href=v.href, media_type=v.media_type) for k, v in stac_item.assets.items()},
            info=self._parse_item_name(stac_item.id),
        )

    def get_files(self, item: Granule) -> list[Path | str]:
        """Get list of files for a downloaded Sentinel-3 granule.

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

    def download_item(self, item: Granule, destination: Path, downloader: Downloader) -> bool:
        """Download single Sentinel-3 item and extract to destination.

        Downloads the product ZIP file, extracts it, saves metadata, and removes the ZIP.
        Can be called in thread pool for parallel downloads.

        Args:
            item (Granule): Granule to download
            destination (Path): Directory to save extracted files

        Returns:
            bool: True if download succeeded, False otherwise
        """
        self.validate(item)
        zip_asset = cast(S3Asset, item.assets["product"])
        local_file = destination / f"{item.granule_id}.zip"

        if result := downloader.download(
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
        stac_url: str,
        auth_builder: AuthBuilder | None,
        down_builder: DownloadBuilder | None,
        default_authenticator: str | None = "odata",
        default_downloader: str | None = "http",
        default_composite: str = "all_bands",
        default_resolution: int = 1000,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ):
        """Initialize Sentinel-3 SLSTR data source.

        Args:
            stac_url (str): STAC catalog URL for searching
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None. Defaults to "odata".
            default_downloader (str | None): Default downloader name to use when down_builder is None. Defaults to "http".
            default_composite (str): Default composite/band to load. Defaults to "all_bands".
            default_resolution (int): Default resolution in meters. Defaults to 1000.
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
        """
        super().__init__(
            "sentinel-3-sl-1-rbt-ntc",
            reader="slstr_l1b",
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
            stac_url=stac_url,
            search_limit=search_limit,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse SLSTR item name into product information.

        Args:
            name (str): SLSTR item identifier (e.g., "S3A_SL_1_RBT____20251024T123456")

        Returns:
            ProductInfo: Parsed product metadata

        Raises:
            ValueError: If name format is invalid
        """
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
        stac_url: str,
        auth_builder: AuthBuilder | None,
        down_builder: DownloadBuilder | None,
        default_authenticator: str | None = "odata",
        default_downloader: str | None = "http",
        default_composite: str = "all_bands",
        default_resolution: int = 300,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ):
        """Initialize Sentinel-3 OLCI data source.

        Args:
            stac_url (str): STAC catalog URL for searching
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None. Defaults to "odata".
            default_downloader (str | None): Default downloader name to use when down_builder is None. Defaults to "http".
            default_composite (str): Default composite/band to load. Defaults to "all_bands".
            default_resolution (int): Default resolution in meters. Defaults to 300.
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
        """
        super().__init__(
            "sentinel-3-olci-1-efr-ntc",
            reader="olci_l1b",
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
            stac_url=stac_url,
            search_limit=search_limit,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse OLCI item name into product information.

        Args:
            name (str): OLCI item identifier (e.g., "S3A_OL_1_EFR____20251024T123456")

        Returns:
            ProductInfo: Parsed product metadata

        Raises:
            ValueError: If name format is invalid
        """
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
