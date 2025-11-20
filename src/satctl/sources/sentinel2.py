import logging
import re
from abc import abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel
from pystac_client import Client
from satpy.scene import Scene

from satctl.auth import AuthBuilder
from satctl.downloaders import DownloadBuilder, Downloader
from satctl.model import Granule, ProductInfo, SearchParams
from satctl.sources import DataSource

log = logging.getLogger(__name__)


class S2Asset(BaseModel):
    href: str
    media_type: str | None


class Sentinel2Source(DataSource):
    """Source for Sentinel-2 MSI product."""

    # Static class variables for required assets
    REQUIRED_ASSETS: set[str] = {
        "AOT_10m",
        "B01_60m",
        "B02_10m",
        "B03_10m",
        "B04_10m",
        "B05_20m",
        "B06_20m",
        "B07_20m",
        "B08_10m",
        "B09_60m",
        "B10_60m",
        "B11_20m",
        "B12_20m",
        "B8A_20m",
        "WVP_20m",
    }

    # Static class variables for metadata assets
    METADATA_ASSETS: set[str] = {
        "safe_manifest",
        "granule_metadata",
        "product_metadata",
    }

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
    ):
        """Initialize Sentinel-2 source.

        Args:
            collection_name (str): Collection name identifier
            reader (str): Satpy reader name
            stac_url (str): STAC catalog URL
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None.
            default_downloader (str | None): Default downloader name to use when down_builder is None.
            default_composite (str | None): Default composite name. Defaults to None.
            default_resolution (int | None): Default resolution in meters. Defaults to None.
        """
        super().__init__(
            collection_name,
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_downloader=default_downloader,
            default_authenticator=default_authenticator,
            default_composite=default_composite,
            default_resolution=default_resolution,
        )
        self.reader = reader
        self.stac_url = stac_url

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse item name into ProductInfo.

        Args:
            name (str): Item name to parse

        Returns:
            ProductInfo: ProductInfo with extracted metadata
        """
        ...

    def search(self, params: SearchParams) -> list[Granule]:
        """Search for Sentinel-2 granules via STAC catalog.

        Args:
            params (SearchParams): Search parameters including time range and area

        Returns:
            list[Granule]: List of matching granules
        """
        log.debug("Setting up the STAC client")
        catalogue = Client.open(self.stac_url)

        log.debug("Searching catalog")
        search = catalogue.search(
            collections=self.collections,
            intersects=params.area_geometry,
            datetime=(params.start, params.end),
            max_items=params.search_limit,
        )
        items = [
            Granule(
                granule_id=stac_item.id,
                source=self.collections[0],
                assets={
                    asset_name: S2Asset(href=asset.href, media_type=asset.media_type)
                    for asset_name, asset in stac_item.assets.items()
                },
                info=self._parse_item_name(stac_item.id),
            )
            for stac_item in search.items()
        ]
        log.debug("Found %d items", len(items))
        return items

    def get_by_id(self, item_id: str, **kwargs: Any) -> Granule:
        """Retrieve a specific granule by ID.

        Args:
            item_id (str): Granule identifier
            **kwargs (Any): Additional keyword arguments (unused)

        Returns:
            Granule: The requested granule

        Raises:
            ValueError: If granule not found
        """
        log.debug("Fetching Sentinel-2 granule by ID: %s", item_id)
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
            assets={
                asset_name: S2Asset(href=asset.href, media_type=asset.media_type)
                for asset_name, asset in stac_item.assets.items()
            },
            info=self._parse_item_name(stac_item.id),
        )

    def get_files(self, item: Granule) -> list[Path | str]:
        """Get list of files from SAFE structure.

        Args:
            item (Granule): Granule with local_path set

        Returns:
            list[Path | str]: List of all files in SAFE structure

        Raises:
            ValueError: If local_path is None or SAFE structure is invalid
        """
        if item.local_path is None:
            raise ValueError(
                f"Resource not found: granule '{item.granule_id}' has no local_path "
                "(download the granule first using download_item())"
            )
        # Check if SAFE structure exists
        granule_dir = item.local_path / "GRANULE"
        manifest_file = item.local_path / "manifest.safe"

        if granule_dir.exists() and manifest_file.exists():
            # SAFE structure detected - return all files recursively
            # Filter out directories and non-data files
            all_files = [f for f in item.local_path.rglob("*") if f.is_file()]
            # Exclude _granule.json metadata file
            all_files = [f for f in all_files if f.name != "_granule.json"]
            return cast(list[Path | str], all_files)
        else:
            raise ValueError(
                f"Invalid data: SAFE structure not found in '{item.local_path}' "
                "(expected GRANULE directory and manifest.safe file)"
            )

    def validate(self, item: Granule) -> None:
        """Validate a Sentinel-2 STAC item.

        Args:
            item (Granule): STAC item to validate

        Raises:
            AssertionError: If asset media types are invalid
        """
        for name, asset in item.assets.items():
            asset = cast(S2Asset, asset)
            # We expect zips, jp2s, xmls, and other image formats
            assert asset.media_type in (
                "application/zip",
                "image/jp2",
                "image/jpeg",
                "application/xml",
                "application/json",
                "text/plain",
            )

    def load_scene(
        self,
        item: Granule,
        reader: str | None = None,
        datasets: list[str] | None = None,
        lazy: bool = False,
        **scene_options: Any,
    ) -> Scene:
        """Load a Sentinel-2 scene with specified calibration.

        Args:
            item (Granule): Granule to load
            reader (str | None): Optional custom reader for extra customization.
            datasets (list[str] | None): List of datasets/composites to load. Defaults to None (uses default_composite).
            lazy (bool): Whether to lazily return the scene without loading datasets. Defaults to False.
            **scene_options (Any): Additional keyword arguments passed to Scene reader to Scene reader

        Returns:
            Scene: Loaded satpy Scene object

        Raises:
            ValueError: If datasets is None and no default_composite is set
        """
        scene = super().load_scene(
            item,
            reader=reader,
            datasets=datasets,
            lazy=True,
            scene_options=scene_options,
        )
        # Load with specified calibration
        if not lazy:
            scene.load(datasets, calibration="counts")
        return scene

    def download_item(self, item: Granule, destination: Path, downloader: Downloader) -> bool:
        """Download required assets preserving SAFE directory structure.

        Args:
            item (Granule): Sentinel-2 MSI product to download
            destination (Path): Base destination directory

        Returns:
            bool: True if all specified assets were downloaded successfully, False otherwise
        """
        self.validate(item)

        # Satpy's msi_safe/msi_safe_l2a reader expects the standard SAFE directory structure
        # SAFE (Standard Archive Format for Europe) format requires .SAFE extension
        local_path = destination / f"{item.granule_id}.SAFE"
        local_path.mkdir(parents=True, exist_ok=True)

        all_success = True

        # Download band files and preserve SAFE directory structure
        for asset_name in self.REQUIRED_ASSETS:
            asset = item.assets.get(asset_name)
            if asset is None:
                log.warning("Missing asset '%s' for granule %s", asset_name, item.granule_id)
                all_success = False
                continue
            asset = cast(S2Asset, asset)

            # Extract the relative path from S3 URI to preserve SAFE structure
            href_parts = asset.href.split(".SAFE/")
            if len(href_parts) > 1 and "GRANULE" in href_parts[1]:
                # Preserve the SAFE structure for proper msi_safe reader support
                relative_path = href_parts[1]  # e.g., GRANULE/L2A_.../IMG_DATA/R10m/file.jp2
                target_file = local_path / relative_path
            else:
                # Fallback to flat structure if pattern not found
                target_file = local_path / (asset_name + Path(asset.href).suffix)

            target_file.parent.mkdir(parents=True, exist_ok=True)

            result = downloader.download(
                uri=asset.href,
                destination=target_file,
                item_id=item.granule_id,
            )
            if not result:
                log.warning("Failed to download asset %s for granule %s", asset_name, item.granule_id)
                all_success = False

        # Download metadata files required by msi_safe reader
        for metadata_name in self.METADATA_ASSETS:
            metadata = item.assets.get(metadata_name)
            if metadata is None:
                log.debug("Missing metadata '%s' for granule %s", metadata_name, item.granule_id)
                continue
            metadata = cast(S2Asset, metadata)

            # Extract relative path from S3 URI
            href_parts = metadata.href.split(".SAFE/")
            if len(href_parts) > 1:
                relative_path = href_parts[1]
                target_file = local_path / relative_path
            else:
                # Fallback
                target_file = local_path / Path(metadata.href).name

            target_file.parent.mkdir(parents=True, exist_ok=True)

            result = downloader.download(
                uri=metadata.href,
                destination=target_file,
                item_id=item.granule_id,
            )
            if not result:
                log.debug("Failed to download metadata %s for granule %s", metadata_name, item.granule_id)

        if all_success:
            item.local_path = local_path
            log.debug("Saving granule metadata to: %s", local_path)
            item.to_file(local_path)
        else:
            log.warning("Failed to download all required assets for: %s", item.granule_id)
        return all_success


class Sentinel2L2ASource(Sentinel2Source):
    """Source for Sentinel-2 MSI L2A product."""

    # L2A assets have resolution suffixes (_10m, _20m, _60m)
    REQUIRED_ASSETS: set[str] = {
        "AOT_10m",
        "B01_60m",
        "B02_10m",
        "B03_10m",
        "B04_10m",
        "B05_20m",
        "B06_20m",
        "B07_20m",
        "B08_10m",
        "B09_60m",
        "B11_20m",
        "B12_20m",
        "B8A_20m",
        "WVP_20m",
    }

    # L2A metadata assets
    METADATA_ASSETS: set[str] = {
        "safe_manifest",
        "granule_metadata",
        "product_metadata",
    }

    def __init__(
        self,
        *,
        stac_url: str,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str | None = "s3",
        default_downloader: str | None = "s3",
        default_composite: str = "true_color",
        default_resolution: int = 10,
        search_limit: int = 100,
    ):
        """Initialize Sentinel-2 L2A source.

        Args:
            stac_url (str): STAC catalog URL
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None. Defaults to "s3".
            default_downloader (str | None): Default downloader name to use when down_builder is None. Defaults to "s3".
            default_composite (str): Default composite name. Defaults to 'true_color'.
            default_resolution (int): Default resolution in meters. Defaults to 10.
            search_limit (int): Maximum search results. Defaults to 100.
        """
        super().__init__(
            "sentinel-2-l2a",
            reader="msi_safe_l2a",
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_downloader=default_downloader,
            default_authenticator=default_authenticator,
            default_composite=default_composite,
            default_resolution=default_resolution,
            stac_url=stac_url,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse Sentinel-2 L2A item name.

        Args:
            name (str): Item name to parse

        Returns:
            ProductInfo: Extracted product information

        Raises:
            ValueError: If name doesn't match L2A pattern
        """
        pattern = r"S2([ABC])_MSIL2A_(\d{8}T\d{6})"
        match = re.match(pattern, name)
        if not match:
            raise ValueError(
                f"Invalid filename format: '{name}' does not match Sentinel-2 L2A pattern (S2X_MSIL2A_YYYYMMDDTHHMMSS)"
            )

        groups = match.groups()
        acquisition_time = datetime.strptime(groups[1], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return ProductInfo(
            instrument="msi",
            level="2A",
            product_type="L2A",
            acquisition_time=acquisition_time,
        )


class Sentinel2L1CSource(Sentinel2Source):
    """Source for Sentinel-2 MSI L1C product."""

    # L1C assets don't have resolution suffixes and include TCI
    REQUIRED_ASSETS: set[str] = {
        "B01",
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B07",
        "B08",
        "B09",
        "B11",
        "B12",
        "B8A",
    }

    # L1C metadata assets (different from L2A)
    METADATA_ASSETS: set[str] = {
        "safe_manifest",
        "granule_metadata",
        "product_metadata",
        "datastrip_metadata",
    }

    def __init__(
        self,
        *,
        stac_url: str,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str | None = "s3",
        default_downloader: str | None = "s3",
        default_composite: str = "true_color",
        default_resolution: int = 10,
        search_limit: int = 100,
    ):
        """Initialize Sentinel-2 L1C source.

        Args:
            stac_url (str): STAC catalog URL
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None. Defaults to "s3".
            default_downloader (str | None): Default downloader name to use when down_builder is None. Defaults to "s3".
            default_composite (str): Default composite name. Defaults to 'true_color'.
            default_resolution (int): Default resolution in meters. Defaults to 10.
            search_limit (int): Maximum search results. Defaults to 100.
        """
        super().__init__(
            "sentinel-2-l1c",
            reader="msi_safe",
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
            stac_url=stac_url,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse Sentinel-2 L1C item name.

        Args:
            name (str): Item name to parse

        Returns:
            ProductInfo: Extracted product information

        Raises:
            ValueError: If name doesn't match L1C pattern
        """
        pattern = r"S2([ABC])_MSIL1C_(\d{8}T\d{6})"
        match = re.match(pattern, name)
        if not match:
            raise ValueError(
                f"Invalid filename format: '{name}' does not match Sentinel-2 L1C pattern (S2X_MSIL1C_YYYYMMDDTHHMMSS)"
            )

        groups = match.groups()
        acquisition_time = datetime.strptime(groups[1], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return ProductInfo(
            instrument="msi",
            level="1C",
            product_type="L1C",
            acquisition_time=acquisition_time,
        )
