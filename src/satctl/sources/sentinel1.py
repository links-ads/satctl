import logging
import re
import warnings
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel
from pystac_client import Client
from satpy.scene import Scene
from xarray import DataArray

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.writers import Writer

log = logging.getLogger(__name__)


class S1Asset(BaseModel):
    href: str
    media_type: str | None


class Sentinel1Source(DataSource):
    """Source for Sentinel-1 product."""

    # Static class variables for required assets
    REQUIRED_ASSETS: set[str] = {
        "vv",
        "vh"
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
        downloader: Downloader,
        stac_url: str,
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = 100,
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

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo: ...

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
                assets={k: S1Asset(href=v.href, media_type=v.media_type) for k, v in i.assets.items()},
                info=self._parse_item_name(i.id),
            )
            for i in search.items()
        ]
        log.debug("Found %d items", len(items))
        return items

    def get_by_id(self, item_id: str) -> Granule:
        raise NotImplementedError()

    def get_files(self, item: Granule) -> list[Path | str]:
        if item.local_path is None:
            raise ValueError("Local path is missing. Did you download this granule?")
        # Check if SAFE structure exists
        granule_dir = item.local_path / "GRANULE"
        manifest_file = item.local_path / "manifest.safe"

        if granule_dir.exists() and manifest_file.exists():
            # SAFE structure detected - return all files recursively
            # Filter out directories and non-data files
            all_files = [f for f in item.local_path.rglob("*") if f.is_file()]
            # Exclude _granule.json metadata file
            all_files = [f for f in all_files if f.name != "_granule.json"]
            return all_files
        else:
            raise ValueError("SAFE structure not found")

    def validate(self, item: Granule) -> None:
        """Validates a Sentinel1 STAC item.

        Args:
            item (Granule): STAC item to validate
        """
        for name, asset in item.assets.items():
            asset = cast(S1Asset, asset)
            # We expect zips, jp2s, xmls, and other image formats
            assert asset.media_type in (
                "application/zip",
                "application=geotiff",
                "image/jp2",
                "image/tiff",
                "image/jpeg",
                "application/xml",
                "application/json",
                "text/plain",
            )

    def load_scene(
        self,
        item: Granule,
        datasets: list[str] | None = None,
        generate: bool = False,
        calibration: str = "counts",
        **scene_options: dict[str, Any],
    ) -> Scene:
        """Load a Sentinel-1 scene with specified calibration.

        Args:
            item (Granule): Granule to load
            datasets (list[str] | None): List of datasets/composites to load
            generate (bool): Whether to generate composites
            calibration (str): Calibration type - 'counts' (DN 0-10000, default) or 'reflectance' (percentage 0-100%)
            **scene_options: Additional scene options

        Returns:
            Scene: Loaded satpy Scene object
        """
        if not datasets:
            if self.default_composite is None:
                raise ValueError("Please provide the source with a default composite, or provide custom composites")
            datasets = [self.default_composite]
        scene = Scene(
            filenames=self.get_files(item),
            reader=self.reader,
            reader_kwargs=scene_options,
        )
        # Load with specified calibration
        scene.load(datasets, calibration=calibration)
        return scene

    def download_item(self, item: Granule, destination: Path) -> bool:
        """Download only the specified assets to destination/item.granule_id.

        Args:
            item (Granule): Sentinel-1 SAR product to download.
            destination (Path): Path to the destination directory.

        Returns:
            bool: True if all specified assets were downloaded successfully, False otherwise.
        """
        self.validate(item)

        # Create directory with .SAFE extension for sar-c_safe reader compatibility
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
            asset = cast(S1Asset, asset)

            # Extract the relative path from S3 URI to preserve SAFE structure
            href_parts = asset.href.split(".SAFE/")
            if len(href_parts) > 1 and "GRANULE" in href_parts[1]:
                # Preserve the SAFE structure for proper sar-c_safe reader support
                relative_path = href_parts[1] 
                target_file = local_path / relative_path
            else:
                # Fallback to flat structure if pattern not found
                target_file = local_path / (asset_name + Path(asset.href).suffix)

            target_file.parent.mkdir(parents=True, exist_ok=True)
            result = self.downloader.download(
                uri=asset.href,
                destination=target_file,
                item_id=item.granule_id,
            )
            if not result:
                log.warning("Failed to download asset %s for granule %s", asset_name, item.granule_id)
                all_success = False

        # Download metadata files required by sar-c_safe reader
        for metadata_name in self.METADATA_ASSETS:
            metadata = item.assets.get(metadata_name)
            if metadata is None:
                log.debug("Missing metadata '%s' for granule %s", metadata_name, item.granule_id)
                continue
            metadata = cast(S1Asset, metadata)

            # Extract relative path from S3 URI
            href_parts = metadata.href.split(".SAFE/")
            if len(href_parts) > 1:
                relative_path = href_parts[1]
                target_file = local_path / relative_path
            else:
                # Fallback
                target_file = local_path / Path(metadata.href).name

            target_file.parent.mkdir(parents=True, exist_ok=True)
            result = self.downloader.download(
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

    def save_item(
        self,
        item: Granule,
        destination: Path,
        writer: Writer,
        params: ConversionParams,
        force: bool = False,
    ) -> dict[str, list]:
        if item.local_path is None or not item.local_path.exists():
            raise FileNotFoundError(f"Invalid source file or directory: {item.local_path}")
        if params.datasets is None and self.default_composite is None:
            raise ValueError("Missing datasets or default composite for storage")

        datasets_dict = writer.parse_datasets(params.datasets or self.default_composite)
        log.debug("Attempting to save the following datasets: %s", datasets_dict)
        # if not forced or already present,
        # remove existing files from the process before loading scene
        if not force:
            for dataset_name, file_name in list(datasets_dict.items()):
                if (destination / item.granule_id / f"{file_name}.{writer.extension}").exists():
                    del datasets_dict[dataset_name]

        files = self.get_files(item)
        log.debug("Found %d files to process", len(files))

        log.debug("Loading and resampling scene")
        scene = self.load_scene(item, datasets=list(datasets_dict.values()))
        # if user does not provide an AoI, we use the entire granule extent
        # similarly, if a user does not provide: source CRS, resolution, datasets,
        # `define_area` will assume some sane defaults (4326, default_res or finest, default_composite)
        if params.area_geometry is not None:
            area_def = self.define_area(
                target_crs=params.target_crs_obj,
                area=params.area_geometry,
                source_crs=params.source_crs_obj,
                resolution=params.resolution,
            )
        else:
            area_def = self.define_area(
                target_crs=params.target_crs_obj,
                scene=scene,
                source_crs=params.source_crs_obj,
                resolution=params.resolution,
            )
        scene = self.resample(scene, area_def=area_def)

        paths: dict[str, list] = defaultdict(list)
        output_dir = destination / item.granule_id
        output_dir.mkdir(exist_ok=True, parents=True)
        for dataset_name, file_name in datasets_dict.items():
            output_path = output_dir / f"{file_name}.{writer.extension}"
            paths[item.granule_id].append(
                writer.write(
                    dataset=cast(DataArray, scene[dataset_name]),
                    output_path=output_path,
                )
            )
        return paths


class Sentinel1GRDSource(Sentinel1Source):
    """Source for Sentinel-1 GRD product."""
    def __init__(
        self,
        *,
        downloader: Downloader,
        stac_url: str,
        composite: str = "true_color",
        search_limit: int = 100,
        download_pool_conns: int = 10,
        download_pool_size: int = 2,
    ):
        super().__init__(
            "sentinel-1-grd",
            reader="sar-c_safe",
            default_composite=composite,
            default_resolution=20,
            downloader=downloader,
            stac_url=stac_url,
            search_limit=search_limit,
            download_pool_conns=download_pool_conns,
            download_pool_size=download_pool_size,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse Sentinel-1 GRD .SAFE directory name to extract metadata.

        Expected format: S1A_EW_GRDM_1SXX_YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS_XXXXXX_XXXXXX_XXXX_COG.SAFE
        Example: S1A_EW_GRDM_1SDH_20250915T081809_20250915T081914_060996_079982_90C1_COG.SAFE.zip
        """
        pattern = r"(S1[ABC])_([A-Z]{2})_([A-Z]{4})_1S[A-Z]{2}_(\d{8}T\d{6})_"

        match = re.match(pattern, name)
        if not match:
            raise ValueError(
                f"Invalid Sentinel-1 .SAFE directory format: {name}")

        groups = match.groups()
        satellite = groups[0]  # A or B or C
        acquisition_mode = groups[1]  # EW, IW, etc.
        level = groups[2]  # GRD, RTC.
        sensing_time = groups[3]  # YYYYMMDDTHHMMSS

        acquisition_time = datetime.strptime(
            sensing_time, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)

        return ProductInfo(
            instrument="sar",
            level=level,
            product_type=f"S1{satellite}",
            acquisition_time=acquisition_time,
        )