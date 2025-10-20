import logging
import re
import uuid
import warnings
from abc import abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from pydantic import BaseModel
from pystac_client import Client
from xarray import DataArray

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, ProgressEventType, SearchParams
from satctl.progress.events import emit_event
from satctl.sources import DataSource
from satctl.writers import Writer

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
                assets={k: S2Asset(href=v.href, media_type=v.media_type) for k, v in i.assets.items()},
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
        """Validates a Sentinel2 STAC item.

        Args:
            item (Granule): STAC item to validate
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

    def download_item(self, item: Granule, destination: Path) -> bool:
        """Download only the specified assets to destination/item.granule_id.

        Args:
            item (Granule): Sentinel-2 MSI product to download.
            destination (Path): Path to the destination directory.

        Returns:
            bool: True if all specified assets were downloaded successfully, False otherwise.
        """
        self.validate(item)

        # Create directory with .SAFE extension for msi_safe reader compatibility
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
            result = self.downloader.download(
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

    def download(
        self,
        items: Granule | list[Granule],
        destination: Path,
        num_workers: int | None = None,
    ) -> tuple[list, list]:
        """Download a list of Sentinel-2 MSI products.

        Args:
            items (Granule | list[Granule]): List of Sentinel-2 MSI products to download.
            destination (Path): Path to the destination directory.
            num_workers (int | None, optional): Number of workers to use for downloading. Defaults to None.

        Returns:
            tuple[list, list]: List of successfully downloaded items and list of failed items.
        """
        # check output folder exists, make sure items is iterable
        destination.mkdir(parents=True, exist_ok=True)
        if not isinstance(items, list):
            items = [items]
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
        self.downloader.init()
        executor = None
        try:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
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
        if params.datasets is None or self.default_composite is None:
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

    def save(
        self,
        items: Granule | list[Granule],
        params: ConversionParams,
        destination: Path,
        writer: Writer,
        num_workers: int | None = None,
        force: bool = False,
    ) -> tuple[list, list]:
        if not isinstance(items, list):
            items = [items]

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
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
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
            log.info("Interruped, cleaning up...")
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


class Sentinel2L2ASource(Sentinel2Source):
    """Source for Sentinel-2 MSI L2A product."""

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
            "sentinel-2-l2a",
            reader="msi_safe_l2a",
            default_composite=composite,
            default_resolution=10,
            downloader=downloader,
            stac_url=stac_url,
            search_limit=search_limit,
            download_pool_conns=download_pool_conns,
            download_pool_size=download_pool_size,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        pattern = r"S2([ABC])_MSIL2A_(\d{8}T\d{6})"
        match = re.match(pattern, name)
        if not match:
            raise ValueError(f"Invalid Sentinel-2 L2A filename format: {name}")

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
            "sentinel-2-l1c",
            reader="msi_safe",
            default_composite=composite,
            downloader=downloader,
            stac_url=stac_url,
            search_limit=search_limit,
            download_pool_conns=download_pool_conns,
            download_pool_size=download_pool_size,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        pattern = r"S2([ABC])_MSIL1C_(\d{8}T\d{6})"
        match = re.match(pattern, name)
        if not match:
            raise ValueError(f"Invalid Sentinel-2 L1C filename format: {name}")

        groups = match.groups()
        acquisition_time = datetime.strptime(groups[1], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return ProductInfo(
            instrument="msi",
            level="1C",
            product_type="L1C",
            acquisition_time=acquisition_time,
        )
