import logging
import re
import uuid
import warnings
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Iterable
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
from satctl.utils import extract_zip
from satctl.writers import Writer

log = logging.getLogger(__name__)


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
            limit=self.search_limit,
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

    def get_by_id(self, item_id: str) -> Granule:
        raise NotImplementedError()

    def get_files(self, item: Granule) -> list[Path | str]:
        if item.local_path is None:
            raise ValueError("Local path is missing. Did you download this granule?")
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


class SLSTRSource(Sentinel3Source):
    """Source for Sentinel-3 SLSTR L1B product."""

    def __init__(
        self,
        *,
        downloader: Downloader,
        stac_url: str,
        composite: str = "all_bands_500m",
        search_limit: int = 100,
        download_pool_conns: int = 10,
        download_pool_size: int = 2,
    ):
        super().__init__(
            "sentinel-3-sl-1-rbt-ntc",
            reader="slstr_l1b",
            default_composite=composite,
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
            raise ValueError(f"Invalid SLSTR filename format: {name}")

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
        composite: str = "all_bands_300m",
        search_limit: int = 100,
        download_pool_conns: int = 10,
        download_pool_size: int = 2,
    ):
        super().__init__(
            "sentinel-3-olci-1-efr-ntc",
            reader="olci_l1b",
            default_composite=composite,
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
            raise ValueError(f"Invalid OLCI filename format: {name}")

        groups = match.groups()
        acquisition_time = datetime.strptime(groups[3], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return ProductInfo(
            instrument="olci",
            level=groups[1],
            product_type=groups[2],
            acquisition_time=acquisition_time,
        )
