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

from eumdac.datastore import DataStore
from pydantic import BaseModel
from pystac_client import Client
from xarray import DataArray

from satctl.downloaders import Downloader, eumetsat
from satctl.model import (ConversionParams, Granule, ProductInfo,
                          ProgressEventType, SearchParams)
from satctl.progress.events import emit_event
from satctl.sources import DataSource
from satctl.utils import extract_zip
from satctl.writers import Writer

log = logging.getLogger(__name__)


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
        self.search_limit = search_limit
        self.download_pool_conns = download_pool_conns
        self.download_pool_size = download_pool_size
        warnings.filterwarnings(action="ignore", category=UserWarning)

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
                        granule_id=str(i),
                        source=str(i.collection),  
                        assets={"product": MTGAsset(href=i.url)},
                        info=ProductInfo(
                            instrument=i.instrument,
                            level="",
                            product_type=i.product_type,
                            acquisition_time=i.sensing_end,
                        ),
                    )
                    for i in results
                ]
            )

        log.debug("Found %d items", len(items))
        return items

    def get_by_id(self, item_id: str) -> Granule:
        raise NotImplementedError()

    def get_files(self, item: Granule) -> list[Path | str]:
        raise NotImplementedError()

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
                zip_path=local_file,
                extract_to=destination / f"{item.granule_id}.MTG",
                item_id=item.granule_id
            )
            item.local_path = local_path
            log.debug("Saving granule metadata to: %s", local_path)
            item.to_file(local_path)
            local_file.unlink()  # delete redundant zip
        else:
            log.warning("Failed to download: %s", item.granule_id)
        return result

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
        raise NotImplementedError()

    def save(
        self,
        items: Granule | list[Granule],
        params: ConversionParams,
        destination: Path,
        writer: Writer,
        num_workers: int | None = None,
        force: bool = False,
    ) -> tuple[list, list]:
        raise NotImplementedError()
