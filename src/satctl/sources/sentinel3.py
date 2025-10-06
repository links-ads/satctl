import logging
import re
import tempfile
import warnings
from abc import abstractmethod
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from pyresample.geometry import AreaDefinition
from pystac_client import Client
from satpy.scene import Scene

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.progress import get_reporter
from satctl.sources import DataSource
from satctl.utils import area_def_from_geometry, extract_zip
from satctl.writers import Writer

log = logging.getLogger(__name__)


class Sentinel3Source(DataSource):
    """Source for Sentinel-3 SLSTR L1B product."""

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        composite: str,
        downloader: Downloader,
        stac_url: str,
        search_limit: int = 100,
        download_pool_conns: int = 10,
        download_pool_size: int = 2,
    ):
        super().__init__(
            collection_name,
            composite=composite,
            downloader=downloader,
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
                assets=i.assets,
                info=self._parse_item_name(i.id),
            )
            for i in search.items()
        ]
        log.debug("Found %d items", len(items))
        return items

    def get(self, item_id: str) -> Granule:
        raise NotImplementedError()

    def validate(self, item: Granule) -> None:
        """Validates a Sentinel3 STAC item.

        Args:
            item (Granule): STAC item to validate
        """
        for name, asset in item.assets.items():
            # We expect zips, netcdfs, xfdumanifest.xml and thumbnail.jpg
            assert asset.media_type in ("application/netcdf", "application/zip", "image/jpeg", "application/xml")
            # The zip is our main interest
            if asset.media_type == "application/zip":
                assert name == "product"
            # Check that we have a manifest file
            if asset.media_type == "application/xml":
                assert name == "xfdumanifest"

    def download(
        self,
        items: Granule | list[Granule],
        output_dir: Path,
    ) -> tuple[list, list]:
        # check output folder exists, make sure items is iterable
        output_dir.mkdir(parents=True, exist_ok=True)
        if not isinstance(items, Iterable):
            items = [items]

        success = []
        failure = []
        progress = get_reporter()
        progress.start(total_items=len(items))
        self.downloader.init()
        try:
            for item in items:
                self.validate(item)
                zip_asset = item.assets["product"]
                local_filename = f"{item.granule_id}.zip"
                local_path = output_dir / local_filename

                progress.add_task(item_id=item.granule_id, description="download")
                if downloaded := self.downloader.download(
                    uri=zip_asset.href,
                    destination=local_path,
                    item_id=item.granule_id,
                    progress=progress,
                ):
                    success.append(zip_asset)
                else:
                    failure.append(zip_asset)
                progress.end_task(item_id=item.granule_id, success=downloaded)

        except KeyboardInterrupt:
            log.info("Interrupted, exiting download")
            return success, failure
        finally:
            progress.stop()
            self.downloader.close()
        return success, failure

    def load_scene(
        self,
        source: Granule | Path | str,
        composites: list[str] | None = None,
        area_definition: AreaDefinition | None = None,
    ) -> Scene:
        raise NotImplementedError()

    def convert(
        self,
        params: ConversionParams,
        source: Path,
        output_dir: Path,
        writer: Writer,
        force: bool = False,
    ) -> tuple[list, list]:
        assert source.exists(), f"Invalid source file or directory: {source}"
        zip_files = list(source.glob("*.zip")) if source.is_dir() else [source]
        assert zip_files, f"No zip files found for: {source}"

        log.debug("Found %d files to process", len(zip_files))
        output_dir.mkdir(exist_ok=True, parents=True)

        log.debug("Creating area definition")
        area_def = area_def_from_geometry(
            name=f"aoi_{self.reader}",
            area=params.area_geometry,  # type: ignore
            target_crs=params.crs,
            resolution=500,
        )

        success = []
        failure = []
        progress = get_reporter()
        progress.start(total_items=len(zip_files))

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                log.debug("Using temporary directory: %s", temp_dir)

                for zip_file in zip_files:
                    output_filename = f"{zip_file.stem}.tif"
                    output_filepath = output_dir / output_filename

                    progress.add_task(item_id=zip_file.stem, description="extract")
                    if output_filepath.exists() and not force:
                        log.debug("Output file %s already exists, skipping", output_filename)
                        progress.end_task(item_id=zip_file.stem, success=True)
                        continue

                    try:
                        item_dir = extract_zip(
                            zip_file,
                            temp_path,
                            expected_dir=f"{zip_file.stem}.SEN3",
                            progress=progress,
                            task_id=zip_file.stem,
                        )
                    except Exception as e:
                        # in case of errors, skip to the next file
                        log.error("Could not extract %s: %s", zip_file.name, str(e))
                        progress.end_task(item_id=zip_file.stem, success=False)
                        failure.append(zip_file)
                        continue

                    log.debug("Loading scene")
                    scene = Scene(filenames=item_dir.glob("*"), reader=self.reader)
                    datasets = [self.composite]
                    scene.load(datasets)

                    log.debug("Resampling to target area definition")
                    resampled = scene.resample(area_def, datasets=datasets, resampler="nearest")  # type: ignore

                    log.debug("Saving to: %s", output_filepath)
                    progress.update_progress(item_id=zip_file.stem, description="writing")
                    if written := writer.write(
                        scene=resampled,
                        output_path=output_filepath,
                        composite=self.composite,
                    ):
                        success.append(zip_file)
                    else:
                        failure.append(zip_file)
                    progress.end_task(item_id=zip_file.stem, success=written)

        except KeyboardInterrupt:
            log.info("Interrupted, exiting conversion")
        finally:
            progress.stop()
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
            composite=composite,
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
            composite=composite,
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
