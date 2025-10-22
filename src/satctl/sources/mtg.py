import logging
import re
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from eumdac.datastore import DataStore
from pydantic import BaseModel
from satpy.scene import Scene
from xarray import DataArray

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
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
            raise ValueError("Local path is missing. Did you download this granule?")
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
                raise ValueError("Please provide the source with a default composite, or provide custom composites")
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
