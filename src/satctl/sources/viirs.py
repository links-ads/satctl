import logging
import re
import warnings
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import earthaccess
from pydantic import BaseModel
from xarray import DataArray

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.writers import Writer

log = logging.getLogger(__name__)


class ParsedGranuleId(BaseModel):
    """Parsed components of a VIIRS granule ID."""

    instrument: str  # VNP, VJ1, VJ2, etc.
    level: str  # 02, 03, etc.
    product_type: str  # MOD, IMG, DNB, etc.
    date: str  # A2025189
    time: str  # 0000
    version: str  # 002
    timestamp: str  # 2025192163307


class VIIRSAsset(BaseModel):
    href: str
    type: str
    media_type: str


class VIIRSSource(DataSource):
    """Base source for VIIRS products"""

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        downloader: Downloader,
        short_name: str,
        version: str | None = None,
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = 100,
    ):
        super().__init__(
            collection_name,
            downloader=downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
        )
        self.reader = reader
        self.short_name = short_name
        self.version = version
        self.search_limit = search_limit

        self.asset_keys = {
            "0": "http",
            "1": "s3",
            "2": "html",
            "3": "doi",
        }
        warnings.filterwarnings(action="ignore", category=UserWarning)

    def _parse_granule_id(self, granule_id: str) -> ParsedGranuleId:
        # Pattern: (INSTRUMENT)(LEVEL)(PRODUCT).(DATE).(TIME).(VERSION).(TIMESTAMP)
        # Instrument: VNP (NPP), VJ1 (NOAA-20), VJ2 (NOAA-21), etc.
        pattern = r"^(V[A-Z0-9]{1,2})(\d{2})([A-Z]{3,6})\.(A\d{7})\.(\d{4})\.(\d{3})\.(\d{13})$"
        match = re.match(pattern, granule_id)

        if not match:
            raise ValueError(f"Invalid VIIRS granule ID format: {granule_id}")

        return ParsedGranuleId(
            instrument=match.group(1),
            level=match.group(2),
            product_type=match.group(3),
            date=match.group(4),
            time=match.group(5),
            version=match.group(6),
            timestamp=match.group(7),
        )

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo: ...

    def convert_granule_id(
        self,
        granule_id: str,
        target_product: str,
        wildcard_timestamp: bool = True,
    ) -> str:
        parsed = self._parse_granule_id(granule_id)
        timestamp = "*" if wildcard_timestamp else parsed.timestamp
        return f"{parsed.instrument}{target_product}{parsed.product_type}.{parsed.date}.{parsed.time}.{parsed.version}.{timestamp}"

    def get_downloader_init_kwargs(self) -> dict:
        """Provide EarthData session to downloader initialization."""
        return {"session": self.downloader.auth.auth_session}

    def search(self, params: SearchParams) -> list[Granule]:
        log.debug("Searching for VIIRS data using earthaccess")

        search_kwargs: dict[str, Any] = {
            "short_name": self.short_name,
            "temporal": (params.start.isoformat(), params.end.isoformat()),
            "count": self.search_limit,
        }

        # Add version if specified
        if self.version:
            search_kwargs["version"] = self.version

        # Add spatial filter if provided
        if params.area_geometry:
            search_kwargs["polygon"] = params.area_geometry

        log.debug("Searching with parameters: %s", search_kwargs)
        results = earthaccess.search_data(**search_kwargs)

        items = [
            Granule(
                granule_id=i["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", ""),
                source=self.collections[0],
                assets={
                    self.asset_keys.get(str(k), "unknown"): VIIRSAsset(
                        href=url["URL"],
                        type=url["Type"],
                        media_type=url["MimeType"],
                    )
                    for k, url in enumerate(i["umm"]["RelatedUrls"])
                },
                info=self._parse_item_name(i["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", "")),
            )
            for i in results
        ]

        log.debug("Found %d items", len(items))

        return items

    def get_by_id(self, item_id: str) -> Granule:
        result = earthaccess.search_data(
            short_name=self.short_name.replace("02", "03"),
            granule_name=item_id,
        )[0]

        return Granule(
            granule_id=result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", ""),
            source=self.collections[0],
            assets={
                self.asset_keys.get(str(k), "unknown"): VIIRSAsset(
                    href=url["URL"],
                    type=url["Type"],
                    media_type=url["MimeType"],
                )
                for k, url in enumerate(result["umm"]["RelatedUrls"])
            },
            info=self._parse_item_name(
                result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", "")
            ),
        )

    def get_files(self, item: Granule) -> list[Path | str]: ...

    def validate(self, item: Granule) -> None: ...

    def download_item(self, item: Granule, destination: Path) -> bool:
        self.validate(item)

        # Download 02 product
        download_dir = destination / f"{item.granule_id}" / "02"
        download_dir.mkdir(parents=True, exist_ok=True)

        http_asset = item.assets["http"]
        local_file = download_dir / f"{item.granule_id}.nc"  # TODO improve download dir

        result = self.downloader.download(
            uri=http_asset.href,
            destination=local_file,
            item_id=item.granule_id,
        )

        log.debug("Saving granule metadata to: %s", download_dir)
        item.local_path = local_file
        item.to_file(download_dir)

        # Download 03 product
        download_dir = destination / f"{item.granule_id}" / "03"
        download_dir.mkdir(parents=True, exist_ok=True)

        goelocation_granule_id = self.convert_granule_id(item.granule_id, "03")
        geolocation_item = self.get_by_id(goelocation_granule_id)

        http_asset = geolocation_item.assets["http"]
        local_file = download_dir / f"{geolocation_item.granule_id}.nc"  # TODO improve download dir

        result = self.downloader.download(
            uri=http_asset.href,
            destination=local_file,
            item_id=geolocation_item.granule_id,
        )

        log.debug("Saving granule metadata to: %s", download_dir)
        geolocation_item.local_path = local_file
        geolocation_item.to_file(download_dir)

        return result

    def save_item(
        self,
        item: Granule,
        destination: Path,
        writer: Writer,
        params: ConversionParams,
        force: bool = False,
    ) -> dict[str, list]:
        """Process and save a single VIIRS granule.

        Args:
            item (Granule): Granule to process
            destination (Path): Output directory
            writer (Writer): Writer for output format
            params (ConversionParams): Processing parameters
            force (bool, optional): Force overwrite existing files. Defaults to False.

        Returns:
            dict[str, list]: Dictionary mapping granule_id to output file paths

        Raises:
            FileNotFoundError: If source files don't exist
            ValueError: If required parameters are missing
        """
        if item.local_path is None or not item.local_path.exists():
            raise FileNotFoundError(f"Invalid source file or directory: {item.local_path}")

        if params.datasets is None and self.default_composite is None:
            raise ValueError("Missing datasets or default composite for storage")

        datasets_dict = writer.parse_datasets(params.datasets or self.default_composite)
        log.debug("Attempting to save the following datasets: %s", datasets_dict)

        # Skip existing files unless forced
        if not force:
            for dataset_name, file_name in list(datasets_dict.items()):
                if (destination / item.granule_id / f"{file_name}.{writer.extension}").exists():
                    del datasets_dict[dataset_name]

        # If no datasets left to process, return early
        if not datasets_dict:
            log.debug("All datasets already exist for %s, skipping", item.granule_id)
            return {item.granule_id: []}

        files = self.get_files(item)
        log.debug("Found %d files to process", len(files))

        # Load and resample scene
        log.debug("Loading and resampling scene")
        scene = self.load_scene(item, datasets=list(datasets_dict.values()))

        # Define area based on user params or scene extent
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

        # Write datasets to output
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


class VIIRSL1BSource(VIIRSSource):
    """Source for VIIRS Level 1B products.

    Supports geolocated radiance products.
    """

    def __init__(
        self,
        *,
        downloader: Downloader,
        short_name: str = "VNP02MOD",
        version: str | None = "2",
        composite: str = "true_color",
        search_limit: int = 100,
    ):
        """Initialize VIIRS L1B source.

        Args:
            downloader (Downloader): Downloader instance
            short_name (str, optional): Product short name. Defaults to "VNP02MOD".
            version (str | None, optional): Product version. Defaults to None.
            composite (str, optional): Default composite. Defaults to "true_color".
            search_limit (int, optional): Maximum search results. Defaults to 100.
        """
        super().__init__(
            f"viirs-l1b-{short_name.lower()}",
            reader="viirs_l1b",
            default_composite=composite,
            downloader=downloader,
            short_name=short_name,
            version=version,
            search_limit=search_limit,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse VIIRS L1B granule name.

        Uses same format as SDR products.

        Args:
            name (str): Granule name

        Returns:
            ProductInfo: Parsed product information

        Raises:
            ValueError: If name format is invalid
        """
        parsed = self._parse_granule_id(name)
        # Date format: A2025189 -> need to strip 'A' prefix for datetime parsing
        date_str = parsed.date[1:]  # Remove 'A' prefix
        acquisition_time = datetime.strptime(f"{date_str}{parsed.time}", "%Y%j%H%M").replace(tzinfo=timezone.utc)

        return ProductInfo(
            instrument=parsed.instrument,
            level=parsed.level,
            product_type=parsed.product_type,
            acquisition_time=acquisition_time,
        )
