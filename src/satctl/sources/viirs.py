import logging
import re
import warnings
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import earthaccess
import xarray as xr
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

    def convert_granule_id_with_wildcard(
        self,
        granule_id: str,
        target_product: str,
        wildcard_timestamp: bool = True,
    ) -> str:
        parsed = self._parse_granule_id(granule_id)
        timestamp = "*" if wildcard_timestamp else parsed.timestamp
        return f"{parsed.instrument}{target_product}{parsed.product_type}.{parsed.date}.{parsed.time}.{parsed.version}.{timestamp}"

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
            search_kwargs["bounding_box"] = params.area_geometry.bounds  # TODO check if ok

        log.debug("Searching with parameters: %s", search_kwargs)
        radiance_results = earthaccess.search_data(**search_kwargs)

        items = []

        for rad_result in radiance_results:
            rad_id = rad_result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", "")
            geo_id = self.convert_granule_id_with_wildcard(rad_id, "03")

            geo_result = earthaccess.search_data(
                short_name=self.short_name.replace("02", "03"),
                granule_name=geo_id,
            )[0]

            geo_id = geo_result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", "")

            items.append(
                Granule(
                    granule_id=rad_id,
                    source=self.collections[0],
                    assets={
                        "radiance": {
                            self.asset_keys.get(str(k), "unknown"): VIIRSAsset(
                                href=url["URL"],
                                type=url["Type"],
                                media_type=url["MimeType"],
                            )
                            for k, url in enumerate(rad_result["umm"]["RelatedUrls"])
                        },
                        "georeference": {
                            self.asset_keys.get(str(k), "unknown"): VIIRSAsset(
                                href=url["URL"],
                                type=url["Type"],
                                media_type=url["MimeType"],
                            )
                            for k, url in enumerate(geo_result["umm"]["RelatedUrls"])
                        },
                    },
                    info=self._parse_item_name(rad_id),
                )
            )

        log.debug("Found %d items", len(items))

        return items

    def get_by_id(self, item_id: str, **kwargs) -> Granule:
        item = earthaccess.search_data(
            short_name=kwargs.get("short_name"),
            granule_name=item_id,
        )[0]

        item_id = item["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", "")

        return Granule(
            granule_id=item_id,
            source=self.collections[0],
            assets={
                self.asset_keys.get(str(k), "unknown"): VIIRSAsset(
                    href=url["URL"],
                    type=url["Type"],
                    media_type=url["MimeType"],
                )
                for k, url in enumerate(item["umm"]["RelatedUrls"])
            },
            info=self._parse_item_name(item_id),
        )

    def get_files(self, item: Granule) -> list[Path | str]:
        if item.local_path is None:
            raise ValueError("Local path is missing. Did you download this granule?")
        return list(item.local_path.glob("*.nc"))

    def validate(self, item: Granule) -> None: ...

    def download_item(self, item: Granule, destination: Path) -> bool:
        self.validate(item)

        destination = destination / f"{item.granule_id}"
        destination.mkdir(parents=True, exist_ok=True)

        # Download 02 product (radiance)
        http_asset = item.assets["radiance"]["http"]
        # Extract original filename from URL (e.g., VNP02MOD.A2025227.1354.002.2025227231707.nc)
        radiance_filename = Path(http_asset.href).name
        local_file = destination / radiance_filename

        if result := self.downloader.download(
            uri=http_asset.href,
            destination=local_file,
            item_id=item.granule_id,
        ):
            # Download 03 product (georeference)
            http_asset = item.assets["georeference"]["http"]
            # Extract original filename from URL (e.g., VNP03MOD.A2025227.1354.002.2025227224504.nc)
            georeference_filename = Path(http_asset.href).name
            local_file = destination / georeference_filename

            if result := self.downloader.download(
                uri=http_asset.href,
                destination=local_file,
                item_id=item.granule_id,
            ):
                log.debug(f"Saving granule metadata to: {destination}")
                item.local_path = destination
                item.to_file(destination)
            else:
                log.warning(f"Failed to download georeference component: {item.granule_id}")
        else:
            log.warning(f"Failed to download radiance component: {item.granule_id}")

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

        if params.datasets is None and self.default_composite is None:
            raise ValueError("Missing datasets or default composite for storage")

        datasets_dict = writer.parse_datasets(params.datasets or self.default_composite)
        log.debug("Attempting to save the following datasets: %s", datasets_dict)

        # Check for automatic dataset selection
        auto_select = False
        automatic_keys = [key for key in datasets_dict.keys() if key.lower() == "automatic"]

        if automatic_keys:
            if len(datasets_dict) > 1:
                raise ValueError(
                    "Cannot mix 'automatic' with other datasets. "
                    "Either use 'automatic' alone or specify explicit datasets."
                )
            auto_select = True
            log.debug("Automatic dataset selection enabled")

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

        # Filter datasets based on day/night flag from the data
        # TODO maybe find a cleaner way
        with xr.open_dataset(files[0]) as ds:
            day_night_flag = str(ds.attrs.get("DayNightFlag", "Not found")).lower()

        # Handle automatic dataset selection
        if auto_select:
            # Determine product type from granule_id (MOD or IMG)
            parsed = self._parse_granule_id(item.granule_id)
            product_type = parsed.product_type

            # Default to day if flag is not recognized
            if day_night_flag not in ("day", "night"):
                log.debug(f"DayNightFlag '{day_night_flag}' not recognized for {item.granule_id}, defaulting to 'day'")
                day_night_flag = "day"

            # Map product type and day/night flag to correct composite
            if product_type == "MOD":
                selected_composite = f"all_bands_m_{day_night_flag}"
            elif product_type == "IMG":
                selected_composite = f"all_bands_h_{day_night_flag}"
            else:
                raise ValueError(f"Unknown product type '{product_type}' for automatic dataset selection")

            datasets_dict = writer.parse_datasets(selected_composite)
            log.debug(f"Automatically selected dataset: {selected_composite}")

        # Remove datasets that don't match the day/night condition (for explicit dataset selection)
        elif day_night_flag in ("day", "night"):
            datasets_to_remove = []
            for dataset_name in datasets_dict.keys():
                if day_night_flag not in dataset_name.lower():
                    datasets_to_remove.append(dataset_name)
                    log.warning(
                        f"Skipping dataset '{dataset_name}' for granule {item.granule_id}: "
                        f"dataset requires different day/night condition (data is {day_night_flag})"
                    )

            # Remove incompatible datasets
            for dataset_name in datasets_to_remove:
                del datasets_dict[dataset_name]

            # If all datasets were filtered out, return early
            if not datasets_dict:
                log.warning(
                    "All datasets incompatible with day/night flag (%s) for %s, skipping",
                    day_night_flag,
                    item.granule_id,
                )
                return {item.granule_id: []}

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
        short_name: str,
        version: str | None = "2",
        default_composite: str,
        default_resolution: int,
        search_limit: int = 100,
    ):
        super().__init__(
            f"viirs-l1b-{short_name.lower()}",
            reader="viirs_l1b",
            default_composite=default_composite,
            default_resolution=default_resolution,
            downloader=downloader,
            short_name=short_name,
            version=version,
            search_limit=search_limit,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
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


# --- MOD Products (M-bands, 750m resolution) ---


class VNP02MODSource(VIIRSL1BSource):
    """VIIRS Level 1B Moderate Resolution from Suomi-NPP (VNP02MOD).

    M-bands with 750m resolution at nadir.
    """

    def __init__(self, downloader: Downloader, search_limit: int = 100):
        super().__init__(
            downloader=downloader,
            short_name="VNP02MOD",
            version="2",
            default_composite="automatic",
            default_resolution=750,
            search_limit=search_limit,
        )


class VJ102MODSource(VIIRSL1BSource):
    """VIIRS Level 1B Moderate Resolution from NOAA-20/JPSS-1 (VJ102MOD).

    M-bands with 750m resolution at nadir.
    """

    def __init__(self, downloader: Downloader, search_limit: int = 100):
        super().__init__(
            downloader=downloader,
            short_name="VJ102MOD",
            version="2.1",
            default_composite="automatic",
            default_resolution=750,
            search_limit=search_limit,
        )


class VJ202MODSource(VIIRSL1BSource):
    """VIIRS Level 1B Moderate Resolution from NOAA-21/JPSS-2 (VJ202MOD).

    M-bands with 750m resolution at nadir.
    """

    def __init__(self, downloader: Downloader, search_limit: int = 100):
        super().__init__(
            downloader=downloader,
            short_name="VJ202MOD",
            version="2.1",
            default_composite="automatic",
            default_resolution=750,
            search_limit=search_limit,
        )


# --- IMG Products (I-bands, 375m resolution) ---


class VNP02IMGSource(VIIRSL1BSource):
    """VIIRS Level 1B Imagery Resolution from Suomi-NPP (VNP02IMG).

    I-bands with 375m resolution at nadir.
    """

    def __init__(self, downloader: Downloader, search_limit: int = 100):
        super().__init__(
            downloader=downloader,
            short_name="VNP02IMG",
            version="2",
            default_composite="automatic",
            default_resolution=375,
            search_limit=search_limit,
        )


class VJ102IMGSource(VIIRSL1BSource):
    """VIIRS Level 1B Imagery Resolution from NOAA-20/JPSS-1 (VJ102IMG).

    I-bands with 375m resolution at nadir.
    """

    def __init__(self, downloader: Downloader, search_limit: int = 100):
        super().__init__(
            downloader=downloader,
            short_name="VJ102IMG",
            version="2.1",
            default_composite="automatic",
            default_resolution=375,
            search_limit=search_limit,
        )


class VJ202IMGSource(VIIRSL1BSource):
    """VIIRS Level 1B Imagery Resolution from NOAA-21/JPSS-2 (VJ202IMG).

    I-bands with 375m resolution at nadir.
    """

    def __init__(self, downloader: Downloader, search_limit: int = 100):
        super().__init__(
            downloader=downloader,
            short_name="VJ202IMG",
            version="2.1",
            default_composite="automatic",
            default_resolution=375,
            search_limit=search_limit,
        )
