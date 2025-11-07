import logging
import re
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Literal, TypedDict

import xarray as xr

from satctl.auth import AuthBuilder
from satctl.downloaders import DownloadBuilder
from satctl.model import Granule, ProductInfo, SearchParams
from satctl.sources.earthdata import (
    DAY_NIGHT_CONDITIONS,
    DEFAULT_SEARCH_LIMIT,
    EarthDataSource,
    ParsedGranuleId,
)
from satctl.writers import Writer

log = logging.getLogger(__name__)

# Constants
SATELLITE_CONFIG = {
    "vnp": {"prefix": "VNP", "version": "2"},
    "jp1": {"prefix": "VJ1", "version": "2.1"},
    "jp2": {"prefix": "VJ2", "version": "2.1"},
}

PRODUCT_CONFIG = {
    "mod": {"resolution": 750},
    "img": {"resolution": 375},
}


class ProductCombination(TypedDict):
    """Configuration for a specific satellite/product_type combination."""

    satellite: str
    product_type: str
    short_name: str
    version: str
    resolution: int


class VIIRSSource(EarthDataSource):
    """Base source for VIIRS products"""

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        short_name: str,
        version: str | None = None,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str = "earthdata",
        default_downloader: str = "http",
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ):
        """Initialize VIIRS data source.

        Args:
            collection_name (str): Name of the VIIRS collection
            reader (str): Satpy reader name for this product type
            short_name (str): NASA CMR short name for the dataset
            version (str | None): Dataset version. Defaults to None.
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str): Default authenticator name to use when auth_builder is None. Defaults to "earthdata".
            default_downloader (str): Default downloader name to use when down_builder is None. Defaults to "http".
            default_composite (str | None): Default composite/band to load. Defaults to None.
            default_resolution (int | None): Default resolution in meters. Defaults to None.
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
        """
        super().__init__(
            collection_name,
            reader=reader,
            short_name=short_name,
            version=version,
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
            search_limit=search_limit,
        )

    def _parse_granule_id(self, granule_id: str) -> ParsedGranuleId:
        """Parse a VIIRS granule ID into its components.

        Pattern: (INSTRUMENT)(LEVEL)(PRODUCT).(DATE).(TIME).(VERSION).(TIMESTAMP)
        Instrument: VNP (NPP), VJ1 (NOAA-20), VJ2 (NOAA-21), etc.

        Args:
            granule_id: VIIRS granule identifier (e.g., "VNP02MOD.A2025227.1354.002.2025227231707")

        Returns:
            ParsedGranuleId with individual components

        Raises:
            ValueError: If granule ID format is invalid
        """
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

    def get_files(self, item: Granule) -> list[Path | str]:
        """Get list of NetCDF files for a granule.

        Args:
            item: Granule with local_path set

        Returns:
            List of .nc file paths

        Raises:
            ValueError: If local_path is not set
        """
        if item.local_path is None:
            raise ValueError("Local path is missing. Did you download this granule?")
        return list(item.local_path.glob("*.nc"))

    def _get_day_night_flag(self, files: list[Path | str]) -> str:
        """Extract day/night flag from the first file's metadata.

        Args:
            files: List of file paths to process

        Returns:
            Day/night flag as lowercase string (e.g., "day", "night", or raw value)
        """
        with xr.open_dataset(files[0]) as ds:
            return str(ds.attrs.get("DayNightFlag", "Not found")).lower()

    def _select_automatic_dataset(self, granule_id: str, day_night_flag: str, writer: Writer) -> dict[str, str]:
        """Select appropriate dataset automatically based on product type and day/night flag.

        Args:
            granule_id: Granule identifier
            day_night_flag: Day/night condition flag
            writer: Writer instance for parsing datasets

        Returns:
            Dictionary of selected dataset

        Raises:
            ValueError: If product type is unknown or day/night flag not recognized
        """
        parsed = self._parse_granule_id(granule_id)
        product_type = parsed.product_type

        # Default to day if flag is not recognized
        if day_night_flag not in DAY_NIGHT_CONDITIONS:
            log.debug("DayNightFlag '%s' not recognized for %s, defaulting to 'day'", day_night_flag, granule_id)
            day_night_flag = "day"

        # Map product type and day/night flag to correct composite
        if product_type == "MOD":
            selected_composite = f"all_bands_m_{day_night_flag}"
        elif product_type == "IMG":
            selected_composite = f"all_bands_h_{day_night_flag}"
        else:
            raise ValueError(f"Unknown product type '{product_type}' for automatic dataset selection")

        log.debug("Automatically selected dataset: %s", selected_composite)
        return writer.parse_datasets(selected_composite)

    def _get_georeference_short_name(self, radiance_short_name: str) -> str:
        """Get VIIRS georeference short_name from radiance short_name.

        VIIRS georeference products keep the product type suffix:
        - VNP02MOD -> VNP03MOD
        - VJ102IMG -> VJ103IMG
        - VJ202MOD -> VJ203MOD

        Args:
            radiance_short_name: Level 02 product short name (e.g., "VNP02MOD")

        Returns:
            Level 03 product short name (e.g., "VNP03MOD")
        """
        # Simply replace "02" with "03" in the short name
        return radiance_short_name.replace("02", "03")

    def _build_georeference_pattern(self, radiance_id: str) -> str:
        """Build VIIRS georeference granule ID pattern.

        VIIRS georeference products keep the product type suffix:
        - VNP02MOD.A2025227.1354.002.2025227231707 -> VNP03MOD.A2025227.1354.002.*
        - VJ102IMG.A2025189.0000.021.2025192163307 -> VJ103IMG.A2025189.0000.021.*

        Args:
            radiance_id: Radiance granule ID (e.g., "VNP02MOD.A2025227.1354.002.2025227231707")

        Returns:
            Georeference granule ID pattern with wildcard timestamp
        """
        parsed = self._parse_granule_id(radiance_id)
        return f"{parsed.instrument}03{parsed.product_type}.{parsed.date}.{parsed.time}.{parsed.version}.*"

    def _get_file_extension(self) -> str:
        """Get file extension for VIIRS data.

        Returns:
            str: File extension "nc" (VIIRS uses NetCDF format)
        """
        return "nc"


class VIIRSL1BSource(VIIRSSource):
    """Source for VIIRS Level 1B products.

    Supports geolocated radiance products from different satellites and product types.
    Accepts lists of satellites and product types, and will search for all combinations.

    Args:
        downloader: HTTP downloader instance
        satellite: List of satellite platforms - ["vnp"] (Suomi-NPP), ["jp1"] (NOAA-20/JPSS-1), ["jp2"] (NOAA-21/JPSS-2)
        product_type: List of product types - ["mod"] (M-bands, 750m), ["img"] (I-bands, 375m)
        search_limit: Maximum number of granules to return in search results per combination

    Examples:
        # Single combination
        satellite=["vnp"], product_type=["mod"] -> searches VNP02MOD

        # Multiple satellites, single product type
        satellite=["vnp", "jp1"], product_type=["mod"] -> searches VNP02MOD, VJ102MOD

        # Single satellite, multiple product types
        satellite=["vnp"], product_type=["mod", "img"] -> searches VNP02MOD, VNP02IMG

        # All combinations (cartesian product)
        satellite=["vnp", "jp1"], product_type=["mod", "img"] -> searches VNP02MOD, VNP02IMG, VJ102MOD, VJ102IMG
    """

    def __init__(
        self,
        *,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str = "earthdata",
        default_downloader: str = "http",
        satellite: list[Literal["vnp", "jp1", "jp2"]],
        product_type: list[Literal["mod", "img"]],
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ):
        """Initialize VIIRS Level 1B data source.

        Args:
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str): Default authenticator name to use when auth_builder is None. Defaults to "earthdata".
            default_downloader (str): Default downloader name to use when down_builder is None. Defaults to "http".
            satellite (list[Literal["vnp", "jp1", "jp2"]]): List of satellite platforms to search
            product_type (list[Literal["mod", "img"]]): List of product types to search
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
        """
        # Generate all combinations (cartesian product)
        self.combinations: list[ProductCombination] = []
        for sat, prod in product(satellite, product_type):
            sat_cfg = SATELLITE_CONFIG[sat]
            prod_cfg = PRODUCT_CONFIG[prod]
            short_name = f"{sat_cfg['prefix']}02{prod.upper()}"

            self.combinations.append(
                ProductCombination(
                    satellite=sat,
                    product_type=prod,
                    short_name=short_name,
                    version=sat_cfg["version"],
                    resolution=prod_cfg["resolution"],
                )
            )
        # Use the first combination as the primary configuration for parent class
        primary = self.combinations[0]
        super().__init__(
            "viirs-l1b",
            reader="viirs_l1b",
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite="automatic",
            default_resolution=primary["resolution"],
            short_name=primary["short_name"],
            version=primary["version"],
            search_limit=search_limit,
        )

    def search(self, params: SearchParams) -> list[Granule]:
        """Search for VIIRS data across all configured satellite/product_type combinations.

        Args:
            params: Search parameters including time range and optional spatial filter

        Returns:
            List of granules from all combinations
        """
        log.debug("Searching for VIIRS data across %d combinations", len(self.combinations))

        all_items = []

        for combo in self.combinations:
            log.debug(
                "Searching combination: %s %s (short_name: %s)",
                combo["satellite"],
                combo["product_type"],
                combo["short_name"],
            )
            items = self._search_single_combination(
                short_name=combo["short_name"],
                version=combo["version"],
                params=params,
            )
            all_items.extend(items)

        log.debug("Found %d total items across all combinations", len(all_items))
        return all_items

    def get_by_id(self, item_id: str, **_kwargs) -> Granule:
        """Get specific VIIRS granule by ID.

        Automatically detects the short_name from the granule ID format.

        Args:
            item_id (str): Granule identifier (e.g., "VNP02MOD.A2025227.1354.002.2025227231707")
            **_kwargs: Additional keyword arguments (unused)

        Returns:
            Granule: Requested granule with metadata

        Raises:
            ValueError: If granule ID format is invalid or not in configured combinations
        """
        # Parse the granule_id to determine which combination it belongs to
        try:
            parsed = self._parse_granule_id(item_id)
            # Reconstruct the short_name from parsed components
            # e.g., VNP + 02 + MOD = VNP02MOD
            short_name = f"{parsed.instrument}{parsed.level}{parsed.product_type}"

            # Verify this combination is configured
            matching_combo = None
            for combo in self.combinations:
                if combo["short_name"] == short_name:
                    matching_combo = combo
                    break

            if not matching_combo:
                configured = [c["short_name"] for c in self.combinations]
                raise ValueError(
                    f"Granule ID '{item_id}' has short_name '{short_name}' which is not in the configured combinations: {configured}"
                )

            log.debug("Auto-detected short_name '%s' from granule_id '%s'", short_name, item_id)

        except Exception as e:
            log.error("Failed to parse granule_id '%s': %s", item_id, e)
            raise ValueError(f"Invalid granule ID format: {item_id}") from e

        # Use the helper method with the determined short_name
        return self._get_granule_by_short_name(item_id, short_name)
