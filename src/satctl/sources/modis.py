import logging
import re
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Literal, TypedDict

from pyhdf.SD import SD, SDC

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
PLATFORM_CONFIG = {
    "mod": {"prefix": "MOD", "version": "6.1"},  # Terra, Collection 6.1
    "myd": {"prefix": "MYD", "version": "6.1"},  # Aqua, Collection 6.1
}

RESOLUTION_CONFIG = {
    "qkm": {"suffix": "QKM", "meters": 250},
    "hkm": {"suffix": "HKM", "meters": 500},
    "1km": {"suffix": "1KM", "meters": 1000},
}


class ProductCombination(TypedDict):
    """Configuration for a specific platform/resolution combination."""

    platform: str
    resolution: str
    short_name: str
    version: str
    resolution_meters: int


class MODISSource(EarthDataSource):
    """Base source for MODIS products"""

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
        """Initialize MODIS data source.

        Args:
            collection_name (str): Name of the MODIS collection
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
        """Parse a MODIS granule ID into its components.

        Pattern: (PLATFORM)(LEVEL)(RESOLUTION).(DATE).(TIME).(VERSION).(TIMESTAMP)
        Platform: MOD (Terra), MYD (Aqua)

        Args:
            granule_id: MODIS granule identifier (e.g., "MOD02QKM.A2025227.1354.061.2025227231707")

        Returns:
            ParsedGranuleId with individual components

        Raises:
            ValueError: If granule ID format is invalid
        """
        pattern = r"^(M[OY]D)(\d{2})([A-Z0-9]{2,3})\.(A\d{7})\.(\d{4})\.(\d{3})\.(\d{13})$"
        match = re.match(pattern, granule_id)

        if not match:
            raise ValueError(f"Invalid MODIS granule ID format: {granule_id}")

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
        """Get list of HDF files for a granule.

        Args:
            item: Granule with local_path set

        Returns:
            List of .hdf file paths

        Raises:
            ValueError: If local_path is not set
        """
        if item.local_path is None:
            raise ValueError("Local path is missing. Did you download this granule?")
        return [str(p) for p in item.local_path.glob("*.hdf")]

    def _get_day_night_flag(self, files: list[Path | str]) -> str:
        """Extract day/night flag from the first file's metadata.

        Args:
            files: List of file paths to process

        Returns:
            Day/night flag as lowercase string (e.g., "day", "night", or raw value)
        """
        try:
            # Open HDF file and read CoreMetadata
            hdf = SD(str(files[1]), SDC.READ)
            core_meta = hdf.attributes().get("CoreMetadata.0", "")
            hdf.end()

            # Parse DAYNIGHTFLAG from CoreMetadata using regex
            match = re.search(
                r'OBJECT\s*=\s*DAYNIGHTFLAG.*?VALUE\s*=\s*"([^"]+)"',
                core_meta,
                re.DOTALL,
            )

            if match:
                return match.group(1).lower()

            log.warning("DAYNIGHTFLAG not found in CoreMetadata, defaulting to 'day'")
            return "day"

        except Exception as e:
            log.warning("Failed to extract day/night flag: %s, defaulting to 'day'", e)
            return "day"

    def _select_automatic_dataset(self, granule_id: str, day_night_flag: str, writer: Writer) -> dict[str, str]:
        """Select appropriate dataset automatically based on resolution and day/night flag.

        Args:
            granule_id: Granule identifier
            day_night_flag: Day/night condition flag
            writer: Writer instance for parsing datasets

        Returns:
            Dictionary of selected dataset

        Raises:
            ValueError: If resolution is unknown or day/night flag not recognized
        """
        parsed = self._parse_granule_id(granule_id)
        resolution = parsed.product_type

        # Default to day if flag is not recognized
        if day_night_flag not in DAY_NIGHT_CONDITIONS:
            log.debug("DayNightFlag '%s' not recognized for %s, defaulting to 'day'", day_night_flag, granule_id)
            day_night_flag = "day"

        # Map resolution and day/night flag to correct composite
        if resolution == "QKM":
            selected_composite = f"all_bands_250m_{day_night_flag}"
        elif resolution == "HKM":
            selected_composite = f"all_bands_500m_{day_night_flag}"
        elif resolution == "1KM":
            selected_composite = f"all_bands_1km_{day_night_flag}"
        else:
            raise ValueError(f"Unknown resolution '{resolution}' for automatic dataset selection")

        log.debug("Automatically selected dataset: %s", selected_composite)
        return writer.parse_datasets(selected_composite)

    def _get_georeference_short_name(self, radiance_short_name: str) -> str:
        """Get MODIS georeference short_name from radiance short_name.

        MODIS georeference products drop the resolution suffix:
        - MOD02QKM -> MOD03
        - MYD02HKM -> MYD03
        - MOD021KM -> MOD03

        Args:
            radiance_short_name: Level 02 product short name (e.g., "MOD02QKM")

        Returns:
            Level 03 product short name (e.g., "MOD03")
        """
        # Extract platform (MOD or MYD) from short name
        # MOD02QKM -> MOD, MYD02HKM -> MYD
        platform = radiance_short_name[:3]
        return f"{platform}03"

    def _build_georeference_pattern(self, radiance_id: str) -> str:
        """Build MODIS georeference granule ID pattern.

        MODIS georeference products drop the resolution suffix:
        - MOD02QKM.A2025227.1354.061.2025227231707 -> MOD03.A2025227.1354.061.*
        - MYD02HKM.A2025189.0000.061.2025192163307 -> MYD03.A2025189.0000.061.*

        Args:
            radiance_id: Radiance granule ID (e.g., "MOD02QKM.A2025227.1354.061.2025227231707")

        Returns:
            Georeference granule ID pattern with wildcard timestamp
        """
        parsed = self._parse_granule_id(radiance_id)
        platform = parsed.instrument  # MOD or MYD
        return f"{platform}03.{parsed.date}.{parsed.time}.{parsed.version}.*"

    def _get_file_extension(self) -> str:
        """Get file extension for MODIS data.

        Returns:
            str: File extension "hdf" (MODIS uses HDF format)
        """
        return "hdf"


class MODISL1BSource(MODISSource):
    """Source for MODIS Level 1B products.

    Supports geolocated radiance products from different platforms and resolutions.
    Accepts lists of platforms and resolutions, and will search for all combinations.

    Args:
        downloader: HTTP downloader instance
        platform: List of satellite platforms - ["mod"] (Terra), ["myd"] (Aqua)
        resolution: List of resolutions - ["qkm"] (250m), ["hkm"] (500m), ["1km"] (1000m)
        search_limit: Maximum number of granules to return in search results per combination

    Examples:
        # Single combination
        platform=["mod"], resolution=["qkm"] -> searches MOD02QKM

        # Multiple platforms, single resolution
        platform=["mod", "myd"], resolution=["qkm"] -> searches MOD02QKM, MYD02QKM

        # Single platform, multiple resolutions
        platform=["mod"], resolution=["qkm", "hkm"] -> searches MOD02QKM, MOD02HKM

        # All combinations (cartesian product)
        platform=["mod", "myd"], resolution=["qkm", "hkm"] -> searches MOD02QKM, MOD02HKM, MYD02QKM, MYD02HKM
    """

    def __init__(
        self,
        *,
        platform: list[Literal["mod", "myd"]],
        resolution: list[Literal["qkm", "hkm", "1km"]],
        search_limit: int = DEFAULT_SEARCH_LIMIT,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str = "earthdata",
        default_downloader: str = "http",
    ):
        """Initialize MODIS Level 1B data source.

        Args:
            platform (list[Literal["mod", "myd"]]): List of satellite platforms to search
            resolution (list[Literal["qkm", "hkm", "1km"]]): List of resolutions to search
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str): Default authenticator name to use when auth_builder is None. Defaults to "earthdata".
            default_downloader (str): Default downloader name to use when down_builder is None. Defaults to "http".
        """
        # Generate all combinations (cartesian product)
        self.combinations: list[ProductCombination] = []
        for plat, res in product(platform, resolution):
            plat_cfg = PLATFORM_CONFIG[plat]
            res_cfg = RESOLUTION_CONFIG[res]
            short_name = f"{plat_cfg['prefix']}02{res_cfg['suffix']}"
            self.combinations.append(
                ProductCombination(
                    platform=plat,
                    resolution=res,
                    short_name=short_name,
                    version=plat_cfg["version"],
                    resolution_meters=res_cfg["meters"],
                )
            )
        # Use the first combination as the primary configuration for parent class
        primary = self.combinations[0]
        super().__init__(
            "modis-l1b",
            reader="modis_l1b",
            auth_builder=auth_builder,
            down_builder=down_builder,
            short_name=primary["short_name"],
            default_composite="automatic",
            default_resolution=primary["resolution_meters"],
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            version=primary["version"],
            search_limit=search_limit,
        )

    def search(self, params: SearchParams) -> list[Granule]:
        """Search for MODIS data across all configured platform/resolution combinations.

        Args:
            params: Search parameters including time range and optional spatial filter

        Returns:
            List of granules from all combinations
        """
        log.debug("Searching for MODIS data across %d combinations", len(self.combinations))

        all_items = []

        for combo in self.combinations:
            log.debug(
                "Searching combination: %s %s (short_name: %s)",
                combo["platform"],
                combo["resolution"],
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
        """Get specific MODIS granule by ID.

        Automatically detects the short_name from the granule ID format.

        Args:
            item_id (str): Granule identifier (e.g., "MOD02QKM.A2025227.1354.061.2025227231707")
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
            # e.g., MOD + 02 + QKM = MOD02QKM
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
