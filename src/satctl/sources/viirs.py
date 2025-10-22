import logging
import re
import warnings
from abc import abstractmethod
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any, Literal, TypedDict

import earthaccess
import xarray as xr
from pydantic import BaseModel

from satctl.auth.earthdata import EarthDataAuthenticator
from satctl.downloaders import Downloader
from satctl.downloaders.http import HTTPDownloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.sources.earthdata_utils import parse_umm_assets
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

DAY_NIGHT_CONDITIONS = ("day", "night")


class ProductCombination(TypedDict):
    """Configuration for a specific satellite/product_type combination."""

    satellite: str
    product_type: str
    short_name: str
    version: str
    resolution: int


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
        warnings.filterwarnings(action="ignore", category=UserWarning)

    # ============================================================================
    # Abstract methods
    # ============================================================================

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo: ...

    # ============================================================================
    # Granule ID utilities
    # ============================================================================

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

    def _get_short_name_from_granule(self, granule_id: str) -> str:
        """Extract the short_name from a granule ID.

        Args:
            granule_id: Full granule ID (e.g., "VNP02MOD.A2025227.1354.002.2025227231707")

        Returns:
            Short name (e.g., "VNP02MOD")

        Example:
            "VNP02MOD.A2025227.1354.002.2025227231707" -> "VNP02MOD"
            "VJ102IMG.A2025189.0000.021.2025192163307" -> "VJ102IMG"
        """
        parsed = self._parse_granule_id(granule_id)
        return f"{parsed.instrument}{parsed.level}{parsed.product_type}"

    def convert_granule_id_with_wildcard(
        self,
        granule_id: str,
        target_product: str,
        wildcard_timestamp: bool = True,
    ) -> str:
        """Convert a granule ID to target a different product level with optional wildcard timestamp.

        Args:
            granule_id: Source granule ID
            target_product: Target product level (e.g., "03" for georeference)
            wildcard_timestamp: If True, replace timestamp with wildcard

        Returns:
            Converted granule ID pattern
        """
        parsed = self._parse_granule_id(granule_id)
        timestamp = "*" if wildcard_timestamp else parsed.timestamp
        return f"{parsed.instrument}{target_product}{parsed.product_type}.{parsed.date}.{parsed.time}.{parsed.version}.{timestamp}"

    # ============================================================================
    # Search operations
    # ============================================================================

    def _search_single_combination(
        self,
        short_name: str,
        version: str | None,
        params: SearchParams,
    ) -> list[Granule]:
        """Search for VIIRS granules for a single satellite/product combination.

        Args:
            short_name: NASA short name (e.g., "VNP02MOD", "VJ102IMG")
            version: Product version (e.g., "2", "2.1")
            params: Search parameters including time range and optional spatial filter

        Returns:
            List of granules for this combination
        """
        search_kwargs: dict[str, Any] = {
            "short_name": short_name,
            "temporal": (params.start.isoformat(), params.end.isoformat()),
            "count": self.search_limit,
        }

        # Add version if specified
        if version:
            search_kwargs["version"] = version

        # Add spatial filter if provided
        if params.area_geometry:
            search_kwargs["bounding_box"] = params.area_geometry.bounds

        log.debug("Searching with parameters: %s", search_kwargs)
        radiance_results = earthaccess.search_data(**search_kwargs)

        items = []

        for radiance_result in radiance_results:
            radiance_id = radiance_result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", "")
            georeference_id_pattern = self.convert_granule_id_with_wildcard(radiance_id, "03")

            georeference_result = earthaccess.search_data(
                short_name=short_name.replace("02", "03"),
                granule_name=georeference_id_pattern,
            )[0]

            items.append(
                Granule(
                    granule_id=radiance_id,
                    source=self.collections[0],
                    assets={
                        "radiance": parse_umm_assets(radiance_result, VIIRSAsset),
                        "georeference": parse_umm_assets(georeference_result, VIIRSAsset),
                    },
                    info=self._parse_item_name(radiance_id),
                )
            )

        return items

    # ============================================================================
    # Retrieval operations
    # ============================================================================

    def _get_granule_by_short_name(self, item_id: str, short_name: str) -> Granule:
        """Fetch a specific granule by ID and short_name.

        Args:
            item_id: The granule ID
            short_name: NASA short name (e.g., "VNP02MOD")

        Returns:
            The requested granule

        Raises:
            ValueError: If granule not found
        """
        try:
            results = earthaccess.search_data(
                short_name=short_name,
                granule_name=item_id,
            )

            if not results:
                raise ValueError(f"No granule found with id: {item_id}")
            item = results[0]

        except Exception as e:
            log.error("Failed to fetch granule %s: %s", item_id, e)
            raise

        item_id = item["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".nc", "")

        return Granule(
            granule_id=item_id,
            source=self.collections[0],
            assets=parse_umm_assets(item, VIIRSAsset),
            info=self._parse_item_name(item_id),
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

    # ============================================================================
    # Download operations
    # ============================================================================

    def get_downloader_init_kwargs(self) -> dict:
        """Provide EarthData session to downloader initialization."""
        # Only provide session if we have HTTPDownloader with EarthDataAuthenticator
        if isinstance(self.downloader, HTTPDownloader) and isinstance(self.downloader.auth, EarthDataAuthenticator):
            return {"session": self.downloader.auth.auth_session}
        return {}

    def download_item(self, item: Granule, destination: Path) -> bool:
        """Download both radiance and georeference files for a VIIRS granule.

        Args:
            item: Granule to download
            destination: Base destination directory

        Returns:
            True if both components downloaded successfully, False otherwise
        """
        granule_dir = destination / item.granule_id
        granule_dir.mkdir(parents=True, exist_ok=True)

        # Download 02 product (radiance)
        radiance_asset = item.assets["radiance"]["http"]
        # Extract original filename from URL (e.g., VNP02MOD.A2025227.1354.002.2025227231707.nc)
        radiance_filename = Path(radiance_asset.href).name
        radiance_file = granule_dir / radiance_filename

        radiance_success = self.downloader.download(
            uri=radiance_asset.href,
            destination=radiance_file,
            item_id=item.granule_id,
        )

        if not radiance_success:
            log.warning("Failed to download radiance component: %s", item.granule_id)
            return False

        # Download 03 product (georeference)
        georeference_asset = item.assets["georeference"]["http"]
        # Extract original filename from URL (e.g., VNP03MOD.A2025227.1354.002.2025227224504.nc)
        georeference_filename = Path(georeference_asset.href).name
        georeference_file = granule_dir / georeference_filename

        georeference_success = self.downloader.download(
            uri=georeference_asset.href,
            destination=georeference_file,
            item_id=item.granule_id,
        )

        if not georeference_success:
            log.warning("Failed to download georeference component: %s", item.granule_id)
            return False

        # Both downloads successful - save metadata
        log.debug("Saving granule metadata to: %s", granule_dir)
        item.local_path = granule_dir
        item.to_file(granule_dir)
        return True

    # ============================================================================
    # Processing helpers (used by save_item)
    # ============================================================================

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

    def _filter_datasets_by_day_night(
        self,
        datasets_dict: dict[str, str],
        day_night_flag: str,
        granule_id: str,
    ) -> dict[str, str]:
        """Filter datasets that don't match the day/night condition.

        Args:
            datasets_dict: Dictionary of dataset names to file names
            day_night_flag: Day/night condition flag
            granule_id: Granule identifier for logging

        Returns:
            Filtered dictionary with only compatible datasets
        """
        if day_night_flag not in DAY_NIGHT_CONDITIONS:
            return datasets_dict

        filtered = datasets_dict.copy()
        for dataset_name in list(datasets_dict.keys()):
            if day_night_flag not in dataset_name.lower():
                del filtered[dataset_name]
                log.warning(
                    f"Skipping dataset '{dataset_name}' for granule {granule_id}: "
                    f"dataset requires different day/night condition (data is {day_night_flag})"
                )
        return filtered

    # ============================================================================
    # Processing operations
    # ============================================================================

    def save_item(
        self,
        item: Granule,
        destination: Path,
        writer: Writer,
        params: ConversionParams,
        force: bool = False,
    ) -> dict[str, list]:
        """Save granule item to output files after processing.

        Args:
            item: Granule to process
            destination: Base destination directory
            writer: Writer instance for output
            params: Conversion parameters
            force: If True, overwrite existing files

        Returns:
            Dictionary mapping granule_id to list of output paths
        """
        # Validate inputs using base class helper
        self._validate_save_inputs(item, params)

        # Parse datasets using base class helper
        datasets_dict = self._prepare_datasets(writer, params)

        # Check for automatic dataset selection (VIIRS-specific)
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

        # Filter existing files using base class helper
        datasets_dict = self._filter_existing_files(datasets_dict, destination, item.granule_id, writer, force)

        # Early return if no datasets to process
        if not datasets_dict:
            log.debug("All datasets already exist for %s, skipping", item.granule_id)
            return {item.granule_id: []}

        # Get files and extract day/night flag (VIIRS-specific)
        files = self.get_files(item)
        log.debug("Found %d files to process", len(files))
        day_night_flag = self._get_day_night_flag(files)

        # Handle dataset selection based on day/night mode (VIIRS-specific)
        if auto_select:
            datasets_dict = self._select_automatic_dataset(item.granule_id, day_night_flag, writer)
        else:
            datasets_dict = self._filter_datasets_by_day_night(datasets_dict, day_night_flag, item.granule_id)
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

        # Define area using base class helper
        area_def = self._create_area_from_params(params, scene)
        scene = self.resample(scene, area_def=area_def)

        # Write datasets using base class helper
        return self._write_scene_datasets(scene, datasets_dict, destination, item.granule_id, writer)

    def validate(self, item: Granule) -> None:
        """Validate a VIIRS granule.

        Args:
            item: Granule to validate

        Raises:
            NotImplementedError: Validation not yet implemented for VIIRS
        """
        raise NotImplementedError("Not implemented for VIIRS yet.")


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
        downloader: Downloader,
        satellite: list[Literal["vnp", "jp1", "jp2"]],
        product_type: list[Literal["mod", "img"]],
        search_limit: int = 100,
    ):
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
            default_composite="automatic",
            default_resolution=primary["resolution"],
            downloader=downloader,
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
