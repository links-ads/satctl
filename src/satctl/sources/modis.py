import logging
import re
import warnings
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any, Literal, TypedDict, cast

import earthaccess
from pydantic import BaseModel
from pyhdf.SD import SD, SDC
from xarray import DataArray

from satctl.auth.earthdata import EarthDataAuthenticator
from satctl.downloaders import Downloader
from satctl.downloaders.http import HTTPDownloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.writers import Writer

log = logging.getLogger(__name__)

# Constants
ASSET_KEY_MAPPING = {
    "0": "http",
    "1": "s3",
    "2": "html",
    "3": "doi",
}

PLATFORM_CONFIG = {
    "mod": {"prefix": "MOD", "version": "6.1"},  # Terra, Collection 6.1
    "myd": {"prefix": "MYD", "version": "6.1"},  # Aqua, Collection 6.1
}

RESOLUTION_CONFIG = {
    "qkm": {"suffix": "QKM", "meters": 250},
    "hkm": {"suffix": "HKM", "meters": 500},
    "1km": {"suffix": "1KM", "meters": 1000},
}

DAY_NIGHT_CONDITIONS = ("day", "night")


class ProductCombination(TypedDict):
    """Configuration for a specific platform/resolution combination."""

    platform: str
    resolution: str
    short_name: str
    version: str
    resolution_meters: int


class ParsedGranuleId(BaseModel):
    """Parsed components of a MODIS granule ID."""

    platform: str  # MOD, MYD
    level: str  # 02, 03, etc.
    resolution: str  # QKM, HKM, 1KM
    date: str  # A2025189
    time: str  # 0000
    version: str  # 061
    timestamp: str  # 2025192163307


class MODISAsset(BaseModel):
    href: str
    type: str
    media_type: str


class MODISSource(DataSource):
    """Base source for MODIS products"""

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
            platform=match.group(1),
            level=match.group(2),
            resolution=match.group(3),
            date=match.group(4),
            time=match.group(5),
            version=match.group(6),
            timestamp=match.group(7),
        )

    def _get_short_name_from_granule(self, granule_id: str) -> str:
        """Extract the short_name from a granule ID.

        Args:
            granule_id: Full granule ID (e.g., "MOD02QKM.A2025227.1354.061.2025227231707")

        Returns:
            Short name (e.g., "MOD02QKM")

        Example:
            "MOD02QKM.A2025227.1354.061.2025227231707" -> "MOD02QKM"
            "MYD02HKM.A2025189.0000.061.2025192163307" -> "MYD02HKM"
        """
        parsed = self._parse_granule_id(granule_id)
        return f"{parsed.platform}{parsed.level}{parsed.resolution}"

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
        return f"{parsed.platform}{target_product}.{parsed.date}.{parsed.time}.{parsed.version}.{timestamp}"

    # ============================================================================
    # Asset parsing utilities
    # ============================================================================

    def _parse_assets_from_umm_result(self, umm_result: dict) -> dict[str, MODISAsset]:
        """Parse assets from UMM search result into MODISAsset objects.

        Args:
            umm_result: UMM format result from earthaccess search

        Returns:
            Dictionary mapping asset keys (http, s3, html, doi) to MODISAsset objects
        """
        return {
            ASSET_KEY_MAPPING.get(str(k), "unknown"): MODISAsset(
                href=url["URL"],
                type=url["Type"],
                media_type=url["MimeType"],
            )
            for k, url in enumerate(umm_result["umm"]["RelatedUrls"])
        }

    # ============================================================================
    # Search operations
    # ============================================================================

    def _search_single_combination(
        self,
        short_name: str,
        version: str | None,
        params: SearchParams,
    ) -> list[Granule]:
        """Search for MODIS granules for a single platform/resolution combination.

        Args:
            short_name: NASA short name (e.g., "MOD02QKM", "MYD02HKM")
            version: Product version (e.g., "6.1")
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
            radiance_id = radiance_result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".hdf", "")
            georeference_id_pattern = self.convert_granule_id_with_wildcard(radiance_id, "03")

            georeference_result = earthaccess.search_data(
                short_name=georeference_id_pattern.split(".")[0],
                granule_name=georeference_id_pattern,
            )[0]

            items.append(
                Granule(
                    granule_id=radiance_id,
                    source=self.collections[0],
                    assets={
                        "radiance": self._parse_assets_from_umm_result(radiance_result),
                        "georeference": self._parse_assets_from_umm_result(georeference_result),
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
            short_name: NASA short name (e.g., "MOD02QKM")

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

        item_id = item["umm"]["DataGranule"]["Identifiers"][0]["Identifier"].replace(".hdf", "")

        return Granule(
            granule_id=item_id,
            source=self.collections[0],
            assets=self._parse_assets_from_umm_result(item),
            info=self._parse_item_name(item_id),
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
        """Download both radiance and georeference files for a MODIS granule.

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
        # Extract original filename from URL (e.g., MOD02QKM.A2025227.1354.061.2025227231707.hdf)
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
        # Extract original filename from URL (e.g., MOD03.A2025227.1354.061.2025227224504.hdf)
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

    def _filter_existing_datasets(
        self,
        datasets_dict: dict[str, str],
        destination: Path,
        granule_id: str,
        writer: Writer,
    ) -> dict[str, str]:
        """Remove datasets that already exist on disk.

        Args:
            datasets_dict: Dictionary of dataset names to file names
            destination: Base destination directory
            granule_id: Granule identifier
            writer: Writer instance with extension property

        Returns:
            Filtered dictionary with only non-existing datasets
        """
        filtered = datasets_dict.copy()
        for dataset_name, file_name in list(datasets_dict.items()):
            if (destination / granule_id / f"{file_name}.{writer.extension}").exists():
                del filtered[dataset_name]
        return filtered

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
        resolution = parsed.resolution

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

    def _write_scene_datasets(
        self,
        scene,
        datasets_dict: dict[str, str],
        destination: Path,
        granule_id: str,
        writer: Writer,
    ) -> dict[str, list]:
        """Write scene datasets to output files.

        Args:
            scene: Loaded and resampled scene
            datasets_dict: Dictionary of dataset names to file names
            destination: Base destination directory
            granule_id: Granule identifier
            writer: Writer instance

        Returns:
            Dictionary mapping granule_id to list of output paths
        """
        paths: dict[str, list] = defaultdict(list)
        output_dir = destination / granule_id
        output_dir.mkdir(exist_ok=True, parents=True)

        for dataset_name, file_name in datasets_dict.items():
            output_path = output_dir / f"{file_name}.{writer.extension}"
            paths[granule_id].append(
                writer.write(
                    dataset=cast(DataArray, scene[dataset_name]),
                    output_path=output_path,
                )
            )

        return paths

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
        # Validate inputs
        if item.local_path is None or not item.local_path.exists():
            raise FileNotFoundError(f"Invalid source file or directory: {item.local_path}")

        if params.datasets is None and self.default_composite is None:
            raise ValueError("Missing datasets or default composite for storage")

        # Parse and validate datasets
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
            datasets_dict = self._filter_existing_datasets(datasets_dict, destination, item.granule_id, writer)

        # Early return if no datasets to process
        if not datasets_dict:
            log.debug("All datasets already exist for %s, skipping", item.granule_id)
            return {item.granule_id: []}

        # Get files and extract day/night flag
        files = self.get_files(item)
        log.debug("Found %d files to process", len(files))
        day_night_flag = self._get_day_night_flag(files)

        # Handle dataset selection based on mode
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
        return self._write_scene_datasets(scene, datasets_dict, destination, item.granule_id, writer)

    def validate(self, item: Granule) -> None:
        """Validate a MODIS granule.

        Args:
            item: Granule to validate

        Raises:
            NotImplementedError: Validation not yet implemented for MODIS
        """
        raise NotImplementedError("Not implemented for MODIS yet.")


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
        downloader: Downloader,
        platform: list[Literal["mod", "myd"]],
        resolution: list[Literal["qkm", "hkm", "1km"]],
        search_limit: int = 100,
    ):
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
            default_composite="automatic",
            default_resolution=primary["resolution_meters"],
            downloader=downloader,
            short_name=primary["short_name"],
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
        # Parse the granule_id to determine which combination it belongs to
        try:
            parsed = self._parse_granule_id(item_id)
            # Reconstruct the short_name from parsed components
            # e.g., MOD + 02 + QKM = MOD02QKM
            short_name = f"{parsed.platform}{parsed.level}{parsed.resolution}"

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
            instrument=parsed.platform,
            level=parsed.level,
            product_type=parsed.resolution,
            acquisition_time=acquisition_time,
        )
