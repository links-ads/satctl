"""Base class for NASA EarthData sources (VIIRS, MODIS, etc.)."""

import logging
import warnings
from abc import abstractmethod
from pathlib import Path
from typing import Any

import earthaccess
from pydantic import BaseModel

from satctl.auth import AuthBuilder
from satctl.downloaders import DownloadBuilder, Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.writers import Writer

log = logging.getLogger(__name__)

# Constants
EARTHDATA_ASSET_KEY_MAPPING = {
    "0": "http",
    "1": "s3",
    "2": "html",
    "3": "doi",
}

DAY_NIGHT_CONDITIONS = ("day", "night")
DEFAULT_SEARCH_LIMIT = 100


class EarthDataAsset(BaseModel):
    """Asset model for EarthData sources."""

    href: str
    type: str
    media_type: str


class ParsedGranuleId(BaseModel):
    """Parsed components of an EarthData granule ID.

    Used by both MODIS and VIIRS sources with sensor-specific parsing.
    """

    instrument: str  # Platform/instrument (MOD, MYD, VNP, VJ1, VJ2, etc.)
    level: str  # Product level (02, 03, etc.)
    product_type: str  # Product type (QKM, HKM, 1KM, MOD, IMG, etc.)
    date: str  # Date string (A2025189)
    time: str  # Time string (0000)
    version: str  # Version (061, 002, etc.)
    timestamp: str  # Processing timestamp


def parse_umm_assets(umm_result: dict, asset_class: type[BaseModel] = EarthDataAsset) -> dict[str, BaseModel]:
    """Parse assets from UMM search result into asset objects.

    Args:
        umm_result (dict): UMM format result from earthaccess search
        asset_class (type[BaseModel]): Asset model class. Defaults to EarthDataAsset.

    Returns:
        dict[str, BaseModel]: Dictionary mapping asset keys (http, s3, html, doi) to Asset objects
    """
    return {
        EARTHDATA_ASSET_KEY_MAPPING.get(str(k), "unknown"): asset_class(
            href=url["URL"],
            type=url["Type"],
            media_type=url["MimeType"],
        )
        for k, url in enumerate(umm_result["umm"]["RelatedUrls"])
    }


class EarthDataSource(DataSource):
    """Base class for NASA EarthData sources.

    Provides common functionality for sources that use earthaccess library
    to search and download data from NASA EarthData repositories (MODIS, VIIRS, etc.).

    Subclasses must implement sensor-specific methods for parsing granule IDs,
    extracting metadata, and handling file formats.
    """

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        short_name: str,
        version: str | None = None,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str | None = "earthdata",
        default_downloader: str | None = "http",
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = DEFAULT_SEARCH_LIMIT,
    ):
        """Initialize EarthData source.

        Args:
            collection_name (str): Collection name identifier.
            reader (str): Satpy reader name.
            short_name (str): NASA short name for this source.
            auth_builder (AuthBuilder): factory that creates an auth object on demand.
            down_builder (DownloadBuilder): factory that create a downloader object on demand.
            default_authenticator (str): Default authenticator name to use when auth_builder is None. Defaults to "earthdata".
            default_downloader (str): Default downloader name to use when down_builder is None. Defaults to "http".
            version (str | None): Product version. Defaults to None.
            default_composite (str | None): Default composite name. Defaults to None.
            default_resolution (int | None): Default resolution in meters. Defaults to None.
            search_limit (int): Maximum search results per query. Defaults to 100.
        """
        super().__init__(
            collection_name,
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_composite=default_composite,
            default_resolution=default_resolution,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
        )
        self.reader = reader
        self.short_name = short_name
        self.version = version
        self.search_limit = search_limit
        warnings.filterwarnings(action="ignore", category=UserWarning)

    @abstractmethod
    def _parse_granule_id(self, granule_id: str) -> ParsedGranuleId:
        """Parse a granule ID into its components.

        Args:
            granule_id (str): Granule identifier

        Returns:
            ParsedGranuleId: ParsedGranuleId with individual components

        Raises:
            ValueError: If granule ID format is invalid
        """
        ...

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse item name into ProductInfo.

        Args:
            name (str): Item name to parse

        Returns:
            ProductInfo: ProductInfo with extracted metadata
        """
        ...

    @abstractmethod
    def get_files(self, item: Granule) -> list[Path | str]:
        """Get list of files for a granule.

        Args:
            item (Granule): Granule with local_path set

        Returns:
            list[Path | str]: List of file paths

        Raises:
            ValueError: If local_path is not set
        """
        ...

    @abstractmethod
    def _get_day_night_flag(self, files: list[Path | str]) -> str:
        """Extract day/night flag from file metadata.

        Args:
            files (list[Path | str]): List of file paths to process

        Returns:
            str: Day/night flag as lowercase string (e.g., "day", "night")
        """
        ...

    @abstractmethod
    def _select_automatic_dataset(self, granule_id: str, day_night_flag: str, writer: Writer) -> dict[str, str]:
        """Select appropriate dataset automatically based on sensor and conditions.

        Args:
            granule_id (str): Granule identifier
            day_night_flag (str): Day/night condition flag
            writer (Writer): Writer instance for parsing datasets

        Returns:
            dict[str, str]: Dictionary of selected dataset

        Raises:
            ValueError: If automatic selection cannot be performed
        """
        ...

    @abstractmethod
    def _get_georeference_short_name(self, radiance_short_name: str) -> str:
        """Get the georeference (level 03) short_name from radiance short_name.

        Different sensors have different naming patterns for georeference products:
        - MODIS: drops resolution suffix (MOD02QKM -> MOD03)
        - VIIRS: keeps product type (VNP02MOD -> VNP03MOD)

        Args:
            radiance_short_name (str): Level 02 product short name (e.g., "MOD02QKM", "VNP02MOD")

        Returns:
            str: Level 03 product short name (e.g., "MOD03", "VNP03MOD")
        """
        ...

    @abstractmethod
    def _build_georeference_pattern(self, radiance_id: str) -> str:
        """Build georeference granule ID pattern from radiance ID.

        Different sensors include different fields in georeference IDs:
        - MODIS: drops resolution (MOD02QKM.A2025227.1354.061.XXX -> MOD03.A2025227.1354.061.*)
        - VIIRS: keeps product type (VNP02MOD.A2025227.1354.002.XXX -> VNP03MOD.A2025227.1354.002.*)

        Args:
            radiance_id (str): Radiance granule ID (e.g., "MOD02QKM.A2025227.1354.061.2025227231707")

        Returns:
            str: Georeference granule ID pattern with wildcard timestamp
        """
        ...

    @abstractmethod
    def _get_file_extension(self) -> str:
        """Get the file extension for this data source.

        Different sensors use different file formats:
        - MODIS: HDF format (.hdf)
        - VIIRS: NetCDF format (.nc)

        Returns:
            str: File extension without dot (e.g., "hdf", "nc")
        """
        ...

    def _get_short_name_from_granule(self, granule_id: str) -> str:
        """Extract the short_name from a granule ID.

        Args:
            granule_id (str): Full granule ID

        Returns:
            str: Short name (e.g., "MOD02QKM", "VNP02MOD")

        Example:
            "MOD02QKM.A2025227.1354.061.2025227231707" -> "MOD02QKM"
            "VNP02MOD.A2025227.1354.002.2025227231707" -> "VNP02MOD"
        """
        parsed = self._parse_granule_id(granule_id)
        return f"{parsed.instrument}{parsed.level}{parsed.product_type}"

    def _search_single_combination(
        self,
        short_name: str,
        version: str | None,
        params: SearchParams,
    ) -> list[Granule]:
        """Search for granules for a single product combination.

        Args:
            short_name (str): NASA short name (e.g., "MOD02QKM", "VNP02MOD")
            version (str | None): Product version (e.g., "6.1", "2")
            params (SearchParams): Search parameters including time range and optional spatial filter

        Returns:
            list[Granule]: List of granules for this combination
        """
        # Ensure authentication before searching (earthaccess requires global auth)
        self.authenticator().ensure_authenticated()

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
            # Get radiance ID - strip file extension
            radiance_id_raw = radiance_result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"]
            radiance_id = ".".join(radiance_id_raw.split(".")[:-1])  # Trick to agnostically remove the file extension

            # Find matching georeference file (level 03)
            georeference_id_pattern = self._build_georeference_pattern(radiance_id)
            georeference_short_name = self._get_georeference_short_name(short_name)

            georeference_result = earthaccess.search_data(
                short_name=georeference_short_name,
                granule_name=georeference_id_pattern,
            )[0]

            items.append(
                Granule(
                    granule_id=radiance_id,
                    source=self.collections[0],
                    assets={
                        "radiance": parse_umm_assets(radiance_result, EarthDataAsset),
                        "georeference": parse_umm_assets(georeference_result, EarthDataAsset),
                    },
                    info=self._parse_item_name(radiance_id),
                )
            )

        return items

    def _get_granule_by_short_name(self, item_id: str, short_name: str) -> Granule:
        """Fetch a specific granule by ID and short_name.

        Fetches both radiance (level 02) and georeference (level 03) data.

        Args:
            item_id (str): The granule ID
            short_name (str): NASA short name (e.g., "MOD02QKM", "VNP02MOD")

        Returns:
            Granule: The requested granule with radiance and georeference assets

        Raises:
            ValueError: If granule not found
        """
        try:
            # Fetch radiance data (level 02)
            extension = self._get_file_extension()
            if not item_id.endswith(f".{extension}"):
                item_id = f"{item_id}.{extension}"

            radiance_results = earthaccess.search_data(
                short_name=short_name,
                granule_name=item_id,
            )

            if not radiance_results:
                raise ValueError(f"No granule found with id: {item_id}")
            radiance_result = radiance_results[0]

            # Get radiance ID - strip file extension
            radiance_id_raw = radiance_result["umm"]["DataGranule"]["Identifiers"][0]["Identifier"]
            radiance_id = ".".join(radiance_id_raw.split(".")[:-1])  # Agnostically remove file extension

            # Find matching georeference file (level 03)
            georeference_id_pattern = self._build_georeference_pattern(radiance_id)
            georeference_short_name = self._get_georeference_short_name(short_name)

            georeference_results = earthaccess.search_data(
                short_name=georeference_short_name,
                granule_name=georeference_id_pattern,
            )

            if not georeference_results:
                raise ValueError(f"No georeference data found for granule: {radiance_id}")
            georeference_result = georeference_results[0]

        except Exception as e:
            log.error("Failed to fetch granule %s: %s", item_id, e)
            raise

        return Granule(
            granule_id=radiance_id,
            source=self.collections[0],
            assets={
                "radiance": parse_umm_assets(radiance_result, EarthDataAsset),
                "georeference": parse_umm_assets(georeference_result, EarthDataAsset),
            },
            info=self._parse_item_name(radiance_id),
        )

    def download_item(self, item: Granule, destination: Path, downloader: Downloader) -> bool:
        """Download both radiance and georeference files for a granule.

        Args:
            item (Granule): Granule to download
            destination (Path): Base destination directory

        Returns:
            bool: True if both components downloaded successfully, False otherwise
        """
        granule_dir = destination / item.granule_id
        granule_dir.mkdir(parents=True, exist_ok=True)

        # Download 02 product (radiance)
        radiance_asset = item.assets["radiance"]["http"]
        radiance_filename = Path(radiance_asset.href).name
        radiance_file = granule_dir / radiance_filename

        radiance_success = downloader.download(
            uri=radiance_asset.href,
            destination=radiance_file,
            item_id=item.granule_id,
        )

        if not radiance_success:
            log.warning("Failed to download radiance component: %s", item.granule_id)
            return False

        # Download 03 product (georeference)
        georeference_asset = item.assets["georeference"]["http"]
        georeference_filename = Path(georeference_asset.href).name
        georeference_file = granule_dir / georeference_filename

        georeference_success = downloader.download(
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

    def _filter_datasets_by_day_night(
        self,
        datasets_dict: dict[str, str],
        day_night_flag: str,
        granule_id: str,
    ) -> dict[str, str]:
        """Filter datasets that don't match the day/night condition.

        Args:
            datasets_dict (dict[str, str]): Dictionary of dataset names to file names
            day_night_flag (str): Day/night condition flag
            granule_id (str): Granule identifier for logging

        Returns:
            dict[str, str]: Filtered dictionary with only compatible datasets
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
            item (Granule): Granule to process
            destination (Path): Base destination directory
            writer (Writer): Writer instance for output
            params (ConversionParams): Conversion parameters
            force (bool): If True, overwrite existing files. Defaults to False.

        Returns:
            dict[str, list]: Dictionary mapping granule_id to list of output paths
        """
        # Validate inputs using base class helper
        self._validate_save_inputs(item, params)

        # Parse datasets using base class helper
        datasets_dict = self._prepare_datasets(writer, params)

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

        # Filter existing files using base class helper
        datasets_dict = self._filter_existing_files(datasets_dict, destination, item.granule_id, writer, force)

        # Early return if no datasets to process
        if not datasets_dict:
            log.debug("All datasets already exist for %s, skipping", item.granule_id)
            return {item.granule_id: []}

        # Get files and extract day/night flag
        files = self.get_files(item)
        log.debug("Found %d files to process", len(files))
        day_night_flag = self._get_day_night_flag(files)

        # Handle dataset selection based on day/night mode
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
        """Validate a granule.

        Args:
            item (Granule): Granule to validate

        Raises:
            NotImplementedError: Validation not yet implemented
        """
        raise NotImplementedError(f"Validation not implemented for {self.source_name}")
