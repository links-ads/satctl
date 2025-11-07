import logging
import re
from abc import abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel
from pystac_client import Client
from satpy.scene import Scene
from xarray import DataArray

from satctl.auth import AuthBuilder
from satctl.downloaders import DownloadBuilder, Downloader
from satctl.model import ConversionParams, Granule, ProductInfo, SearchParams
from satctl.sources import DataSource
from satctl.writers import Writer

log = logging.getLogger(__name__)


class S1Asset(BaseModel):
    """Model for Sentinel-1 STAC asset.

    Attributes:
        href: URL to the asset file
        media_type: MIME type of the asset (optional)
    """

    href: str
    media_type: str | None


class Sentinel1Source(DataSource):
    """Source for Sentinel-1 SAR products.

    This class handles access to Sentinel-1 data via STAC catalogs, managing
    the download and processing of SAR data in SAFE format. It preserves the
    SAFE directory structure required by the satpy sar-c_safe reader.

    The SAFE (Standard Archive Format for Europe) structure includes:
    - manifest.safe: Product metadata
    - measurement/: SAR backscatter data (GeoTIFF)
    - annotation/: Detailed product metadata (XML)
    - preview/: Quick-look images
    - support/: Calibration data
    """

    # Required assets for SAR processing - both polarizations and their metadata
    REQUIRED_ASSETS: set[str] = {
        "vv",  # VV polarization measurement data
        "vh",  # VH polarization measurement data
        "schema-noise-vv",  # Noise calibration metadata for VV
        "schema-noise-vh",  # Noise calibration metadata for VH
        "schema-product-vv",  # Product metadata for VV
        "schema-product-vh",  # Product metadata for VH
        "schema-calibration-vv",  # Radiometric calibration metadata for VV
        "schema-calibration-vh",  # Radiometric calibration metadata for VH
    }

    # Optional metadata assets for visualization and validation
    METADATA_ASSETS: set[str] = {
        "safe_manifest",  # SAFE manifest file (required by sar-c_safe reader)
        "thumbnail",  # Quick-look preview image
    }

    def __init__(
        self,
        collection_name: str,
        *,
        reader: str,
        stac_url: str,
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str | None = "s3",
        default_downloader: str | None = "s3",
        default_composite: str | None = None,
        default_resolution: int | None = None,
        search_limit: int = 100,
    ):
        """Initialize Sentinel-1 data source.

        Args:
            collection_name (str): Name of the Sentinel-1 collection (e.g., "sentinel-1-grd")
            reader (str): Satpy reader name for this product type (typically "sar-c_safe")
            stac_url (str): URL of the STAC catalog API endpoint
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None. Defaults to "s3".
            default_downloader (str | None): Default downloader name to use when down_builder is None. Defaults to "s3".
            default_composite (str | None): Default composite/band to load. Defaults to None.
            default_resolution (int | None): Default resolution in meters. Defaults to None.
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
        """
        super().__init__(
            collection_name,
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite=default_composite,
            default_resolution=default_resolution,
        )
        self.reader = reader
        self.stac_url = stac_url
        self.search_limit = search_limit

    @abstractmethod
    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse Sentinel-1 item name into product information.

        This method must be implemented by subclasses to handle specific
        product naming conventions (e.g., GRD vs SLC format).

        Args:
            name: Sentinel-1 item identifier (SAFE directory name)

        Returns:
            ProductInfo: Parsed product metadata including satellite, mode, level, and time

        Raises:
            ValueError: If name format is invalid
        """
        ...

    def search(self, params: SearchParams) -> list[Granule]:
        """Search for Sentinel-1 data using STAC catalog.

        Args:
            params: Search parameters including:
                - area_geometry: Geographic area of interest (GeoJSON)
                - start: Start datetime for temporal filtering
                - end: End datetime for temporal filtering

        Returns:
            List of matching granules with metadata and asset information.
            Each granule includes:
            - granule_id: Unique identifier
            - source: Collection name
            - assets: Dictionary of available assets (measurements, metadata)
            - info: Parsed product information (satellite, mode, time, etc.)
        """
        log.debug("Setting up the STAC client")
        catalogue = Client.open(self.stac_url)

        log.debug("Searching catalog")
        search = catalogue.search(
            collections=self.collections,
            intersects=params.area_geometry,
            datetime=(params.start, params.end),
            max_items=self.search_limit,
        )

        # Convert STAC items to internal Granule model
        items = [
            Granule(
                granule_id=i.id,
                source=self.collections[0],
                assets={k: S1Asset(href=v.href, media_type=v.media_type) for k, v in i.assets.items()},
                info=self._parse_item_name(i.id),
            )
            for i in search.items()
        ]
        log.debug("Found %d items", len(items))
        return items

    def get_by_id(self, item_id: str, **kwargs) -> Granule:
        """Get specific Sentinel-1 granule by ID.

        Args:
            item_id: Granule identifier
            **kwargs: Additional keyword arguments (unused)

        Returns:
            Granule: Requested granule with metadata

        Raises:
            ValueError: If granule not found
        """
        log.debug("Fetching Sentinel-1 granule by ID: %s", item_id)
        catalogue = Client.open(self.stac_url)

        try:
            collection = catalogue.get_collection(self.collections[0])
            stac_item = collection.get_item(item_id)
            if stac_item is None:
                raise ValueError(f"No granule found with id: {item_id}")
        except Exception as e:
            log.error("Failed to fetch granule %s: %s", item_id, e)
            raise ValueError(f"No granule found with id: {item_id}") from e

        return Granule(
            granule_id=stac_item.id,
            source=self.collections[0],
            assets={k: S1Asset(href=v.href, media_type=v.media_type) for k, v in stac_item.assets.items()},
            info=self._parse_item_name(stac_item.id),
        )

    def get_files(self, item: Granule) -> list[Path | str]:
        """Get list of all files in a downloaded Sentinel-1 SAFE directory.

        The sar-c_safe reader requires access to all files in the SAFE structure,
        including measurements, annotations, and metadata files.

        Args:
            item: Granule with local_path pointing to the .SAFE directory

        Returns:
            List of all file paths in the SAFE directory (excluding directories
            and the _granule.json metadata file)

        Raises:
            ValueError: If local_path is not set or SAFE structure is invalid
        """
        if item.local_path is None:
            raise ValueError("Local path is missing. Did you download this granule?")

        # Verify SAFE structure by checking for manifest file
        manifest_file = item.local_path / "manifest.safe"

        if manifest_file.exists():
            # Recursively collect all files in SAFE structure
            all_files = [f for f in item.local_path.rglob("*") if f.is_file()]
            # Exclude internal metadata file used for tracking
            all_files = [f for f in all_files if f.name != "_granule.json"]
            return cast(list[Path | str], all_files)
        else:
            raise ValueError("SAFE structure not found - manifest.safe is missing")

    def validate(self, item: Granule) -> None:
        """Validate a Sentinel-1 STAC item has expected asset types.

        Checks that all assets have valid media types for SAR data processing.

        Args:
            item: STAC item to validate

        Raises:
            AssertionError: If any asset has an unexpected media type
        """
        for name, asset in item.assets.items():
            asset = cast(S1Asset, asset)
            # Sentinel-1 assets are typically Cloud-Optimized GeoTIFFs, XMLs, PNGs, or ZIPs
            assert asset.media_type in (
                "image/tiff; application=geotiff; profile=cloud-optimized",
                "image/png",
                "application/zip",
                "application/xml",
            ), f"Unexpected media type for asset {name}: {asset.media_type}"

    def load_scene(
        self,
        item: Granule,
        datasets: list[str] | None = None,
        generate: bool = False,
        calibration: str = "counts",
        **scene_options: dict[str, Any],
    ) -> Scene:
        """Load a Sentinel-1 scene into a Satpy Scene object.

        Note: The 'calibration' parameter is currently unused but retained for
        API compatibility. SAR calibration is typically specified in the dataset
        name or composite definition (e.g., 'sigma_nought', 'beta_nought').

        Args:
            item: Granule to load (must have local_path set)
            datasets: List of datasets/composites to load (e.g., ['measurement_vv'])
            generate: Whether to generate composites (unused)
            calibration: Calibration type - retained for compatibility but not used
                        (actual calibration is specified in dataset queries)
            **scene_options: Additional options passed to Scene reader_kwargs

        Returns:
            Loaded satpy Scene object with requested datasets

        Raises:
            ValueError: If datasets is None and no default composite is configured
        """
        if not datasets:
            if self.default_composite is None:
                raise ValueError("Please provide the source with a default composite, or provide custom composites")
            datasets = [self.default_composite]

        # Create scene with all files in SAFE directory
        scene = Scene(
            filenames=self.get_files(item),
            reader=self.reader,
            reader_kwargs=scene_options,
        )

        # Load datasets (calibration is handled by dataset definition, not this parameter)
        # TODO: Remove unused calibration parameter in future version
        scene.load(datasets, calibration=calibration)
        return scene

    def download_item(self, item: Granule, destination: Path, downloader: Downloader) -> bool:
        """Download Sentinel-1 assets and reconstruct SAFE directory structure.

        Downloads only the required assets (measurements and metadata) and organizes
        them into a valid SAFE directory structure that the sar-c_safe reader can process.

        The SAFE structure is reconstructed by:
        1. Extracting relative paths from S3 URIs (.SAFE/measurement/*, .SAFE/annotation/*)
        2. Creating the directory hierarchy (measurement/, annotation/, etc.)
        3. Downloading files to their proper locations
        4. Adding the manifest.safe file

        Args:
            item: Sentinel-1 granule to download with asset information
            destination: Path to the base destination directory

        Returns:
            True if all required assets were downloaded successfully, False otherwise.
            Metadata assets (thumbnail, preview) are optional and won't cause failure.
        """
        self.validate(item)

        # Create directory with .SAFE extension for sar-c_safe reader compatibility
        local_path = destination / f"{item.granule_id}.SAFE"
        local_path.mkdir(parents=True, exist_ok=True)

        all_success = True

        # Download required measurement and annotation files
        for asset_name in self.REQUIRED_ASSETS:
            asset = item.assets.get(asset_name)
            if asset is None:
                log.warning("Missing required asset '%s' for granule %s", asset_name, item.granule_id)
                all_success = False
                continue
            asset = cast(S1Asset, asset)

            # Extract the relative path from S3 URI to preserve SAFE structure
            # Example: s3://bucket/path/file.SAFE/measurement/data.tif -> measurement/data.tif
            href_parts = asset.href.split(".SAFE/")
            if len(href_parts) > 1 and ("measurement" in href_parts[1] or "annotation" in href_parts[1]):
                # Preserve the SAFE structure for proper sar-c_safe reader support
                relative_path = href_parts[1]
                target_file = local_path / relative_path
            else:
                # Fallback to flat structure if pattern not found
                target_file = local_path / (asset_name + Path(asset.href).suffix)

            # Create subdirectories (measurement/, annotation/, etc.)
            target_file.parent.mkdir(parents=True, exist_ok=True)

            result = downloader.download(
                uri=asset.href,
                destination=target_file,
                item_id=item.granule_id,
            )
            if not result:
                log.warning("Failed to download asset %s for granule %s", asset_name, item.granule_id)
                all_success = False

        # Download optional metadata files (manifest, thumbnail, etc.)
        for metadata_name in self.METADATA_ASSETS:
            metadata = item.assets.get(metadata_name)
            if metadata is None:
                log.debug("Missing optional metadata '%s' for granule %s", metadata_name, item.granule_id)
                continue
            metadata = cast(S1Asset, metadata)

            # Extract relative path from S3 URI
            href_parts = metadata.href.split(".SAFE/")
            if len(href_parts) > 1:
                relative_path = href_parts[1]
                target_file = local_path / relative_path
            else:
                # Fallback to root of SAFE directory
                target_file = local_path / Path(metadata.href).name

            target_file.parent.mkdir(parents=True, exist_ok=True)
            result = downloader.download(
                uri=metadata.href,
                destination=target_file,
                item_id=item.granule_id,
            )
            if not result:
                log.debug("Failed to download optional metadata %s for granule %s", metadata_name, item.granule_id)
                # Don't mark as failure for optional metadata

        if all_success:
            # Save granule metadata for tracking and future reference
            item.local_path = local_path
            log.debug("Saving granule metadata to: %s", local_path)
            item.to_file(local_path)
        else:
            log.warning("Failed to download all required assets for: %s", item.granule_id)

        return all_success

    def save_item(
        self,
        item: Granule,
        destination: Path,
        writer: Writer,
        params: ConversionParams,
        force: bool = False,
    ) -> dict[str, list]:
        """Process and save Sentinel-1 granule to output files.

        Workflow:
        1. Validate inputs (local files exist, datasets specified)
        2. Load scene with SAR data using sar-c_safe reader
        3. Define target area (from params or full granule extent)
        4. Resample to target projection and resolution
        5. Write datasets to output files

        Args:
            item: Granule to process (must have local_path set)
            destination: Base destination directory for outputs
            writer: Writer instance for file output (GeoTIFF, NetCDF, etc.)
            params: Conversion parameters including:
                - datasets: List of datasets to process
                - area_geometry: Optional AOI for spatial subsetting
                - target_crs: Target coordinate reference system
                - resolution: Target spatial resolution
            force: If True, overwrite existing output files. Defaults to False.

        Returns:
            Dictionary mapping granule_id to list of output file paths

        Raises:
            FileNotFoundError: If local_path doesn't exist
            ValueError: If datasets is None and no default composite is configured
        """
        # Validate that granule was downloaded
        if item.local_path is None or not item.local_path.exists():
            raise FileNotFoundError(f"Invalid source file or directory: {item.local_path}")

        # Ensure datasets are specified
        if params.datasets is None and self.default_composite is None:
            raise ValueError("Missing datasets or default composite for storage")

        # Parse dataset names and prepare output filenames
        datasets_dict = self._prepare_datasets(writer, params)

        # Filter existing files using base class helper
        datasets_dict = self._filter_existing_files(datasets_dict, destination, item.granule_id, writer, force)

        # Load scene with requested SAR datasets
        log.debug("Loading and resampling scene")
        scene = self.load_scene(item, datasets=list(datasets_dict.values()))

        # Define target area for resampling
        if params.area_geometry is not None:
            # User provided AOI - use it with specified CRS and resolution
            area_def = self.define_area(
                target_crs=params.target_crs_obj,
                area=params.area_geometry,
                source_crs=params.source_crs_obj,
                resolution=params.resolution,
            )
        else:
            # No AOI provided - use entire granule extent
            area_def = self.define_area(
                target_crs=params.target_crs_obj,
                scene=scene,
                source_crs=params.source_crs_obj,
                resolution=params.resolution,
            )

        # Resample to target area
        scene = self.resample(scene, area_def=area_def)

        # Write each dataset to output file
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


class Sentinel1GRDSource(Sentinel1Source):
    """Source for Sentinel-1 Ground Range Detected (GRD) products.

    GRD products are multi-looked and projected to ground range, suitable
    for most operational SAR applications. They have ~20m spatial resolution
    in IW mode and are available in VV+VH or HH+HV polarizations.
    """

    def __init__(
        self,
        *,
        stac_url: str,
        composite: str = "sar_rgb",
        auth_builder: AuthBuilder | None = None,
        down_builder: DownloadBuilder | None = None,
        default_authenticator: str | None = "s3",
        default_downloader: str | None = "s3",
        search_limit: int = 100,
    ):
        """Initialize Sentinel-1 GRD data source.

        Args:
            stac_url (str): URL of the STAC catalog API endpoint
            composite (str): Default composite to load. Defaults to "sar_rgb".
            auth_builder (AuthBuilder | None): Factory that creates an authenticator object on demand. Defaults to None.
            down_builder (DownloadBuilder | None): Factory that creates a downloader object on demand. Defaults to None.
            default_authenticator (str | None): Default authenticator name to use when auth_builder is None. Defaults to "s3".
            default_downloader (str | None): Default downloader name to use when down_builder is None. Defaults to "s3".
            search_limit (int): Maximum number of items to return per search. Defaults to 100.
        """
        super().__init__(
            "sentinel-1-grd",
            reader="sar-c_safe",
            auth_builder=auth_builder,
            down_builder=down_builder,
            default_authenticator=default_authenticator,
            default_downloader=default_downloader,
            default_composite=composite,
            default_resolution=20,  # Native GRD resolution in IW mode
            stac_url=stac_url,
            search_limit=search_limit,
        )

    def _parse_item_name(self, name: str) -> ProductInfo:
        """Parse Sentinel-1 GRD product name to extract metadata.

        Expected format: S1X_MM_LLLL_PSSS_YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS_OOOOOO_DDDDDD_XXXX[_COG][.SAFE][.zip]
        Where:
        - S1X: Satellite (S1A, S1B, S1C)
        - MM: Acquisition mode (EW, IW, SM, WV)
        - LLLL: Product level (GRDH, GRDM, etc.)
        - PSSS: Polarization and class (1SDH, 1SDV, etc.)
        - YYYYMMDDTHHMMSS: Sensing start time

        Example: S1A_EW_GRDM_1SDH_20250915T081809_20250915T081914_060996_079982_90C1_COG.SAFE.zip

        Args:
            name: Product filename or identifier

        Returns:
            ProductInfo with satellite, mode, level, and acquisition time

        Raises:
            ValueError: If name format doesn't match expected pattern
        """
        pattern = r"(S1[ABC])_([A-Z]{2})_([A-Z]{4})_1S[A-Z]{2}_(\d{8}T\d{6})_"

        match = re.match(pattern, name)
        if not match:
            raise ValueError(f"Invalid Sentinel-1 .SAFE directory format: {name}")

        groups = match.groups()
        satellite = groups[0]  # S1A, S1B, or S1C
        # acquisition_mode = groups[1]  # EW (Extra Wide), IW (Interferometric Wide), etc.
        level = groups[2]  # GRDH (High res), GRDM (Medium res)
        sensing_time = groups[3]  # Start of acquisition

        acquisition_time = datetime.strptime(sensing_time, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)

        return ProductInfo(
            instrument="sar",
            level=level,
            product_type=f"S1{satellite}",
            acquisition_time=acquisition_time,
        )
