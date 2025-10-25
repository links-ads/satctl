"""Integration tests for satellite data sources.

These tests verify the complete workflow for each source:
1. Authentication and initialization
2. Search for granules
3. Download granules
4. Convert/process granules to output format

Tests are designed to run in sequence. If any step fails, subsequent steps
will be skipped automatically using class-level state tracking.
"""

import logging
from pathlib import Path

import pytest

from satctl.model import Granule
from satctl.sources import DataSource

log = logging.getLogger(__name__)


class IntegrationTestBase:
    """Base class for source integration tests with pipeline state management.

    This class provides infrastructure for running tests in a pipeline where
    each test depends on the success of previous tests. State is stored in
    class attributes that are checked before running dependent tests.

    Attributes:
        source: DataSource instance (set by test_auth_and_init)
        granules: List of granules from search (set by test_search)
        downloaded_item: Downloaded granule (set by test_download)
        output_files: List of output file paths (set by test_convert)
        _auth_failed: Flag indicating auth failure
        _search_failed: Flag indicating search failure
        _download_failed: Flag indicating download failure
    """

    source: DataSource | None = None
    granules: list[Granule] = []
    downloaded_item: list[Granule] = []
    output_files: list[Path] = []

    _auth_failed: bool = False
    _search_failed: bool = False
    _download_failed: bool = False

    @classmethod
    def reset_state(cls) -> None:
        """Reset all state variables. Called at start of each test class."""
        cls.source = None
        cls.granules = []
        cls.downloaded_item = []
        cls.output_files = []
        cls._auth_failed = False
        cls._search_failed = False
        cls._download_failed = False

    @classmethod
    def check_prerequisites(cls, *steps: str) -> None:
        """Check if any prerequisite steps failed and skip if necessary.

        Args:
            *steps: Step names to check ('auth', 'search', 'download')

        Raises:
            pytest.skip: If any prerequisite step failed
        """
        if "auth" in steps and cls._auth_failed:
            pytest.skip("Skipping: authentication failed")
        if "search" in steps and cls._search_failed:
            pytest.skip("Skipping: search failed")
        if "download" in steps and cls._download_failed:
            pytest.skip("Skipping: download failed")

    @classmethod
    def mark_failure(cls, step: str, error: Exception) -> None:
        """Mark a step as failed and log the error.

        Args:
            step: Step name ('auth', 'search', 'download')
            error: The exception that caused the failure
        """
        if step == "auth":
            cls._auth_failed = True
        elif step == "search":
            cls._search_failed = True
        elif step == "download":
            cls._download_failed = True

        log.error(f"{step.capitalize()} failed: {type(error).__name__}: {error}")


# Run once per test class in this module to reset IntegrationTestBase-derived classes
@pytest.fixture(scope="class", autouse=True)
def integration_class_setup(request):
    """Reset class state before and after each IntegrationTestBase-derived test class."""
    cls = getattr(request, "cls", None)
    if cls and issubclass(cls, IntegrationTestBase):
        cls.reset_state()
        yield
        # Tear down / reset after class finishes to avoid leaking state between classes
        cls.reset_state()
    else:
        yield


@pytest.mark.integration
@pytest.mark.requires_credentials
@pytest.mark.slow
class TestVIIRSL1BIntegration(IntegrationTestBase):
    """Integration tests for VIIRS L1B source.

    Tests the complete pipeline for VIIRS Level 1B data from NASA EarthData:
    - Authentication with NASA EarthData
    - Search for VIIRS granules
    - Download granule files
    - Convert to GeoTIFF using Satpy
    """

    def test_auth_and_init(
        self,
        earthdata_authenticator,
    ) -> None:
        """Test VIIRS source initialization and authentication.

        This test:
        1. Creates an HTTPDownloader with EarthData authentication
        2. Initializes the downloader
        3. Creates a VIIRSL1BSource instance
        4. Verifies the source is properly configured
        5. Stores the source instance for subsequent tests

        Args:
            earthdata_authenticator: Fixture providing EarthData authenticator
        """
        try:
            from satctl.downloaders import HTTPDownloader
            from satctl.sources.viirs import VIIRSL1BSource

            # Create and initialize downloader
            downloader = HTTPDownloader(authenticator=earthdata_authenticator)

            # Create VIIRS source with NPP satellite and M-band product (750m resolution)
            source = VIIRSL1BSource(
                downloader=downloader,
                satellite=["vnp"],  # NPP satellite
                product_type=["mod"],  # M-bands (750m)
                search_limit=1,  # Limit results for testing
            )

            # Verify source is configured
            assert source is not None, "Source should be created"
            assert source.downloader is not None, "Downloader should be set"
            assert len(source.combinations) > 0, "Should have at least one satellite/product combination"

            # Store for subsequent tests on the class (so other test methods can access it)
            type(self).source = source

        except Exception as e:
            type(self).mark_failure("auth", e)
            raise

    def test_search(
        self,
        test_search_params,
    ) -> None:
        """Test searching for VIIRS granules.

        This test:
        1. Skips if authentication failed
        2. Searches for VIIRS granules using test parameters
        3. Verifies that at least one granule is found
        4. Logs the number of results
        5. Stores the granules for subsequent tests

        Args:
            test_search_params: Fixture providing test search parameters
        """
        self.check_prerequisites("auth")

        try:
            # Search for granules
            granules = self.source.search(test_search_params)

            # Verify we got results
            assert isinstance(granules, list), "Search should return a list"
            assert len(granules) > 0, f"Search should return at least one granule, got {len(granules)}"

            # Log what we found
            log.info(f"Found {len(granules)} VIIRS granules")
            if granules:
                log.info(f"First granule ID: {granules[0].granule_id}")

            # Store for subsequent tests on the class
            type(self).granules = granules

        except Exception as e:
            type(self).mark_failure("search", e)
            raise

    def test_download(
        self,
        temp_download_dir,
    ) -> None:
        """Test downloading a VIIRS granule.

        This test:
        1. Skips if authentication, search failed, or no granules found
        2. Downloads the first granule from search results
        3. Verifies download succeeded
        4. Verifies files exist at the local_path
        5. Stores the downloaded item for conversion test

        Args:
            temp_download_dir: Fixture providing temporary download directory
        """
        self.check_prerequisites("auth", "search")

        if not self.granules:
            pytest.skip("Skipping download: no granules found")

        try:
            success, failure = self.source.download(self.granules, temp_download_dir)

            # Verify download succeeded
            assert len(success) > 0, f"Should have at least one successful download, got {len(success)}"
            assert len(failure) == 0, f"Should have no failed downloads, got {len(failure)} failures"

            # Verify local_path is set and exists
            for item in success:
                assert item.local_path is not None, "local_path should be set after download"
                assert item.local_path.exists(), f"Downloaded files should exist at {item.local_path}"

                # Verify we have some files
                files = list(item.local_path.glob("*.nc"))
                assert len(files) > 0, f"Should have downloaded .nc files, found {len(files)}"
                log.info(f"Downloaded {len(files)} files to {item.local_path}")

                # Store for subsequent tests on the class
                type(self).downloaded_item.append(item)

        except Exception as e:
            type(self).mark_failure("download", e)
            raise

    def test_convert(
        self,
        temp_download_dir,
        test_conversion_params,
        geotiff_writer,
    ) -> None:
        """Test converting VIIRS granule(s) to GeoTIFF.

        This test:
        1. Skips if any previous step failed
        2. Uses the configured GeoTIFFWriter instance
        3. Converts all downloaded granules using save()
        4. Verifies conversion succeeded with no failures
        5. Verifies output files exist for each granule and have non-zero size
        6. Stores all output files list

        Args:
            temp_download_dir: Fixture providing temporary download directory
            test_conversion_params: Fixture providing test conversion parameters
            geotiff_writer: Fixture providing configured GeoTIFF writer
        """
        self.check_prerequisites("auth", "search", "download")

        if not self.downloaded_item:
            pytest.skip("Skipping convert: no downloaded item")

        log.info(f"Converting {len(self.downloaded_item)} granule(s)")

        # Convert granule(s) to GeoTIFF using save()
        success, failure = self.source.save(
            self.downloaded_item,
            test_conversion_params,
            temp_download_dir,
            geotiff_writer,
            force=False,
        )

        # Verify conversion succeeded
        assert len(success) > 0, f"Should have at least one successful conversion, got {len(success)}"
        assert len(failure) == 0, f"Should have no conversion failures, got {len(failure)}"

        log.info(f"Successfully processed {len(success)} granule(s)")

        # Collect all output paths from all processed granules
        all_output_paths = []

        # Verify each successfully processed granule
        for granule in success:
            granule_id = granule.granule_id
            log.info(f"Verifying output for granule: {granule_id}")

            # Find output files in the granule's output directory
            output_dir = temp_download_dir / granule_id
            assert output_dir.exists(), f"Output directory should exist: {output_dir}"

            # Collect all output files (assuming GeoTIFF extension)
            output_paths = list(output_dir.glob(f"*.{geotiff_writer.extension}"))
            assert len(output_paths) > 0, (
                f"Should have at least one output file for {granule_id}, got {len(output_paths)}"
            )

            log.info(f"Created {len(output_paths)} output file(s) for {granule_id}")

            # Verify each output file exists and has content
            for output_path in output_paths:
                assert isinstance(output_path, Path), f"Output path should be a Path object, got {type(output_path)}"
                assert output_path.exists(), f"Output file should exist: {output_path}"

                file_size = output_path.stat().st_size
                assert file_size > 0, f"Output file should have non-zero size: {output_path} ({file_size} bytes)"

                log.info(f"  {output_path.name}: {file_size:,} bytes")
                all_output_paths.append(output_path)

        # Store all output files for inspection if needed
        type(self).output_files = all_output_paths
