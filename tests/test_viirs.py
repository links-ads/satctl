import logging

import pytest

from tests.base import IntegrationTestBase

log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestVIIRSL1BIntegration(IntegrationTestBase):
    """Integration tests for VIIRS L1B source.

    Tests the complete pipeline for VIIRS Level 1B data from NASA EarthData:
    - Authentication with NASA EarthData
    - Search for VIIRS granules
    - Download granule files
    - Convert to GeoTIFF using Satpy
    """

    def test_auth_and_init(self, earthdata_credentials) -> None:
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
            from satctl.auth import configure_authenticator
            from satctl.downloaders import configure_downloader
            from satctl.sources.viirs import VIIRSL1BSource

            # Create VIIRS source with NPP satellite and M-band product (750m resolution)
            source = VIIRSL1BSource(
                auth_builder=configure_authenticator("earthdata", **earthdata_credentials),
                down_builder=configure_downloader("http"),
                satellite=["vnp"],  # NPP satellite
                product_type=["mod"],  # M-bands (750m)
                search_limit=1,  # Limit results for testing
            )

            # Verify source is configured using helper
            self.verify_source_initialized(source)
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

            # Verify we got results using helper
            self.verify_search_results(granules, min_count=1)

            # Store for subsequent tests on the class
            type(self).granules = granules

        except Exception as e:
            type(self).mark_failure("search", e)
            raise

    @pytest.mark.slow
    def test_download(self, temp_download_dir) -> None:
        """Test downloading a VIIRS granule.

        This test:
        1. Skips if authentication, search failed, or no granules found
        2. Downloads the first granule from search results
        3. Verifies download succeeded
        4. Verifies files exist at the local_path
        5. Stores the downloaded item for conversion test

        Args:
            temp_download_dir: Fixture providing temporary download directory
            earthdata_authenticator: Fixture providing EarthData authenticator
        """
        self.check_prerequisites("auth", "search")

        if not self.granules:
            pytest.skip("Skipping download: no granules found")

        try:
            success, failure = self.source.download(self.granules, temp_download_dir)
            # Verify download succeeded using helper
            self.verify_download_success(success, failure, min_success=1)
            # Store for subsequent tests on the class
            type(self).downloaded_item.extend(success)

        except Exception as e:
            type(self).mark_failure("download", e)
            raise

    @pytest.mark.slow
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

        # Verify conversion succeeded using helper
        all_output_paths = self.verify_conversion_output(
            success,
            failure,
            temp_download_dir,
            geotiff_writer,
            min_success=1,
        )

        # Store all output files for inspection if needed
        type(self).output_files = all_output_paths
