import logging

import pytest

from tests.base import IntegrationTestBase

log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestOLCIIntegration(IntegrationTestBase):
    """Integration tests for OLCI source.

    Tests the complete pipeline for Sentinel-3 OLCI L1B data from Copernicus:
    - Authentication with Copernicus Data Space (OData)
    - Search for OLCI granules via STAC
    - Download granule files
    - Convert to GeoTIFF using Satpy
    """

    def test_auth_and_init(
        self,
        odata_credentials,
    ) -> None:
        """Test OLCI source initialization and authentication.

        This test:
        1. Creates an HTTPDownloader with Copernicus OData authentication
        2. Creates an OLCISource instance
        3. Verifies the source is properly configured
        4. Stores the source instance for subsequent tests

        Args:
            odata_authenticator: Fixture providing Copernicus OData authenticator
        """
        try:
            from satctl.auth import configure_authenticator
            from satctl.downloaders import configure_downloader
            from satctl.sources.sentinel3 import OLCISource

            # Create OLCI source
            source = OLCISource(
                auth_builder=configure_authenticator("odata", **odata_credentials),
                down_builder=configure_downloader("http"),
                stac_url="https://stac.dataspace.copernicus.eu/v1",
                search_limit=1,  # Limit results for testing
            )

            # Verify source is configured using helper
            self.verify_source_initialized(source)

            # Store for subsequent tests on the class
            type(self).source = source

        except Exception as e:
            type(self).mark_failure("auth", e)
            raise

    def test_search(
        self,
        test_search_params,
    ) -> None:
        """Test searching for OLCI granules.

        This test:
        1. Skips if authentication failed
        2. Searches for OLCI granules using test parameters
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
        """Test downloading an OLCI granule.

        This test:
        1. Skips if authentication, search failed, or no granules found
        2. Downloads the first granule from search results
        3. Verifies download succeeded
        4. Verifies files exist at the local_path
        5. Stores the downloaded item for conversion test

        Args:
            temp_download_dir: Fixture providing temporary download directory
            odata_authenticator: Fixture providing Copernicus OData authenticator
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
        """Test converting OLCI granule(s) to GeoTIFF.

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
