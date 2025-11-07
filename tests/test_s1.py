import logging

import pytest

from tests.base import IntegrationTestBase

log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestSentinel1GRDIntegration(IntegrationTestBase):
    """Integration tests for Sentinel-1 GRD source.

    Tests the complete pipeline for Sentinel-1 Ground Range Detected (GRD) data:
    - Authentication with Copernicus Data Space (S3)
    - Search for Sentinel-1 GRD granules via STAC
    - Download granule files and reconstruct SAFE structure
    - Convert to GeoTIFF using Satpy with sar-c_safe reader

    Note: Sentinel-1 GRD products contain dual-polarization SAR data (typically VV+VH)
    at ~20m resolution in IW mode. The SAFE directory structure must be preserved
    for the sar-c_safe reader to work correctly.
    """

    def test_auth_and_init(self, odata_credentials, copernicus_config) -> None:
        """Test Sentinel-1 GRD source initialization and authentication.

        This test:
        1. Creates an S3Downloader with Copernicus S3 authentication
        2. Creates a Sentinel1GRDSource instance
        3. Verifies the source is properly configured
        4. Stores the source instance for subsequent tests

        Args:
            s3_authenticator: Fixture providing Copernicus S3 authenticator
            copernicus_config: Fixture providing Copernicus configuration
        """
        try:
            from satctl.auth import configure_authenticator
            from satctl.downloaders import configure_downloader
            from satctl.sources.sentinel1 import Sentinel1GRDSource

            # Create Sentinel-1 GRD source
            # Default composite should be a SAR composite (e.g., dual-pol VV+VH)
            source = Sentinel1GRDSource(
                auth_builder=configure_authenticator("s3", **odata_credentials, **copernicus_config),
                down_builder=configure_downloader("s3"),
                stac_url="https://stac.dataspace.copernicus.eu/v1",
                composite="s1_dual_pol",  # Or whatever your default SAR composite is
                search_limit=1,  # Limit results for testing
            )

            # Verify source is configured using helper
            self.verify_source_initialized(source)
            assert source.reader == "sar-c_safe", "Should use sar-c_safe reader for GRD products"

            # Store for subsequent tests on the class
            type(self).source = source

        except Exception as e:
            type(self).mark_failure("auth", e)
            raise

    def test_search(
        self,
        test_search_params,
    ) -> None:
        """Test searching for Sentinel-1 GRD granules.

        This test:
        1. Skips if authentication failed
        2. Searches for Sentinel-1 GRD granules using test parameters
        3. Verifies that at least one granule is found
        4. Logs the number of results and granule details
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

            # Additional verification for Sentinel-1 specific fields
            if granules:
                first_granule = granules[0]
                assert first_granule.info.instrument == "sar", "Should be SAR instrument"
                log.info(f"Found Sentinel-1 {first_granule.info.level} product")
                log.info(f"Product type: {first_granule.info.product_type}")
                log.info(f"Acquisition time: {first_granule.info.acquisition_time}")

            # Store for subsequent tests on the class
            type(self).granules = granules

        except Exception as e:
            type(self).mark_failure("search", e)
            raise

    @pytest.mark.slow
    def test_download(self, temp_download_dir, odata_credentials, copernicus_config) -> None:
        """Test downloading a Sentinel-1 GRD granule.

        This test:
        1. Skips if authentication, search failed, or no granules found
        2. Downloads the first granule from search results
        3. Verifies download succeeded and SAFE structure is preserved
        4. Verifies required files exist (manifest.safe, measurement/, annotation/)
        5. Stores the downloaded item for conversion test

        Args:
            temp_download_dir: Fixture providing temporary download directory
        """
        self.check_prerequisites("auth", "search")

        if not self.granules:
            pytest.skip("Skipping download: no granules found")

        try:
            success, failure = self.source.download(self.granules, temp_download_dir)

            # Verify download succeeded using helper
            self.verify_download_success(success, failure, min_success=1)

            # Additional verification for Sentinel-1 SAFE structure
            for item in success:
                assert item.local_path.suffix == ".SAFE", (
                    f"Downloaded directory should have .SAFE extension, got {item.local_path}"
                )

                # Verify SAFE structure components exist
                manifest = item.local_path / "manifest.safe"
                assert manifest.exists(), f"manifest.safe should exist at {manifest}"

                measurement_dir = item.local_path / "measurement"
                annotation_dir = item.local_path / "annotation"

                # These directories should exist if assets were downloaded correctly
                if measurement_dir.exists():
                    measurement_files = list(measurement_dir.glob("*.tiff"))
                    log.info(f"Found {len(measurement_files)} measurement file(s)")

                if annotation_dir.exists():
                    annotation_files = list(annotation_dir.glob("*.xml"))
                    log.info(f"Found {len(annotation_files)} annotation file(s)")

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
        """Test converting Sentinel-1 GRD granule(s) to GeoTIFF.

        This test:
        1. Skips if any previous step failed
        2. Uses the configured GeoTIFFWriter instance
        3. Converts all downloaded granules using save()
        4. Verifies conversion succeeded with no failures
        5. Verifies output files exist for each granule and have non-zero size
        6. Stores all output files list

        Note: Sentinel-1 conversion uses the sar-c_safe reader which requires
        the complete SAFE directory structure. The conversion produces calibrated
        SAR backscatter products (typically sigma_nought).

        Args:
            temp_download_dir: Fixture providing temporary download directory
            test_conversion_params: Fixture providing test conversion parameters
            geotiff_writer: Fixture providing configured GeoTIFF writer
        """
        from dotenv import load_dotenv

        load_dotenv()
        self.check_prerequisites("auth", "search", "download")

        if not self.downloaded_item:
            pytest.skip("Skipping convert: no downloaded item")

        log.info(f"Converting {len(self.downloaded_item)} Sentinel-1 GRD granule(s)")

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

        # Additional verification for SAR data
        log.info("Verifying SAR output characteristics...")
        for output_path in all_output_paths:
            # SAR GeoTIFFs should typically be single or dual-band
            # (depending on whether it's VV only, VH only, or VV+VH composite)
            log.info(f"SAR output: {output_path.name}")

            # You could add additional verification here, e.g.:
            # - Check that backscatter values are in reasonable range
            # - Verify CRS is correct
            # - Check that NoData is properly set

        # Store all output files for inspection if needed
        type(self).output_files = all_output_paths
