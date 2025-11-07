import logging

import pytest

from tests.base import IntegrationTestBase

log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestMTGIntegration(IntegrationTestBase):
    """Integration tests for MTG source.

    Tests the complete pipeline for EUMETSAT MTG (Meteosat Third Generation) data:
    - Authentication with EUMETSAT DataStore
    - Search for MTG granules via EUMDAC
    - Download granule files and extract ZIP structure
    - Convert to GeoTIFF using Satpy with FCI reader

    Note: MTG products contain high-resolution imagery from the Flexible Combined
    Imager (FCI) instrument. The data is stored in NetCDF format within ZIP archives
    and needs to be extracted for processing.
    """

    def test_auth_and_init(
        self,
        eumetsat_credentials,
    ) -> None:
        """Test MTG source initialization and authentication.

        This test:
        1. Creates an MTGSource instance with EUMETSAT authentication
        2. Verifies the source is properly configured
        3. Stores the source instance for subsequent tests

        Args:
            eumetsat_authenticator: Fixture providing EUMETSAT authenticator
        """
        try:
            from satctl.auth import configure_authenticator
            from satctl.downloaders import configure_downloader
            from satctl.sources.mtg import MTGSource

            # Create MTG source
            # Default composite should be a visible/IR composite (e.g., natural_color, airmass)
            source = MTGSource(
                auth_builder=configure_authenticator("eumetsat", **eumetsat_credentials),
                down_builder=configure_downloader("http"),
                collection_name="EO:EUM:DAT:0662",  # Or appropriate MTG collection
                reader="fci_l1c_nc",  # FCI Level 1C NetCDF reader
                default_composite="simple_fci_fire_mask",  # Or whatever your default composite is
                default_resolution=2000,  # 2km resolution for FCI
                search_limit=1,  # Limit results for testing
            )

            # Verify source is configured using helper
            self.verify_source_initialized(source)
            assert source.reader == "fci_l1c_nc", "Should use fci_l1c_nc reader for MTG FCI products"

            # Store for subsequent tests on the class
            type(self).source = source

        except Exception as e:
            type(self).mark_failure("auth", e)
            raise

    def test_search(
        self,
        test_mtg_search_params,
    ) -> None:
        """Test searching for MTG granules.

        This test:
        1. Skips if authentication failed
        2. Searches for MTG granules using test parameters
        3. Verifies that at least one granule is found
        4. Logs the number of results and granule details
        5. Stores the granules for subsequent tests

        Args:
            test_search_params: Fixture providing test search parameters
        """
        self.check_prerequisites("auth")

        try:
            # Search for granules
            granules = self.source.search(test_mtg_search_params)

            # Verify we got results using helper
            self.verify_search_results(granules, min_count=1)

            # Additional verification for MTG specific fields
            if granules:
                first_granule = granules[0]
                log.info(f"Found MTG product from {first_granule.info.instrument} instrument")
                log.info(f"Product type: {first_granule.info.product_type}")
                log.info(f"Acquisition time: {first_granule.info.acquisition_time}")

                # Verify MTG asset structure
                assert "product" in first_granule.assets, "MTG granule should have 'product' asset"
                product_asset = first_granule.assets["product"]
                assert hasattr(product_asset, "href"), "Product asset should have href attribute"
                log.info(f"Product URL: {product_asset.href}")

            # Store for subsequent tests on the class
            type(self).granules = granules

        except Exception as e:
            type(self).mark_failure("search", e)
            raise

    @pytest.mark.slow
    def test_download(self, temp_download_dir) -> None:
        """Test downloading an MTG granule.

        This test:
        1. Skips if authentication, search failed, or no granules found
        2. Downloads the first granule from search results
        3. Verifies download succeeded and ZIP was extracted to .MTG directory
        4. Verifies required files exist (NetCDF files)
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

            # Additional verification for MTG structure
            for item in success:
                assert item.local_path.suffix == ".MTG", (
                    f"Downloaded directory should have .MTG extension, got {item.local_path}"
                )

                # Verify extracted contents exist
                nc_files = list(item.local_path.glob("*.nc"))
                log.info(f"Found {len(nc_files)} NetCDF file(s)")
                assert len(nc_files) > 0, "Should have at least one NetCDF file"

                # Check for metadata file
                metadata_file = item.local_path / "granule.json"
                if metadata_file.exists():
                    log.info("Granule metadata file saved successfully")

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
        """Test converting MTG granule(s) to GeoTIFF.

        This test:
        1. Skips if any previous step failed
        2. Uses the configured GeoTIFFWriter instance
        3. Converts all downloaded granules using save()
        4. Verifies conversion succeeded with no failures
        5. Verifies output files exist for each granule and have non-zero size
        6. Stores all output files list

        Note: MTG conversion uses the fci_l1c_nc reader which requires
        NetCDF files. The conversion produces calibrated radiance or
        reflectance products that can be composed into RGB imagery.

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

        log.info(f"Converting {len(self.downloaded_item)} MTG granule(s)")

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

        # Additional verification for MTG data
        log.info("Verifying MTG output characteristics...")
        for output_path in all_output_paths:
            # MTG GeoTIFFs can be multi-band (RGB composites) or single-band
            log.info(f"MTG output: {output_path.name}")

            # You could add additional verification here, e.g.:
            # - Check that radiance/reflectance values are in reasonable range
            # - Verify CRS is correct (should be geographic or appropriate projection)
            # - Check that NoData is properly set
            # - Verify spatial extent matches expected coverage

        # Store all output files for inspection if needed
        type(self).output_files = all_output_paths
