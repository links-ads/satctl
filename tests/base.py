import logging
from pathlib import Path

import pytest

from satctl.model import Granule
from satctl.sources import DataSource
from satctl.writers import Writer

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

    source: DataSource = None  # type: ignore
    granules: list[Granule] = []
    downloaded_item: list[Granule] = []
    output_files: list[Path] = []

    _auth_failed: bool = False
    _search_failed: bool = False
    _download_failed: bool = False

    @classmethod
    def reset_state(cls) -> None:
        """Reset all state variables. Called at start of each test class."""
        cls.source = None  # type: ignore
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

    @classmethod
    def verify_source_initialized(cls, source: DataSource) -> None:
        """Verify that a source is properly initialized.

        Args:
            source: DataSource instance to verify

        Raises:
            AssertionError: If source is not properly configured
        """
        assert source is not None, "Source should be created"
        assert source.authenticator is not None, "Authenticator should be set"

    @classmethod
    def verify_search_results(cls, granules: list[Granule], min_count: int = 1) -> None:
        """Verify search results are valid.

        Args:
            granules: List of granules from search
            min_count: Minimum number of granules expected

        Raises:
            AssertionError: If search results are invalid
        """
        assert isinstance(granules, list), "Search should return a list"
        assert len(granules) >= min_count, f"Search should return at least {min_count} granule(s), got {len(granules)}"

        if granules:
            log.info(f"Found {len(granules)} granule(s)")
            log.info(f"First granule ID: {granules[0].granule_id}")

    @classmethod
    def verify_download_success(
        cls,
        success: list[Granule],
        failure: list[Granule],
        min_success: int = 1,
    ) -> None:
        """Verify download operation succeeded.

        Args:
            success: List of successfully downloaded granules
            failure: List of failed downloads
            min_success: Minimum number of successful downloads expected

        Raises:
            AssertionError: If download did not meet expectations
        """
        assert len(success) >= min_success, (
            f"Should have at least {min_success} successful download(s), got {len(success)}"
        )
        assert len(failure) == 0, f"Should have no failed downloads, got {len(failure)} failure(s)"

        # Verify local_path is set and exists
        for item in success:
            assert item.local_path is not None, "local_path should be set after download"
            assert item.local_path.exists(), f"Downloaded files should exist at {item.local_path}"
            log.info(f"Downloaded to {item.local_path}")

    @classmethod
    def verify_conversion_output(
        cls,
        success: list[Granule],
        failure: list[Granule],
        output_base_dir: Path,
        writer: Writer,
        min_success: int = 1,
    ) -> list[Path]:
        """Verify conversion/processing output files.

        Args:
            success: List of successfully processed granules
            failure: List of failed conversions
            output_base_dir: Base directory where outputs are stored
            writer: Writer instance used for conversion
            min_success: Minimum number of successful conversions expected

        Returns:
            list[Path]: List of all output file paths

        Raises:
            AssertionError: If conversion output is invalid
        """
        assert len(success) >= min_success, (
            f"Should have at least {min_success} successful conversion(s), got {len(success)}"
        )
        assert len(failure) == 0, f"Should have no conversion failures, got {len(failure)}"

        log.info(f"Successfully processed {len(success)} granule(s)")

        all_output_paths = []

        # Verify each successfully processed granule
        for granule in success:
            granule_id = granule.granule_id
            log.info(f"Verifying output for granule: {granule_id}")

            # Find output files in the granule's output directory
            output_dir = output_base_dir / granule_id
            assert output_dir.exists(), f"Output directory should exist: {output_dir}"

            # Collect all output files
            output_paths = list(output_dir.glob(f"*.{writer.extension}"))
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

        return all_output_paths


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
