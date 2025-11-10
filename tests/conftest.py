"""Pytest configuration and fixtures for integration tests."""

import logging
import multiprocessing as mp
from datetime import datetime
from pathlib import Path

import pytest
from dotenv import load_dotenv

log = logging.getLogger(__name__)

if mp.get_start_method(allow_none=True) != "spawn":
    mp.set_start_method("spawn", force=True)
# Load .env file BEFORE any imports that might use satpy
# This must happen at module import time, not in a fixture, because satpy
# reads SATPY_CONFIG_PATH when it's first imported (during test collection)
load_dotenv()


def pytest_addoption(parser):
    parser.addoption("--slow", action="store_true", default=False, help="Run slow tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--slow"):
        # --slow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="Marked as slow, skipping")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture
def temp_download_dir(tmp_path):
    """Provide a temporary directory for downloads."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    return download_dir


@pytest.fixture
def test_search_params():
    """Provide SearchParams for integration tests.

    Uses the EMSR760 GeoJSON file located in the data/ directory at the project root.
    Configured with a date range that has known satellite coverage for the test area.

    Returns:
        SearchParams: Search parameters configured for testing
    """
    from satctl.model import SearchParams

    # Use absolute path relative to project root
    project_root = Path(__file__).parent
    geojson_path = project_root / "assets" / "area.json"

    return SearchParams.from_file(
        path=geojson_path,
        start=datetime.strptime("2024-09-01", "%Y-%m-%d"),
        end=datetime.strptime("2024-09-04", "%Y-%m-%d"),
    )


@pytest.fixture
def test_mtg_search_params():
    """Provide SearchParams for integration tests.

    Uses the EMSR760 GeoJSON file located in the data/ directory at the project root.
    Configured with a date range that has known satellite coverage for the test area.

    Returns:
        SearchParams: Search parameters configured for testing
    """
    from satctl.model import SearchParams

    # Use absolute path relative to project root
    project_root = Path(__file__).parent
    geojson_path = project_root / "assets" / "area.json"

    return SearchParams.from_file(
        path=geojson_path,
        start=datetime.strptime("2024-09-25", "%Y-%m-%d"),
        end=datetime.strptime("2024-09-26", "%Y-%m-%d"),
    )


@pytest.fixture
def test_conversion_params():
    """Provide ConversionParams for integration tests.

    Uses the EMSR760 GeoJSON file located in the data/ directory at the project root.
    Configured to output in WGS84 (EPSG:4326) coordinate reference system.

    Returns:
        ConversionParams: Conversion parameters configured for testing
    """
    from satctl.model import ConversionParams

    # Use absolute path relative to project root
    test_root = Path(__file__).parent
    geojson_path = test_root / "assets" / "area.json"
    return ConversionParams.from_file(
        path=geojson_path,
        target_crs="EPSG:4326",
    )


@pytest.fixture
def geotiff_writer():
    """Provide a configured GeoTIFFWriter instance for tests.

    Configured with LZW compression and tiling enabled for efficient storage.

    Returns:
        GeoTIFFWriter: Writer instance configured for test outputs
    """
    from satctl.writers import GeoTIFFWriter

    return GeoTIFFWriter(compress="lzw", tiled=True)
