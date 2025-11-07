"""Pytest configuration and fixtures for integration tests."""

import logging
import multiprocessing as mp
import os
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


@pytest.fixture(scope="session")
def earthdata_credentials():
    """Provide EarthData credentials from environment."""
    username = os.getenv("EARTHDATA_USERNAME")
    password = os.getenv("EARTHDATA_PASSWORD")

    if not username or not password:
        pytest.skip("EARTHDATA_USERNAME and EARTHDATA_PASSWORD must be set in .env")

    return {"username": username, "password": password}


@pytest.fixture(scope="session")
def odata_credentials():
    """Provide Copernicus OData credentials from environment."""
    username = os.getenv("ODATA_USERNAME")
    password = os.getenv("ODATA_PASSWORD")

    if not username or not password:
        pytest.skip("ODATA_USERNAME and ODATA_PASSWORD must be set in .env")

    return {"username": username, "password": password}


@pytest.fixture(scope="session")
def eumetsat_credentials():
    """Provide EUMETSAT credentials from environment."""
    consumer_key = os.getenv("EUMETSAT_CONSUMER_KEY")
    consumer_secret = os.getenv("EUMETSAT_CONSUMER_SECRET")

    if not consumer_key or not consumer_secret:
        pytest.skip("EUMETSAT_CONSUMER_KEY and EUMETSAT_CONSUMER_SECRET must be set in .env")

    return {"consumer_key": consumer_key, "consumer_secret": consumer_secret}


@pytest.fixture(scope="session")
def copernicus_config():
    """Provide Copernicus OAuth configuration constants."""
    return {
        "token_url": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        "client_id": "cdse-public",
        "s3_credentials_url": "https://eodata.dataspace.copernicus.eu/s3-credentials",
        "endpoint_url": "https://eodata.dataspace.copernicus.eu",
    }


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
