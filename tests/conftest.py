"""Pytest configuration and fixtures for integration tests."""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv


@pytest.fixture(scope="session", autouse=True)
def load_env():
    """Load environment variables from .env file at test session start."""
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        pytest.skip(f".env file not found at {env_path}")


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
def earthdata_authenticator(earthdata_credentials):
    """Create an EarthDataAuthenticator instance."""
    from satctl.auth import EarthDataAuthenticator

    return EarthDataAuthenticator(
        strategy="environment",
        username=earthdata_credentials["username"],
        password=earthdata_credentials["password"],
        mode="requests_https",
    )


@pytest.fixture
def odata_authenticator(odata_credentials, copernicus_config):
    """Create an ODataAuthenticator instance."""
    from satctl.auth import ODataAuthenticator

    return ODataAuthenticator(
        token_url=copernicus_config["token_url"],
        client_id=copernicus_config["client_id"],
        username=odata_credentials["username"],
        password=odata_credentials["password"],
    )


@pytest.fixture
def s3_authenticator(odata_credentials, copernicus_config):
    """Create an S3Authenticator instance."""
    from satctl.auth import S3Authenticator

    return S3Authenticator(
        token_url=copernicus_config["token_url"],
        client_id=copernicus_config["client_id"],
        username=odata_credentials["username"],
        password=odata_credentials["password"],
        endpoint_url=copernicus_config["endpoint_url"],
        s3_credentials_url=copernicus_config["s3_credentials_url"],
        use_temp_credentials=False,
    )


@pytest.fixture
def eumetsat_authenticator(eumetsat_credentials):
    """Create an EUMETSATAuthenticator instance."""
    from satctl.auth import EUMETSATAuthenticator

    return EUMETSATAuthenticator(
        consumer_key=eumetsat_credentials["consumer_key"],
        consumer_secret=eumetsat_credentials["consumer_secret"],
    )


# Downloader Fixtures


@pytest.fixture
def temp_download_dir(tmp_path):
    """Provide a temporary directory for downloads."""
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    return download_dir


@pytest.fixture
def http_downloader(odata_authenticator):
    """Create an HTTPDownloader instance."""
    from satctl.downloaders import HTTPDownloader

    downloader = HTTPDownloader(
        authenticator=odata_authenticator,
        max_retries=3,
        chunk_size=8192,
        timeout=30,
    )
    downloader.init()
    yield downloader
    downloader.close()


@pytest.fixture
def s3_downloader(s3_authenticator):
    """Create an S3Downloader instance."""
    from satctl.downloaders import S3Downloader

    downloader = S3Downloader(
        authenticator=s3_authenticator,
        max_retries=3,
        chunk_size=8192,
        endpoint_url="https://eodata.dataspace.copernicus.eu",
    )
    downloader.init()
    yield downloader
    downloader.close()


@pytest.fixture(scope="session")
def test_urls():
    """Provide known test URLs for download testing."""
    return {
        # Small public STAC catalog endpoint (JSON, few KB)
        "stac_catalog": "https://stac.dataspace.copernicus.eu/v1/collections",
        # Note: S3 URIs require searching for actual files first
        # These would need to be discovered dynamically or hardcoded from known data
        "s3_uri": None,  # To be filled with actual S3 URI if available
    }
