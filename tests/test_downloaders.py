"""Integration tests for downloader classes.

These tests verify that downloaders can successfully download files
using real authenticated connections.
"""


import pytest


@pytest.mark.integration
@pytest.mark.requires_credentials
@pytest.mark.slow
class TestHTTPDownloader:
    """Integration tests for HTTPDownloader."""

    def test_init(self, http_downloader):
        """Test that HTTPDownloader initializes correctly."""
        assert http_downloader.session is not None, "Session should be initialized"
        assert hasattr(http_downloader.session, "get"), "Session should have get method"

    def test_download_success(self, http_downloader, temp_download_dir, test_urls):
        """Test successful file download via HTTP."""
        url = test_urls["stac_catalog"]
        destination = temp_download_dir / "catalog.json"
        item_id = "test_catalog"

        # Download the file
        result = http_downloader.download(uri=url, destination=destination, item_id=item_id)

        # Verify download succeeded
        assert result is True, "download() should return True on success"

        # Verify file exists
        assert destination.exists(), f"Downloaded file should exist at {destination}"

        # Verify file has content
        file_size = destination.stat().st_size
        assert file_size > 0, f"Downloaded file should have non-zero size, got {file_size} bytes"

    def test_close(self, http_downloader):
        """Test that close() method properly cleans up."""
        # Downloader is already initialized via fixture
        assert http_downloader.session is not None

        # Close should not raise an exception
        http_downloader.close()

        # Session should be closed (can't reliably check this without accessing internals,
        # but we verify no exception was raised)


@pytest.mark.integration
@pytest.mark.requires_credentials
@pytest.mark.slow
class TestS3Downloader:
    """Integration tests for S3Downloader."""

    def test_init(self, s3_downloader):
        """Test that S3Downloader initializes correctly."""
        assert s3_downloader.s3_client is not None, "S3 client should be initialized"
        # Verify it's a boto3 client
        assert hasattr(s3_downloader.s3_client, "download_file"), "Should have download_file method"
        assert hasattr(s3_downloader.s3_client, "get_object"), "Should have get_object method"

    def test_parse_s3_uri_valid(self, s3_downloader):
        """Test parsing of valid S3 URIs."""
        uri = "s3://test-bucket/path/to/file.txt"
        bucket, key = s3_downloader._parse_s3_uri(uri)

        assert bucket == "test-bucket", f"Expected bucket 'test-bucket', got '{bucket}'"
        assert key == "path/to/file.txt", f"Expected key 'path/to/file.txt', got '{key}'"

    def test_parse_s3_uri_invalid(self, s3_downloader):
        """Test parsing of invalid S3 URIs raises ValueError."""
        with pytest.raises(ValueError, match="Invalid S3 URI format"):
            s3_downloader._parse_s3_uri("http://not-an-s3-uri.com/file.txt")

        with pytest.raises(ValueError, match="Invalid S3 URI format"):
            s3_downloader._parse_s3_uri("s3://bucket-without-key")

    @pytest.mark.skip(reason="Requires a known S3 URI from Copernicus - configure test_urls fixture")
    def test_download_success(self, s3_downloader, temp_download_dir, test_urls):
        """Test successful file download from S3.

        Note: This test is skipped by default because it requires a valid S3 URI.
        To enable, add a valid S3 URI to the test_urls fixture in conftest.py.
        Example: "s3://eodata/Sentinel-2/MSI/L1C/2024/01/01/..."
        """
        s3_uri = test_urls["s3_uri"]
        if not s3_uri:
            pytest.skip("No S3 URI configured in test_urls fixture")

        destination = temp_download_dir / "s3_test_file"
        item_id = "test_s3_item"

        # Download the file
        result = s3_downloader.download(uri=s3_uri, destination=destination, item_id=item_id)

        # Verify download succeeded
        assert result is True, "download() should return True on success"

        # Verify file exists
        assert destination.exists(), f"Downloaded file should exist at {destination}"

        # Verify file has content
        file_size = destination.stat().st_size
        assert file_size > 0, f"Downloaded file should have non-zero size, got {file_size} bytes"

    def test_close(self, s3_downloader):
        """Test that close() method properly cleans up."""
        # Downloader is already initialized via fixture
        assert s3_downloader.s3_client is not None

        # Close should not raise an exception
        s3_downloader.close()

        # Client reference should be cleared
        assert s3_downloader.s3_client is None, "S3 client should be None after close()"


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestDownloaderLifecycle:
    """Test complete lifecycle of downloaders (init → download → close)."""

    def test_http_lifecycle(self, odata_authenticator, temp_download_dir, test_urls):
        """Test full lifecycle of HTTPDownloader."""
        from satctl.downloaders import HTTPDownloader

        # Create downloader
        downloader = HTTPDownloader(authenticator=odata_authenticator, max_retries=2, timeout=30)

        # Init
        downloader.init()
        assert downloader.session is not None

        # Download
        url = test_urls["stac_catalog"]
        destination = temp_download_dir / "lifecycle_test.json"
        result = downloader.download(uri=url, destination=destination, item_id="lifecycle_test")
        assert result is True
        assert destination.exists()

        # Close
        downloader.close()

    def test_s3_lifecycle(self, s3_authenticator):
        """Test full lifecycle of S3Downloader."""
        from satctl.downloaders import S3Downloader

        # Create downloader
        downloader = S3Downloader(
            authenticator=s3_authenticator,
            max_retries=2,
            endpoint_url="https://eodata.dataspace.copernicus.eu",
        )

        # Init
        downloader.init()
        assert downloader.s3_client is not None

        # We don't actually download because we don't have a guaranteed S3 URI
        # but we verify the initialization worked

        # Close
        downloader.close()
        assert downloader.s3_client is None
