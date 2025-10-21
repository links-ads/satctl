"""Unit tests for Sentinel-2 sources."""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from satctl.model import Granule, ProductInfo
from satctl.sources.sentinel2 import S2Asset, Sentinel2L1CSource, Sentinel2L2ASource


class TestS2Asset:
    """Test the S2Asset model."""

    def test_create_asset(self):
        """Test creating a basic S2 asset."""
        asset = S2Asset(href="https://example.com/data.zip", media_type="application/zip")
        assert asset.href == "https://example.com/data.zip"
        assert asset.media_type == "application/zip"

    def test_create_asset_no_media_type(self):
        """Test creating an asset without media type."""
        asset = S2Asset(href="https://example.com/data.jp2", media_type=None)
        assert asset.href == "https://example.com/data.jp2"
        assert asset.media_type is None


class TestSentinel2L2ASource:
    """Test Sentinel-2 L2A source."""

    @pytest.fixture
    def mock_downloader(self):
        """Create a mock downloader."""
        downloader = Mock()
        downloader.download = Mock(return_value=True)
        downloader.init = Mock()
        return downloader

    @pytest.fixture
    def l2a_source(self, mock_downloader):
        """Create a Sentinel-2 L2A source instance."""
        return Sentinel2L2ASource(
            downloader=mock_downloader,
            stac_url="https://example.com/stac",
            composite="true_color",
            search_limit=10,
        )

    def test_initialization(self, l2a_source):
        """Test that L2A source initializes correctly."""
        assert l2a_source.collections == ["sentinel-2-l2a"]
        assert l2a_source.reader == "msi_safe_l2a"
        assert l2a_source.default_composite == "true_color"
        assert l2a_source.stac_url == "https://example.com/stac"
        assert l2a_source.search_limit == 10

    def test_parse_item_name_valid(self, l2a_source):
        """Test parsing a valid L2A product name."""
        name = "S2A_MSIL2A_20231015T103021"
        info = l2a_source._parse_item_name(name)

        assert isinstance(info, ProductInfo)
        assert info.instrument == "msi"
        assert info.level == "2A"
        assert info.product_type == "L2A"
        assert info.acquisition_time == datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc)

    def test_parse_item_name_sentinel_2b(self, l2a_source):
        """Test parsing a Sentinel-2B product name."""
        name = "S2B_MSIL2A_20231220T153045"
        info = l2a_source._parse_item_name(name)

        assert info.instrument == "msi"
        assert info.level == "2A"
        assert info.acquisition_time == datetime(2023, 12, 20, 15, 30, 45, tzinfo=timezone.utc)

    def test_parse_item_name_invalid(self, l2a_source):
        """Test that invalid product names raise ValueError."""
        with pytest.raises(ValueError, match="Invalid Sentinel-2 L2A filename format"):
            l2a_source._parse_item_name("INVALID_NAME")

    def test_parse_item_name_wrong_level(self, l2a_source):
        """Test that L1C names don't parse as L2A."""
        with pytest.raises(ValueError, match="Invalid Sentinel-2 L2A filename format"):
            l2a_source._parse_item_name("S2A_MSIL1C_20231015T103021")


class TestSentinel2L1CSource:
    """Test Sentinel-2 L1C source."""

    @pytest.fixture
    def mock_downloader(self):
        """Create a mock downloader."""
        downloader = Mock()
        downloader.download = Mock(return_value=True)
        downloader.init = Mock()
        return downloader

    @pytest.fixture
    def l1c_source(self, mock_downloader):
        """Create a Sentinel-2 L1C source instance."""
        return Sentinel2L1CSource(
            downloader=mock_downloader,
            stac_url="https://example.com/stac",
            composite="true_color",
            search_limit=10,
        )

    def test_initialization(self, l1c_source):
        """Test that L1C source initializes correctly."""
        assert l1c_source.collections == ["sentinel-2-l1c"]
        assert l1c_source.reader == "msi_safe"
        assert l1c_source.default_composite == "true_color"
        assert l1c_source.stac_url == "https://example.com/stac"
        assert l1c_source.search_limit == 10

    def test_parse_item_name_valid(self, l1c_source):
        """Test parsing a valid L1C product name."""
        name = "S2A_MSIL1C_20231015T103021"
        info = l1c_source._parse_item_name(name)

        assert isinstance(info, ProductInfo)
        assert info.instrument == "msi"
        assert info.level == "1C"
        assert info.product_type == "L1C"
        assert info.acquisition_time == datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc)

    def test_parse_item_name_sentinel_2b(self, l1c_source):
        """Test parsing a Sentinel-2B product name."""
        name = "S2B_MSIL1C_20240101T000000"
        info = l1c_source._parse_item_name(name)

        assert info.instrument == "msi"
        assert info.level == "1C"
        assert info.acquisition_time == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    def test_parse_item_name_invalid(self, l1c_source):
        """Test that invalid product names raise ValueError."""
        with pytest.raises(ValueError, match="Invalid Sentinel-2 L1C filename format"):
            l1c_source._parse_item_name("INVALID_NAME")

    def test_parse_item_name_wrong_level(self, l1c_source):
        """Test that L2A names don't parse as L1C."""
        with pytest.raises(ValueError, match="Invalid Sentinel-2 L1C filename format"):
            l1c_source._parse_item_name("S2A_MSIL2A_20231015T103021")


class TestSentinel2SourceValidation:
    """Test validation methods of Sentinel2Source."""

    @pytest.fixture
    def mock_downloader(self):
        """Create a mock downloader."""
        return Mock()

    @pytest.fixture
    def l2a_source(self, mock_downloader):
        """Create a Sentinel-2 L2A source instance."""
        return Sentinel2L2ASource(
            downloader=mock_downloader,
            stac_url="https://example.com/stac",
        )

    def test_validate_valid_item(self, l2a_source):
        """Test validation of a valid granule."""
        granule = Granule(
            granule_id="S2A_MSIL2A_20231015T103021",
            source="sentinel-2-l2a",
            assets={
                "Product": S2Asset(href="https://example.com/data.zip", media_type="application/zip"),
                "thumbnail": S2Asset(href="https://example.com/thumb.jpg", media_type="image/jpeg"),
                "metadata": S2Asset(href="https://example.com/meta.xml", media_type="application/xml"),
            },
            info=ProductInfo(
                instrument="msi",
                level="2A",
                product_type="L2A",
                acquisition_time=datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc),
            ),
        )
        # Should not raise
        l2a_source.validate(granule)

    def test_validate_invalid_media_type(self, l2a_source):
        """Test validation fails for invalid media type."""
        granule = Granule(
            granule_id="S2A_MSIL2A_20231015T103021",
            source="sentinel-2-l2a",
            assets={
                "bad_asset": S2Asset(href="https://example.com/data.bad", media_type="application/bad"),
            },
            info=ProductInfo(
                instrument="msi",
                level="2A",
                product_type="L2A",
                acquisition_time=datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc),
            ),
        )
        with pytest.raises(AssertionError):
            l2a_source.validate(granule)

    def test_validate_zip_not_named_product(self, l2a_source):
        """Test validation fails when zip asset is not named 'product'."""
        granule = Granule(
            granule_id="S2A_MSIL2A_20231015T103021",
            source="sentinel-2-l2a",
            assets={
                "wrong_name": S2Asset(href="https://example.com/data.zip", media_type="application/zip"),
            },
            info=ProductInfo(
                instrument="msi",
                level="2A",
                product_type="L2A",
                acquisition_time=datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc),
            ),
        )
        with pytest.raises(AssertionError):
            l2a_source.validate(granule)

    def test_get_files_no_local_path(self, l2a_source):
        """Test get_files raises error when local_path is None."""
        granule = Granule(
            granule_id="S2A_MSIL2A_20231015T103021",
            source="sentinel-2-l2a",
            assets={},
            info=ProductInfo(
                instrument="msi",
                level="2A",
                product_type="L2A",
                acquisition_time=datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc),
            ),
            local_path=None,
        )
        with pytest.raises(ValueError, match="Local path is missing"):
            l2a_source.get_files(granule)

    def test_get_by_id_not_implemented(self, l2a_source):
        """Test that get_by_id raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            l2a_source.get_by_id("some_id")


class TestSentinel2SourceSearch:
    """Test search functionality of Sentinel2Source."""

    @pytest.fixture
    def mock_downloader(self):
        """Create a mock downloader."""
        downloader = Mock()
        downloader.download = Mock(return_value=True)
        downloader.init = Mock()
        return downloader

    @pytest.fixture
    def l2a_source(self, mock_downloader):
        """Create a Sentinel-2 L2A source instance with real STAC URL."""
        return Sentinel2L2ASource(
            downloader=mock_downloader,
            stac_url="https://stac.dataspace.copernicus.eu/v1",
            composite="true_color",
            search_limit=10,
        )

    @pytest.fixture
    def l1c_source(self, mock_downloader):
        """Create a Sentinel-2 L1C source instance with real STAC URL."""
        return Sentinel2L1CSource(
            downloader=mock_downloader,
            stac_url="https://stac.dataspace.copernicus.eu/v1",
            composite="true_color",
            search_limit=10,
        )

    def test_search_l2a_returns_granules(self, l2a_source):
        """Test that L2A search returns list of Granule objects using real STAC API."""
        from satctl.model import SearchParams
        from geojson_pydantic import Feature
        from shapely.geometry import Polygon

        # Use a small area in Europe (Rome, Italy) for faster search
        # Bounding box: roughly 12.4-12.6 lon, 41.8-42.0 lat
        polygon_geom = Polygon([(12.4, 41.8), (12.6, 41.8), (12.6, 42.0), (12.4, 42.0), (12.4, 41.8)])

        search_params = SearchParams(
            start=datetime(2024, 8, 1, tzinfo=timezone.utc),
            end=datetime(2024, 8, 10, tzinfo=timezone.utc),
            area=Feature(type="Feature", geometry=polygon_geom.__geo_interface__, properties={}),
        )

        results = l2a_source.search(search_params)

        # Verify we got results
        assert len(results) > 0
        assert isinstance(results[0], Granule)
        # Verify the granule has the expected properties
        assert results[0].granule_id.startswith("S2")
        assert "MSIL2A" in results[0].granule_id
        assert results[0].source == "sentinel-2-l2a"
        assert len(results[0].assets) > 0
        assert results[0].info is not None
        assert results[0].info.level == "2A"

    def test_search_l1c_returns_granules(self, l1c_source):
        """Test that L1C search returns list of Granule objects using real STAC API."""
        from satctl.model import SearchParams
        from geojson_pydantic import Feature
        from shapely.geometry import Polygon

        # Use a small area in Europe (Rome, Italy) for faster search
        polygon_geom = Polygon([(12.4, 41.8), (12.6, 41.8), (12.6, 42.0), (12.4, 42.0), (12.4, 41.8)])

        search_params = SearchParams(
            start=datetime(2024, 8, 1, tzinfo=timezone.utc),
            end=datetime(2024, 8, 10, tzinfo=timezone.utc),
            area=Feature(type="Feature", geometry=polygon_geom.__geo_interface__, properties={}),
        )

        results = l1c_source.search(search_params)

        # Verify we got results
        assert len(results) > 0
        assert isinstance(results[0], Granule)
        # Verify the granule has the expected properties
        assert results[0].granule_id.startswith("S2")
        assert "MSIL1C" in results[0].granule_id
        assert results[0].source == "sentinel-2-l1c"
        assert len(results[0].assets) > 0
        assert results[0].info is not None
        assert results[0].info.level == "1C"


class TestSentinel2SourceDownload:
    """Test download functionality of Sentinel2Source."""

    @pytest.fixture
    def mock_downloader(self):
        """Create a mock downloader."""
        downloader = Mock()
        downloader.download = Mock(return_value=True)
        downloader.init = Mock()
        return downloader

    @pytest.fixture
    def l2a_source(self, mock_downloader):
        """Create a Sentinel-2 L2A source instance."""
        return Sentinel2L2ASource(
            downloader=mock_downloader,
            stac_url="https://example.com/stac",
        )

    @pytest.fixture
    def sample_granule(self):
        """Create a sample granule for testing."""
        return Granule(
            granule_id="S2A_MSIL2A_20231015T103021",
            source="sentinel-2-l2a",
            assets={
                "Product": S2Asset(href="https://example.com/data.zip", media_type="application/zip"),
            },
            info=ProductInfo(
                instrument="msi",
                level="2A",
                product_type="L2A",
                acquisition_time=datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc),
            ),
        )

    def test_download_item_failure(self, l2a_source, sample_granule, tmp_path, mock_downloader):
        """Test failed download of a single item."""
        mock_downloader.download.return_value = False

        result = l2a_source.download_item(sample_granule, tmp_path)

        assert result is False
        assert sample_granule.local_path is None


class TestProductInfoDateParsing:
    """Test date parsing in various formats."""

    @pytest.fixture
    def mock_downloader(self):
        """Create a mock downloader."""
        return Mock()

    @pytest.fixture
    def l2a_source(self, mock_downloader):
        """Create a Sentinel-2 L2A source instance."""
        return Sentinel2L2ASource(
            downloader=mock_downloader,
            stac_url="https://example.com/stac",
        )

    def test_parse_various_dates(self, l2a_source):
        """Test parsing various date formats."""
        test_cases = [
            ("S2A_MSIL2A_20231015T103021", datetime(2023, 10, 15, 10, 30, 21, tzinfo=timezone.utc)),
            ("S2B_MSIL2A_20240101T000000", datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
            ("S2A_MSIL2A_20231231T235959", datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc)),
        ]

        for name, expected_date in test_cases:
            info = l2a_source._parse_item_name(name)
            assert info.acquisition_time == expected_date, f"Failed for {name}"
