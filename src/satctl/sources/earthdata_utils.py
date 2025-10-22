"""Shared utilities for NASA EarthData sources (VIIRS, MODIS)."""

from typing import TypeVar

from pydantic import BaseModel

# UMM asset key mapping for EarthData RelatedUrls
EARTHDATA_ASSET_KEY_MAPPING = {
    "0": "http",
    "1": "s3",
    "2": "html",
    "3": "doi",
}

T = TypeVar("T", bound=BaseModel)


def parse_umm_assets(umm_result: dict, asset_class: type[T]) -> dict[str, T]:
    """Parse assets from UMM search result into asset objects.

    Args:
        umm_result: UMM format result from earthaccess search
        asset_class: Asset model class (VIIRSAsset, MODISAsset, etc.)

    Returns:
        Dictionary mapping asset keys (http, s3, html, doi) to Asset objects
    """
    return {
        EARTHDATA_ASSET_KEY_MAPPING.get(str(k), "unknown"): asset_class(
            href=url["URL"],
            type=url["Type"],
            media_type=url["MimeType"],
        )
        for k, url in enumerate(umm_result["umm"]["RelatedUrls"])
    }
