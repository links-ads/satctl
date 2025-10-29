"""Downloader implementations for different protocols.

This package provides downloader implementations for retrieving satellite data:
- HTTPDownloader: Standard HTTP/HTTPS downloads with retries and progress tracking
- S3Downloader: S3-compatible downloads (AWS, MinIO, etc.)

All downloaders implement the Downloader interface and support authentication,
retries, and progress reporting.
"""

from typing import Any

from satctl.config import get_settings
from satctl.downloaders.base import Downloader
from satctl.downloaders.http import HTTPDownloader
from satctl.downloaders.s3 import S3Downloader
from satctl.registry import Registry

registry = Registry[Downloader](name="downloader")
registry.register("http", HTTPDownloader)
registry.register("s3", S3Downloader)


def create_downloader(
    source_name: str,
    authenticator=None,
    downloader_name: str | None = None,
    **kwargs: dict[str, Any],
):
    """Create a downloader instance for a given source.

    Args:
        source_name (str): Name of the data source (to get downloader config from)
        authenticator: Authenticator instance to use
        downloader_name (str | None): Explicit downloader name. If None, inferred from source config.
        kwargs (dict[str, Any]): Additional downloader configuration

    Returns:
        Downloader instance configured for the source
    """
    from satctl.downloaders import registry as dwl_registry

    config = get_settings()
    source_params = config.sources.get(source_name, {})

    # Get downloader name from explicit param, source config, or fallback
    if downloader_name is None:
        downloader_name = source_params.get("downloader")

    if downloader_name is None:
        raise ValueError(
            f"No downloader configured for source '{source_name}'. "
            "Specify downloader in config or pass downloader_name parameter."
        )

    # Get downloader config and merge with kwargs
    dwl_config = config.download.get(downloader_name, {}).copy()
    dwl_config.update(kwargs)

    return dwl_registry.create(downloader_name, authenticator=authenticator, **dwl_config)


__all__ = [
    "Downloader",
    "HTTPDownloader",
    "S3Downloader",
]
