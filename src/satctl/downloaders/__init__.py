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
from satctl.registry import Builder, Registry

registry = Registry[Downloader](name="downloader")
registry.register("http", HTTPDownloader)
registry.register("s3", S3Downloader)


class DownloadBuilder(Builder[Downloader]):
    """Authenticator factory type definition.
    Provides the means to defer the creation of an authenticator when we need one.
    """


def configure_downloader(dwl_name: str, **overrides: Any) -> DownloadBuilder:
    """Create a downloader factory from config for a given source.

    Args:
        dwl_name: Name of the downloader instance
        **overrides: Override download config parameters

    Returns:
        Factory function that creates the configured downloader

    Example:
        >>> factory = configure_downloader("http")
        >>> auth = factory()  # Creates HTTPDownloader from config
    """
    if not registry.is_registered(dwl_name):
        raise ValueError(f"Unknown authenticator: {dwl_name}")
    config = get_settings()
    auth_config = config.download.get(dwl_name, {}).copy()
    auth_config.update(overrides)
    return DownloadBuilder(name=dwl_name, registry=registry, **auth_config)


__all__ = [
    "configure_downloader",
    "DownloadBuilder",
    "Downloader",
    "HTTPDownloader",
    "S3Downloader",
]
