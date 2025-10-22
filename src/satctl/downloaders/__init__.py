"""Downloader implementations for different protocols.

This package provides downloader implementations for retrieving satellite data:
- HTTPDownloader: Standard HTTP/HTTPS downloads with retries and progress tracking
- S3Downloader: S3-compatible downloads (AWS, MinIO, etc.)

All downloaders implement the Downloader interface and support authentication,
retries, and progress reporting.
"""

from satctl.downloaders.base import Downloader
from satctl.downloaders.http import HTTPDownloader
from satctl.downloaders.s3 import S3Downloader
from satctl.registry import Registry

registry = Registry[Downloader](name="downloader")
registry.register("http", HTTPDownloader)
registry.register("s3", S3Downloader)

__all__ = [
    "Downloader",
    "HTTPDownloader",
    "S3Downloader",
]
