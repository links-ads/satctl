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
