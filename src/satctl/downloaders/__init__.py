from satctl.downloaders.base import Downloader
from satctl.downloaders.http import HTTPDownloader
from satctl.registry import Registry

registry = Registry[Downloader](name="downloader")
registry.register("http", HTTPDownloader)

__all__ = [
    "Downloader",
    "HTTPDownloader",
]
