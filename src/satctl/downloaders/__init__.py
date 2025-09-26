from satctl.downloaders.base import Downloader
from satctl.downloaders.earthdata import EarthDataDownloader
from satctl.downloaders.http import HTTPDownloader
from satctl.registry import Registry

registry = Registry[Downloader]()
registry.register("http", HTTPDownloader)
registry.register("earthaccess", EarthDataDownloader)

__all__ = ["Downloader", "HTTPDownloader", "EarthDataDownloader"]
