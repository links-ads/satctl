from abc import ABC, abstractmethod
from pathlib import Path

from pyresample.geometry import AreaDefinition
from satpy.scene import Scene

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, Granule, SearchParams
from satctl.writers import Writer


class DataSource(ABC):
    """Abstract base class for all satellite data sources."""

    def __init__(
        self,
        *collections,
        composite: str,
        downloader: Downloader,
    ):
        self.collections = list(collections)
        self.downloader = downloader
        self.composite = composite

    @abstractmethod
    def search(self, params: SearchParams) -> list[Granule]: ...

    @abstractmethod
    def get(self, item_id: str) -> Granule: ...

    @abstractmethod
    def validate(self, item: Granule) -> None:
        raise NotImplementedError()

    @abstractmethod
    def download(
        self,
        items: Granule | list[Granule],
        output_dir: Path,
    ) -> tuple[list, list]: ...

    @abstractmethod
    def convert(
        self,
        params: ConversionParams,
        source: Path,
        output_dir: Path,
        writer: Writer,
        force: bool = True,
    ) -> tuple[list, list]: ...

    @abstractmethod
    def load_scene(
        self,
        source: Granule | Path | str,
        composites: list[str] | None = None,
        area_definition: AreaDefinition | None = None,
    ) -> Scene: ...
