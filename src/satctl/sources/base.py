from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from satctl.downloaders import Downloader
from satctl.model import ConversionParams, SearchParams
from satctl.progress import ProgressReporter
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
    def search(self, params: SearchParams) -> list[Any]: ...

    @abstractmethod
    def download(
        self,
        items: Any,
        output_dir: Path,
        progress: ProgressReporter,
    ) -> tuple[list, list]: ...

    @abstractmethod
    def convert(
        self,
        params: ConversionParams,
        source: Path,
        output_dir: Path,
        writer: Writer,
        progress: ProgressReporter,
        force: bool = True,
    ) -> tuple[list, list]: ...

    @abstractmethod
    def validate(self, item: Any) -> None:
        raise NotImplementedError()
