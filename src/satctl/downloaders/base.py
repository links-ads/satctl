from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from satctl.auth import Authenticator


class Downloader(ABC):
    """Abstract base class for downloaders."""

    def __init__(self, authenticator: Authenticator) -> None:
        """Initialize downloader.

        Args:
            authenticator (Authenticator): Authenticator instance for credential management
        """
        super().__init__()
        self.auth = authenticator

    @abstractmethod
    def init(self, **kwargs: Any) -> None:
        """Initialize downloader with optional configuration.

        Args:
            **kwargs (Any): Additional keyword arguments for initialization
        """
        ...

    @abstractmethod
    def download(
        self,
        uri: str,
        destination: Path,
        item_id: str,
    ) -> bool:
        """Download a file from URI to destination.

        Args:
            uri (str): URI to download from
            destination (Path): Local file path to save to
            item_id (str): Item identifier for progress tracking

        Returns:
            bool: True if download succeeded, False otherwise
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Close downloader and release resources."""
        ...
