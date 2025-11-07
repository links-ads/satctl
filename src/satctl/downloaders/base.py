from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from satctl.auth import Authenticator


class Downloader(ABC):
    """Abstract base class for downloaders."""

    @abstractmethod
    def init(self, authenticator: Authenticator, **kwargs: Any) -> None:
        """Initialize downloader with an authenticator, and optional configuration.

        Args:
            authenticator (Authenticator): auth object required for access tokens or sessions
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
