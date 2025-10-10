from abc import ABC, abstractmethod
from pathlib import Path

from satctl.auth import Authenticator


class Downloader(ABC):
    def __init__(self, authenticator: Authenticator) -> None:
        super().__init__()
        self.auth = authenticator

    @abstractmethod
    def init(self) -> None: ...

    @abstractmethod
    def download(
        self,
        uri: str,
        destination: Path,
        task_id: str,
    ) -> bool: ...

    @abstractmethod
    def close(self) -> None: ...
