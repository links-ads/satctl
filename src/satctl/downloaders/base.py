from abc import ABC, abstractmethod
from pathlib import Path

from satctl.auth import Authenticator
from satctl.progress import ProgressReporter


class Downloader(ABC):
    def __init__(self, authenticator: Authenticator) -> None:
        super().__init__()
        self.auth = authenticator

    @abstractmethod
    def init(self) -> None: ...

    @abstractmethod
    def download(
        self,
        uri: str,  # TODO is this too restrictive?
        destination: Path,
        progress: ProgressReporter | None = None,
        task_id: str | None = None,
    ) -> bool: ...

    @abstractmethod
    def close(self) -> None: ...
