from abc import ABC, abstractmethod
from typing import Any


class Authenticator(ABC):
    """
    Base authenticator class to abstract the authentication process
    for each source.
    """

    @abstractmethod
    def authenticate(self) -> bool: ...

    @abstractmethod
    def ensure_authenticated(self, refresh: bool = False) -> bool: ...

    @property
    @abstractmethod
    def auth_headers(self) -> dict[str, str]: ...

    @property
    @abstractmethod
    def auth_session(self) -> Any: ...
