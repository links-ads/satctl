from abc import ABC, abstractmethod


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
