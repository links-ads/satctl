from abc import ABC, abstractmethod
from typing import Any


class Authenticator(ABC):
    """Base authenticator class for different satellite data providers.

    This abstract class defines the interface for authentication with various
    satellite data providers. Implementations handle provider-specific auth
    mechanisms (OAuth2, basic auth, API keys, etc.).
    """

    @abstractmethod
    def authenticate(self) -> bool:
        """Perform initial authentication with the provider.

        Returns:
            bool: True if authentication succeeded, False otherwise
        """
        ...

    @abstractmethod
    def ensure_authenticated(self, refresh: bool = False) -> bool:
        """Ensure valid authentication, optionally refreshing credentials.

        Args:
            refresh (bool): If True, force refresh of credentials. Defaults to False.

        Returns:
            bool: True if authenticated (or refresh succeeded), False otherwise
        """
        ...

    @property
    @abstractmethod
    def auth_headers(self) -> dict[str, str]:
        """Get authentication headers for HTTP requests.

        Returns:
            Dictionary of HTTP headers for authenticated requests
        """
        ...

    @property
    @abstractmethod
    def auth_session(self) -> Any:
        """Get an authenticated session object.

        Returns:
            Any: Provider-specific authenticated session (requests.Session, boto3 client, etc.)
        """
        ...
