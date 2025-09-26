import logging
import os
from typing import Any

import earthaccess

from satctl.auth.base import Authenticator

log = logging.getLogger(__name__)


class EarthDataAuthenticator(Authenticator):
    """Handles authentication for NASA Earthdata using earthaccess library."""

    def __init__(
        self,
        strategy: str = "environment",
        username: str | None = None,
        password: str | None = None,
    ):
        """
        Initialize EarthData authenticator.

        Args:
            strategy: Authentication strategy ('environment', 'interactive', 'netrc')
            username: Optional username (if not using environment strategy)
            password: Optional password (if not using environment strategy)
        """
        self.strategy = strategy
        self.username = username
        self.password = password
        self._authenticated = False
        self._auth_session: Any = None

        # Validate environment variables if using environment strategy
        if strategy == "environment":
            username_env = os.getenv("EARTHDATA_USERNAME")
            password_env = os.getenv("EARTHDATA_PASSWORD")
            if not username_env or not password_env:
                raise ValueError(
                    "EARTHDATA_USERNAME and EARTHDATA_PASSWORD environment variables must be set "
                    "when using 'environment' strategy"
                )

    def authenticate(self) -> bool:
        """Authenticate with NASA Earthdata using earthaccess."""
        try:
            if self.strategy == "environment":
                log.debug("Authenticating with earthaccess using environment strategy")
                self._auth_session = earthaccess.login(strategy="environment")
            elif self.strategy == "interactive":
                log.debug("Authenticating with earthaccess using interactive strategy")
                self._auth_session = earthaccess.login()
            elif self.strategy == "netrc":
                log.debug("Authenticating with earthaccess using netrc strategy")
                self._auth_session = earthaccess.login(strategy="netrc")
            else:
                raise ValueError(f"Unsupported authentication strategy: {self.strategy}")

            self._authenticated = True
            log.info("Successfully authenticated with NASA Earthdata")
            return True

        except Exception as e:
            log.error(f"Authentication failed: {e}")
            self._authenticated = False
            self._auth_session = None
            return False

    def ensure_authenticated(self, refresh: bool = False) -> bool:
        """Ensure we have valid authentication with NASA Earthdata."""
        if not self._authenticated or refresh:
            return self.authenticate()
        return True

    @property
    def auth_headers(self) -> dict[str, str]:
        """
        Get authentication headers.

        Note: earthaccess handles authentication internally,
        so we don't need to provide explicit headers.
        This returns empty dict since earthaccess manages session cookies.
        """
        if not self.ensure_authenticated():
            raise RuntimeError("Failed to authenticate with NASA Earthdata")

        # earthaccess handles authentication internally via session cookies
        # so we don't need to provide explicit authorization headers
        return {}

    @property
    def auth_session(self) -> Any:
        """Get the earthaccess authentication session."""
        if not self.ensure_authenticated():
            raise RuntimeError("Failed to authenticate with NASA Earthdata")
        return self._auth_session
