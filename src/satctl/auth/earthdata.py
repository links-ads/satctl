import logging
import os
from typing import Any, Literal

import earthaccess

from satctl.auth.base import Authenticator

log = logging.getLogger(__name__)


class EarthDataAuthenticator(Authenticator):
    """Handles authentication for NASA Earthdata using earthaccess library."""

    ENV_USER_NAME = "EARTHDATA_USERNAME"
    ENV_PASS_NAME = "EARTHDATA_PASSWORD"

    def __init__(
        self,
        strategy: Literal["environment", "interactive", "netrc"] = "environment",
        username: str | None = None,
        password: str | None = None,
        mode: Literal["requests_https", "fsspec_https", "s3fs"] = "requests_https",
    ):
        """Initialize EarthData authenticator.

        Args:
            strategy (Literal["environment", "interactive", "netrc"]): Authentication strategy. Defaults to "environment".
            username (str | None): Username to inject. Defaults to None.
            password (str | None): Password to inject. Defaults to None.
            mode (Literal["requests_https", "fsspec_https", "s3fs"]): Session mode. Defaults to "requests_https".

        Raises:
            ValueError: If credentials are missing when using environment strategy
        """
        self.strategy = strategy
        self.mode = mode
        self._auth = None
        self.username = None
        self.password = None
        # ensure credentials are provided with environment strategy
        if strategy == "environment":
            self.username = username or os.getenv(self.ENV_USER_NAME)
            self.password = password or os.getenv(self.ENV_PASS_NAME)

            if not self.username or not self.password:
                raise ValueError(
                    f"Invalid configuration: {self.ENV_USER_NAME} and {self.ENV_PASS_NAME} "
                    "environment variables are required when using 'environment' strategy"
                )

            os.environ[self.ENV_USER_NAME] = self.username
            os.environ[self.ENV_PASS_NAME] = self.password

    def authenticate(self) -> bool:
        """Perform authentication with NASA Earthdata.

        Returns:
            bool: True if authentication succeeded, False otherwise
        """
        log.debug("Authenticating to earthaccess using strategy: %s", self.strategy)
        self._auth = earthaccess.login(strategy=self.strategy)
        return self._auth.authenticated

    def ensure_authenticated(self, refresh: bool = False) -> bool:
        """Ensure we have valid authentication with NASA Earthdata.

        Args:
            refresh (bool): If True, force re-authentication. Defaults to False.

        Returns:
            bool: True if authenticated, False otherwise
        """
        if not self._auth or not self._auth.authenticated or refresh:
            return self.authenticate()
        return self._auth.authenticated

    @property
    def auth_headers(self) -> dict[str, str]:
        """Get authentication headers for HTTP requests.

        Note: earthaccess handles authentication internally,
        so this returns an empty dict.

        Returns:
            dict[str, str]: Empty dictionary (earthaccess manages auth internally)
        """
        self.ensure_authenticated()
        return {}

    @property
    def auth_session(self) -> Any:
        """Get authenticated session from earthaccess.

        Returns:
            Any: Session object based on configured mode (requests, fsspec, or s3fs)

        Raises:
            ValueError: If mode is not supported by earthaccess
        """
        self.ensure_authenticated()
        session_name = f"get_{self.mode}_session"
        if not hasattr(earthaccess, session_name):
            raise ValueError(f"Invalid mode: '{self.mode}' (earthaccess does not support this mode)")
        return getattr(earthaccess, session_name)()
