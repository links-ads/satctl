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
        """Authenticates the user on EarthData using earthaccess.

        Args:
            strategy (["environment", "interactive", "netrc"], optional): Authentication strategy. Defaults to "environment".
            username (str | None, optional): Username to inject. Defaults to None.
            password (str | None, optional): Password to inject. Defaults to None.
            mode (["https", "fsspec", "s3"], optional): session mode to be returned. Defaults to https.

        Raises:
            ValueError: validation might fail.
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
        log.debug("Authenticating to earthaccess using strategy: %s", self.strategy)
        self._auth = earthaccess.login(strategy=self.strategy)
        return self._auth.authenticated

    def ensure_authenticated(self, refresh: bool = False) -> bool:
        """Ensure we have valid authentication with NASA Earthdata."""
        if not self._auth or not self._auth.authenticated or refresh:
            return self.authenticate()
        return self._auth.authenticated

    @property
    def auth_headers(self) -> dict[str, str]:
        """
        Note: earthaccess handles authentication internally,
        so we don't need to provide explicit headers.
        """
        self.ensure_authenticated()
        return {}

    @property
    def auth_session(self) -> Any:
        self.ensure_authenticated()
        session_name = f"get_{self.mode}_session"
        if not hasattr(earthaccess, session_name):
            raise ValueError(f"Invalid mode: '{self.mode}' (earthaccess does not support this mode)")
        return getattr(earthaccess, session_name)()
