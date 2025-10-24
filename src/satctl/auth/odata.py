import logging
from typing import Any

import requests

from satctl.auth.base import Authenticator

log = logging.getLogger(__name__)


class ODataAuthenticator(Authenticator):
    """Handles OAuth2 authentication for Copernicus Data Space Ecosystem"""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        username: str,
        password: str,
    ):
        """Initialize OData authenticator for Copernicus Data Space.

        Args:
            token_url (str): OAuth2 token endpoint URL
            client_id (str): OAuth2 client ID
            username (str): Copernicus username
            password (str): Copernicus password

        Raises:
            ValueError: If any required parameter is missing
        """
        self.token_url = token_url
        self.client_id = client_id
        self.username = username
        self.password = password
        self.access_token: str | None = None
        self.refresh_token: str | None = None

        if not self.token_url or not self.client_id:
            raise ValueError("Invalid configuration: token_url and client_id are required")
        if not self.username or not self.password:
            raise ValueError("Invalid configuration: username and password are required")

    def authenticate(self) -> bool:
        """Authenticate with username/password and get tokens.

        Returns:
            bool: True if authentication succeeded, False otherwise
        """
        try:
            data = {
                "grant_type": "password",
                "username": self.username,
                "password": self.password,
                "client_id": self.client_id,
            }
            response = requests.post(
                self.token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token")

            if not self.access_token:
                log.error("No access token received from authentication")
                return False

            log.debug("Successfully authenticated with Copernicus")
            return True

        except requests.exceptions.RequestException as e:
            log.error("Authentication failed: %s", e)
            return False

    def refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token.

        Returns:
            bool: True if refresh succeeded, False otherwise
        """
        if not self.refresh_token:
            log.warning("No refresh token available, need to re-authenticate")
            return self.authenticate()

        try:
            data = {"grant_type": "refresh_token", "refresh_token": self.refresh_token, "client_id": self.client_id}
            response = requests.post(
                self.token_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            response.raise_for_status()
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            # Note: refresh_token might be updated too
            if "refresh_token" in token_data:
                self.refresh_token = token_data["refresh_token"]
            if not self.access_token:
                log.error("No access token received from refresh")
                return False
            log.info("Successfully refreshed access token")
            return True

        except requests.exceptions.RequestException as e:
            log.error("Token refresh failed: %s", e)
            # If refresh fails, try to re-authenticate
            return self.authenticate()

    @property
    def auth_headers(self) -> dict[str, str]:
        """Get headers with Bearer token for authenticated requests.

        Returns:
            dict[str, str]: Dictionary with Authorization header

        Raises:
            RuntimeError: If authentication fails
        """
        if not self.access_token:
            if not self.authenticate():
                raise RuntimeError("Authentication failed for Copernicus Data Space: could not obtain access token")
        return {"Authorization": f"Bearer {self.access_token}"}

    @property
    def auth_session(self) -> Any:
        """Get authenticated session object.

        OData authenticator does not provide a session object.

        Returns:
            Any: None (no session object for OData)
        """
        return None

    def ensure_authenticated(self, refresh: bool = False) -> bool:
        """Ensure we have a valid access token.

        Args:
            refresh (bool): If True, refresh the token. Defaults to False.

        Returns:
            bool: True if authenticated, False otherwise
        """
        if not self.access_token:
            return self.authenticate()
        if refresh:
            return self.refresh_access_token()
        return True
