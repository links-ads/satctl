import logging
from datetime import datetime, timezone
from typing import Any

import boto3
import requests

from satctl.auth.base import Authenticator

log = logging.getLogger(__name__)


class S3Authenticator(Authenticator):
    """Handles S3 authentication for Copernicus Data Space Ecosystem.

    This authenticator obtains temporary S3 credentials from the Copernicus
    S3 credentials endpoint using an OAuth2 access token.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        username: str,
        password: str,
        endpoint_url: str,
        s3_credentials_url: str | None = None,
        use_temp_credentials: bool = True,
    ):
        """Initialize S3 authenticator for Copernicus Data Space Ecosystem.

        Args:
            token_url (str): OAuth2 token endpoint URL
            client_id (str): OAuth2 client ID
            username (str): Copernicus username
            password (str): Copernicus password
            endpoint_url (str): S3 endpoint URL
            s3_credentials_url (str | None): URL to obtain temporary S3 credentials. Defaults to None.
            use_temp_credentials (bool): Whether to use temporary S3 credentials. Defaults to True.

        Raises:
            ValueError: If any required parameter is missing
        """
        self.token_url = token_url
        self.client_id = client_id
        self.username = username
        self.password = password
        self.s3_credentials_url = s3_credentials_url or f"{token_url.rsplit('/protocol/', 1)[0]}/s3-credentials"
        self.endpoint_url = endpoint_url
        self.use_temp_credentials = use_temp_credentials

        # OAuth tokens
        self.access_token: str | None = None
        self.refresh_token: str | None = None

        # S3 credentials
        self.s3_access_key: str | None = None
        self.s3_secret_key: str | None = None
        self.s3_session_token: str | None = None
        self.s3_expiration: datetime | None = None

        if not self.token_url or not self.client_id:
            raise ValueError("Invalid configuration: token_url and client_id are required")
        if not self.username or not self.password:
            raise ValueError("Invalid configuration: username and password are required")

    def authenticate(self) -> bool:
        """Authenticate with OAuth2 and optionally obtain S3 credentials.

        Returns:
            bool: True if authentication succeeded, False otherwise
        """
        # First, get OAuth2 token
        if not self._get_oauth_token():
            return False

        # If using temporary credentials, get S3 credentials
        if self.use_temp_credentials:
            return self._get_s3_credentials()

        # Otherwise, just OAuth token is enough
        return True

    def _get_oauth_token(self) -> bool:
        """Get OAuth2 access token from Copernicus.

        Returns:
            bool: True if token obtained successfully, False otherwise
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

            log.debug("Successfully authenticated with Copernicus OAuth2")
            return True

        except requests.exceptions.RequestException as e:
            log.error("OAuth2 authentication failed: %s", e)
            return False

    def _get_s3_credentials(self) -> bool:
        """Get temporary S3 credentials using OAuth2 token.

        Returns:
            bool: True if credentials obtained successfully, False otherwise
        """
        if not self.access_token:
            log.error("No OAuth access token available")
            return False

        try:
            headers = {"Authorization": f"Bearer {self.access_token}"}
            log.debug("Requesting S3 credentials from: %s", self.s3_credentials_url)
            response = requests.get(self.s3_credentials_url, headers=headers, timeout=30)
            response.raise_for_status()

            creds = response.json()
            self.s3_access_key = creds.get("access_key") or creds.get("AccessKeyId")
            self.s3_secret_key = creds.get("secret_key") or creds.get("SecretAccessKey")
            self.s3_session_token = creds.get("session_token") or creds.get("SessionToken")

            # Parse expiration if provided
            expiration_str = creds.get("expiration") or creds.get("Expiration")
            if expiration_str:
                try:
                    self.s3_expiration = datetime.fromisoformat(expiration_str.replace("Z", "+00:00"))
                except Exception as e:
                    log.warning("Could not parse S3 expiration time: %s", e)
                    self.s3_expiration = None

            if not self.s3_access_key or not self.s3_secret_key:
                log.error("Failed to obtain S3 credentials from response")
                return False

            log.debug("Successfully obtained S3 credentials")
            return True

        except requests.exceptions.RequestException as e:
            log.warning("Failed to get S3 credentials from %s: %s", self.s3_credentials_url, e)
            log.warning("S3 access may require environment variables or ~/.aws/credentials")
            return False

    def _refresh_oauth_token(self) -> bool:
        """Refresh OAuth2 access token using refresh token.

        Returns:
            bool: True if refresh succeeded, False otherwise
        """
        if not self.refresh_token:
            log.warning("No refresh token available, need to re-authenticate")
            return self._get_oauth_token()

        try:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
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

            if "refresh_token" in token_data:
                self.refresh_token = token_data["refresh_token"]

            if not self.access_token:
                log.error("No access token received from refresh")
                return False

            log.debug("Successfully refreshed OAuth2 token")
            return True

        except requests.exceptions.RequestException as e:
            log.error("Token refresh failed: %s", e)
            return self._get_oauth_token()

    def _are_s3_credentials_valid(self) -> bool:
        """Check if S3 credentials are still valid.

        Returns:
            bool: True if credentials are valid, False otherwise
        """
        if not self.s3_access_key or not self.s3_secret_key:
            return False

        if self.s3_expiration:
            now = datetime.now(timezone.utc)
            if now >= self.s3_expiration:
                log.debug("S3 credentials expired")
                return False

        return True

    def ensure_authenticated(self, refresh: bool = False) -> bool:
        """Ensure we have valid S3 credentials.

        Args:
            refresh (bool): If True, force credential refresh. Defaults to False.

        Returns:
            bool: True if credentials are valid, False otherwise
        """
        if self.use_temp_credentials:
            if refresh or not self._are_s3_credentials_valid():
                # Try to refresh OAuth token and get new S3 credentials
                if not self._refresh_oauth_token():
                    return False
                return self._get_s3_credentials()
            return True
        else:
            # When using permanent credentials from environment, just ensure OAuth token is valid
            if refresh or not self.access_token:
                return self._refresh_oauth_token()
            return True

    @property
    def auth_headers(self) -> dict[str, str]:
        """Get OAuth2 authorization headers.

        Returns:
            dict[str, str]: Dictionary with Authorization header

        Raises:
            RuntimeError: If OAuth2 authentication fails
        """
        if not self.access_token:
            if not self._get_oauth_token():
                raise RuntimeError("Authentication failed for Copernicus Data Space: could not obtain OAuth2 token")
        return {"Authorization": f"Bearer {self.access_token}"}

    @property
    def auth_session(self) -> Any:
        """Return boto3 session configured with S3 credentials.

        Returns:
            Any: boto3.Session configured with temporary or default credentials

        Raises:
            RuntimeError: If S3 credentials cannot be obtained
        """
        if self.use_temp_credentials:
            if not self._are_s3_credentials_valid():
                if not self.ensure_authenticated():
                    raise RuntimeError(
                        "Authentication failed for Copernicus Data Space: could not obtain valid S3 credentials"
                    )

            # Create boto3 session with temporary credentials
            session = boto3.Session(
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                aws_session_token=self.s3_session_token,
            )
        else:
            # Use default credential chain (environment variables, ~/.aws/credentials, etc.)
            session = boto3.Session()

        return session
