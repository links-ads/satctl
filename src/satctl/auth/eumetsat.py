import logging
from typing import Optional

from eumdac.token import AccessToken

from satctl.auth.base import Authenticator

log = logging.getLogger(__name__)


class EUMETSATAuthenticator(Authenticator):
    """
    Handles OAuth2 authentication for EUMETSAT Data Space Ecosystem
    and provides the eumdac.AccessToken object for client creation.
    """

    def __init__(self, consumer_key: str, consumer_secret: str):
        """Initialize EUMETSAT authenticator.

        Args:
            consumer_key (str): EUMETSAT API consumer key
            consumer_secret (str): EUMETSAT API consumer secret

        Raises:
            ValueError: If consumer_key or consumer_secret is missing
        """
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self._authenticated = False
        self.access_token: Optional[AccessToken] = None
        if not self.consumer_key or not self.consumer_secret:
            raise ValueError("Invalid configuration: consumer_key and consumer_secret are required")

        # Attempt initial authentication immediately
        self.ensure_authenticated()

    def authenticate(self) -> bool:
        """Authenticate with consumer key/secret and get AccessToken object.

        Returns:
            bool: True if authentication succeeded, False otherwise
        """
        try:
            # Use eumdac to handle the connection and OAuth flow
            self.access_token = AccessToken((self.consumer_key, self.consumer_secret))

            if not self.access_token:
                log.error("No AccessToken object received from authentication")
                self._authenticated = False
                return self._authenticated

            self._authenticated = True
            log.info("Successfully authenticated with EUMETSAT")
            return self._authenticated

        except Exception as e:
            log.error("Authentication failed: %s", e)
            self._authenticated = False
            self.access_token = None
            return False

    @property
    def auth_headers(self) -> dict[str, str]:
        """Get standard authorization headers (Bearer token) for generic HTTP use.

        Returns:
            dict[str, str]: Dictionary with Authorization header

        Raises:
            RuntimeError: If authentication fails
        """
        if not self.ensure_authenticated():
            raise RuntimeError("Authentication failed for EUMETSAT: could not obtain access token")

        # Extract the token string from the AccessToken object
        token_string = str(self.access_token)
        return {"Authorization": f"Bearer {token_string}"}

    @property
    def auth_session(self) -> Optional[AccessToken]:
        """Return the authenticated eumdac AccessToken object.

        Required for creating the eumdac.DataStore client.

        Returns:
            Optional[AccessToken]: The AccessToken object for EUMETSAT API access

        Raises:
            RuntimeError: If authentication fails
        """
        if not self.ensure_authenticated(refresh=True):
            raise RuntimeError("Authentication failed for EUMETSAT: AccessToken is not available")
        return self.access_token

    def ensure_authenticated(self, refresh: bool = False) -> bool:
        """Ensure we have a valid access token.

        Args:
            refresh (bool): If True, force re-authentication. Defaults to False.

        Returns:
            bool: True if authenticated, False otherwise
        """
        if not self._authenticated or refresh:
            return self.authenticate()
        return True
