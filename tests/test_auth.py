"""Integration tests for authentication classes.

These tests verify that authenticators can successfully connect to real APIs
and perform authentication flows using credentials from the .env file.
"""

import pytest


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestEarthDataAuthenticator:
    """Integration tests for EarthDataAuthenticator."""

    def test_authenticate(self, earthdata_authenticator):
        """Test successful authentication with NASA EarthData."""
        result = earthdata_authenticator.authenticate()
        assert result is True, "Authentication should succeed"
        assert earthdata_authenticator._auth is not None, "Auth object should be set"
        assert earthdata_authenticator._auth.authenticated is True, "Should be authenticated"

    def test_auth_headers(self, earthdata_authenticator):
        """Test auth_headers property returns expected format."""
        headers = earthdata_authenticator.auth_headers
        assert isinstance(headers, dict), "auth_headers should return a dict"
        # EarthData handles auth internally, so headers are empty
        assert headers == {}, "EarthData auth_headers should be empty dict"

    def test_auth_session(self, earthdata_authenticator):
        """Test auth_session returns a valid session object."""
        session = earthdata_authenticator.auth_session
        assert session is not None, "auth_session should not be None"
        # Verify we got a requests session
        assert hasattr(session, "get"), "Session should have get method"
        assert hasattr(session, "post"), "Session should have post method"

    def test_ensure_authenticated(self, earthdata_authenticator):
        """Test ensure_authenticated maintains authentication state."""
        result = earthdata_authenticator.ensure_authenticated()
        assert result is True, "ensure_authenticated should return True"
        assert earthdata_authenticator._auth.authenticated is True


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestODataAuthenticator:
    """Integration tests for ODataAuthenticator."""

    def test_authenticate(self, odata_authenticator):
        """Test successful authentication with Copernicus OData."""
        result = odata_authenticator.authenticate()
        assert result is True, "Authentication should succeed"
        assert odata_authenticator.access_token is not None, "Access token should be set"
        assert odata_authenticator.refresh_token is not None, "Refresh token should be set"

    def test_auth_headers(self, odata_authenticator):
        """Test auth_headers returns Bearer token."""
        odata_authenticator.authenticate()
        headers = odata_authenticator.auth_headers
        assert isinstance(headers, dict), "auth_headers should return a dict"
        assert "Authorization" in headers, "Should have Authorization header"
        assert headers["Authorization"].startswith("Bearer "), "Should be a Bearer token"

    def test_ensure_authenticated(self, odata_authenticator):
        """Test ensure_authenticated when not yet authenticated."""
        result = odata_authenticator.ensure_authenticated()
        assert result is True, "ensure_authenticated should authenticate successfully"
        assert odata_authenticator.access_token is not None

    def test_auth_session(self, odata_authenticator):
        """Test auth_session property."""
        odata_authenticator.authenticate()
        session = odata_authenticator.auth_session
        assert session is None, "OData auth_session should return None"


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestS3Authenticator:
    """Integration tests for S3Authenticator.

    Note: These tests use use_temp_credentials=False, so they only verify OAuth2
    authentication. S3 credentials come from AWS environment/credentials file.
    """

    def test_authenticate(self, s3_authenticator):
        """Test successful OAuth2 authentication."""
        result = s3_authenticator.authenticate()
        assert result is True, "Authentication should succeed"
        assert s3_authenticator.access_token is not None, "OAuth access token should be set"
        # With use_temp_credentials=False, S3 keys won't be fetched from API
        assert s3_authenticator.use_temp_credentials is False

    def test_auth_headers(self, s3_authenticator):
        """Test auth_headers returns OAuth Bearer token."""
        s3_authenticator.authenticate()
        headers = s3_authenticator.auth_headers
        assert isinstance(headers, dict), "auth_headers should return a dict"
        assert "Authorization" in headers, "Should have Authorization header"
        assert headers["Authorization"].startswith("Bearer "), "Should be a Bearer token"

    def test_auth_session(self, s3_authenticator):
        """Test auth_session returns boto3 Session."""
        s3_authenticator.authenticate()
        session = s3_authenticator.auth_session
        assert session is not None, "auth_session should not be None"
        # Verify it's a boto3 Session
        assert hasattr(session, "client"), "Session should have client method"
        assert hasattr(session, "resource"), "Session should have resource method"

    def test_ensure_authenticated(self, s3_authenticator):
        """Test ensure_authenticated maintains valid OAuth token."""
        result = s3_authenticator.ensure_authenticated()
        assert result is True, "ensure_authenticated should succeed"
        assert s3_authenticator.access_token is not None, "OAuth token should be set"


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestEUMETSATAuthenticator:
    """Integration tests for EUMETSATAuthenticator."""

    def test_authenticate(self, eumetsat_authenticator):
        """Test successful authentication with EUMETSAT."""
        # Note: EUMETSATAuthenticator authenticates in __init__
        assert eumetsat_authenticator._authenticated is True, "Should be authenticated"
        assert eumetsat_authenticator.access_token is not None, "Access token should be set"

    def test_auth_headers(self, eumetsat_authenticator):
        """Test auth_headers returns Bearer token."""
        headers = eumetsat_authenticator.auth_headers
        assert isinstance(headers, dict), "auth_headers should return a dict"
        assert "Authorization" in headers, "Should have Authorization header"
        assert headers["Authorization"].startswith("Bearer "), "Should be a Bearer token"

    def test_auth_session(self, eumetsat_authenticator):
        """Test auth_session returns eumdac AccessToken."""
        session = eumetsat_authenticator.auth_session
        assert session is not None, "auth_session should not be None"
        # Verify it's a eumdac AccessToken
        from eumdac.token import AccessToken

        assert isinstance(session, AccessToken), "Should be eumdac.AccessToken instance"

    def test_ensure_authenticated(self, eumetsat_authenticator):
        """Test ensure_authenticated maintains authentication state."""
        result = eumetsat_authenticator.ensure_authenticated()
        assert result is True, "ensure_authenticated should return True"
        assert eumetsat_authenticator._authenticated is True
