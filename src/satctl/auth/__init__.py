"""Authentication modules for different satellite data providers.

This package provides authenticator implementations for various satellite data
providers including:
- ODataAuthenticator: OAuth2 authentication for Copernicus Data Space
- EarthDataAuthenticator: NASA EarthData authentication
- S3Authenticator: S3-compatible authentication (AWS, MinIO, etc.)
- EUMETSATAuthenticator: EUMETSAT Data Store authentication

All authenticators implement the Authenticator interface and are registered
for use throughout satctl.
"""

from typing import Any

from satctl.auth.base import Authenticator
from satctl.auth.earthdata import EarthDataAuthenticator
from satctl.auth.eumetsat import EUMETSATAuthenticator
from satctl.auth.odata import ODataAuthenticator
from satctl.auth.s3 import S3Authenticator
from satctl.config import get_settings
from satctl.registry import Builder, Registry

registry = Registry[Authenticator](name="authenticator")
registry.register("odata", ODataAuthenticator)
registry.register("earthdata", EarthDataAuthenticator)
registry.register("s3", S3Authenticator)
registry.register("eumetsat", EUMETSATAuthenticator)


class AuthBuilder(Builder[Authenticator]):
    """Authenticator factory type definition.
    Provides the means to defer the creation of an authenticator when we need one.
    """


def configure_authenticator(auth_name: str, **overrides: Any) -> AuthBuilder:
    """Create an authenticator factory from config for a given source.

    Args:
        auth_name: Name of the data source
        **overrides: Override auth config parameters

    Returns:
        Factory function that creates the configured authenticator

    Example:
        >>> factory = configure_authenticator("odata")
        >>> auth = factory()  # Creates ODataAuthenticator from config
    """
    if not registry.is_registered(auth_name):
        raise ValueError(f"Unknown authenticator: {auth_name}")
    config = get_settings()
    auth_config = config.auth.get(auth_name, {}).copy()
    auth_config.update(overrides)
    return AuthBuilder(name=auth_name, registry=registry, **auth_config)


__all__ = [
    "configure_authenticator",
    "AuthBuilder",
    "EarthDataAuthenticator",
    "EUMETSATAuthenticator",
    "ODataAuthenticator",
    "S3Authenticator",
]
