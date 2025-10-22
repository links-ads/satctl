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

from satctl.auth.base import Authenticator
from satctl.auth.earthdata import EarthDataAuthenticator
from satctl.auth.eumetsat import EUMETSATAuthenticator
from satctl.auth.odata import ODataAuthenticator
from satctl.auth.s3 import S3Authenticator
from satctl.registry import Registry

registry = Registry[Authenticator](name="authenticator")
registry.register("odata", ODataAuthenticator)
registry.register("earthaccess", EarthDataAuthenticator)
registry.register("s3", S3Authenticator)
registry.register("eumetsat", EUMETSATAuthenticator)


__all__ = ["ODataAuthenticator", "EarthDataAuthenticator", "S3Authenticator", "EUMETSATAuthenticator"]
