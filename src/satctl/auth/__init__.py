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
