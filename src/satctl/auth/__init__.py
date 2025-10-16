from satctl.auth.base import Authenticator
from satctl.auth.earthdata import EarthDataAuthenticator
from satctl.auth.eumetsat import EUMETSATAuthenticator
from satctl.auth.odata import ODataAuthenticator
from satctl.registry import Registry

registry = Registry[Authenticator](name="authenticator")
registry.register("odata", ODataAuthenticator)
registry.register("eumetsat", EUMETSATAuthenticator)
registry.register("earthdata", EarthDataAuthenticator)

__all__ = ["ODataAuthenticator", "EarthDataAuthenticator", "EUMETSATAuthenticator"]
