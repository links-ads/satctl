from satctl.auth.base import Authenticator
from satctl.auth.earthdata import EarthDataAuthenticator
from satctl.auth.eumetsat import EUMETSATAuthenticator
from satctl.auth.odata import ODataAuthenticator
from satctl.registry import Registry

registry = Registry[Authenticator](name="authenticator")
registry.register("odata", ODataAuthenticator)
registry.register("earthaccess", EarthDataAuthenticator)
registry.register("eumetsat", EUMETSATAuthenticator)

__all__ = ["ODataAuthenticator", "EarthDataAuthenticator", "EUMETSATAuthenticator"]
