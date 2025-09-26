from satctl.auth.base import Authenticator
from satctl.auth.earthdata import EarthDataAuthenticator
from satctl.auth.odata import ODataAuthenticator
from satctl.registry import Registry

registry = Registry[Authenticator]()
registry.register("odata", ODataAuthenticator)
registry.register("earthaccess", EarthDataAuthenticator)

__all__ = ["ODataAuthenticator", "EarthDataAuthenticator"]
