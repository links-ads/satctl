from satctl.auth import registry as auth_registry
from satctl.config import MainSettings
from satctl.downloaders import registry as dwl_registry
from satctl.registry import Registry
from satctl.sources.base import DataSource
from satctl.sources.sentinel3 import OLCISource, SLSTRSource

registry = Registry[DataSource]()
registry.register("slstr", SLSTRSource)
registry.register("olci", OLCISource)


def create_source(source_name: str, config: MainSettings):
    """Create and configure a source with authentication."""
    source_params = config.sources.get(source_name, {}).copy()

    authenticator = None
    if auth_name := source_params.pop("authenticator", None):
        auth_config = config.auth.get(auth_name, {})
        authenticator = auth_registry.create(auth_name, **auth_config)

    downloader = None
    if dwl_name := source_params.pop("downloader", None):
        dwl_config = config.download.get(dwl_name, {})
        downloader = dwl_registry.create(dwl_name, authenticator=authenticator, **dwl_config)

    return registry.create(
        source_name,
        downloader=downloader,
        **source_params,
    )


__all__ = ["OLCISource", "SLSTRSource", "create_source"]
