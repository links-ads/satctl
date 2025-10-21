from typing import Any

from satctl.auth import registry as auth_registry
from satctl.config import get_settings
from satctl.downloaders import registry as dwl_registry
from satctl.registry import Registry
from satctl.sources.base import DataSource
from satctl.sources.modis import MODISL1BSource
from satctl.sources.sentinel3 import OLCISource, SLSTRSource
from satctl.sources.sentinel2 import Sentinel2L2ASource, Sentinel2L1CSource
from satctl.sources.viirs import VIIRSL1BSource

registry = Registry[DataSource](name="source")
registry.register("slstr", SLSTRSource)
registry.register("olci", OLCISource)
registry.register("s2l2a", Sentinel2L2ASource)
registry.register("s2l1c", Sentinel2L1CSource)
registry.register("viirs-l1b", VIIRSL1BSource)
registry.register("modis-l1b", MODISL1BSource)


def create_source(
    source_name: str,
    authenticator: str | None = None,
    downloader: str | None = None,
    **kwargs: dict[str, Any],
) -> DataSource:
    """Create a data source from the given parameters.
    When left empty, parameters are inferred from the configuration, if present.

    Args:
        source_name (str): Name of the data source, strictly required.
        authenticator (str | None, optional): Authenticator class name. Inferrred from config when it defaults to None.
        downloader (str | None, optional): Downloader class name. Inferred from config when it defaults to None.
        kwargs (dict[str, Any], optional): Any other keyword argument to be passed to the source.

    Returns:
        DataSource: instance of the given data source.
    """
    config = get_settings()
    source_params = config.sources.get(source_name, {}).copy()
    source_params.update(kwargs)

    auth_instance = None
    if auth_name := source_params.pop("authenticator", authenticator):
        auth_config = config.auth.get(auth_name, {})
        auth_instance = auth_registry.create(auth_name, **auth_config)

    dwl_instance = None
    if dwl_name := source_params.pop("downloader", downloader):
        dwl_config = config.download.get(dwl_name, {})
        dwl_instance = dwl_registry.create(dwl_name, authenticator=auth_instance, **dwl_config)

    return registry.create(
        source_name,
        downloader=dwl_instance,
        **source_params,
    )


__all__ = [
    "OLCISource",
    "SLSTRSource",
    "VIIRSL1BSource",
    "MODISL1BSource",
    "Sentinel2L2ASource",
    "Sentinel2L1CSource",
    "create_source",
]
