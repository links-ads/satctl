"""Data source implementations for different satellite missions.

This package provides source implementations for various satellite missions:
- Sentinel2L1CSource, Sentinel2L2ASource: Copernicus Sentinel-2 MSI data
- OLCISource, SLSTRSource: Copernicus Sentinel-3 OLCI and SLSTR data
- VIIRSL1BSource: NASA/NOAA VIIRS Level 1B data
- MODISL1BSource: NASA MODIS Level 1B data
- MTGSource: EUMETSAT Meteosat Third Generation data

All sources implement the DataSource interface and provide unified search,
download, and processing capabilities. Sources are configured via the registry
system and can be created using the create_source() factory function.
"""

from typing import Any

from satctl.config import get_settings
from satctl.registry import Registry
from satctl.sources.base import DataSource
from satctl.sources.earthdata import EarthDataSource
from satctl.sources.modis import MODISL1BSource
from satctl.sources.mtg import MTGSource
from satctl.sources.sentinel1 import Sentinel1GRDSource
from satctl.sources.sentinel2 import Sentinel2L1CSource, Sentinel2L2ASource
from satctl.sources.sentinel3 import OLCISource, SLSTRSource
from satctl.sources.viirs import VIIRSL1BSource

registry = Registry[DataSource](name="source")
registry.register("slstr", SLSTRSource)
registry.register("olci", OLCISource)
registry.register("s2l2a", Sentinel2L2ASource)
registry.register("s2l1c", Sentinel2L1CSource)
registry.register("mtg", MTGSource)
registry.register("viirs-l1b", VIIRSL1BSource)
registry.register("modis-l1b", MODISL1BSource)
registry.register("s1grd", Sentinel1GRDSource)


def create_source(source_name: str, **overrides: dict[str, Any]) -> DataSource:
    """Create a data source with optional factory overrides.

    Args:
        source_name: Name of the data source
        **overrides: Additional parameters to override source config

    Returns:
        DataSource instance

    Examples:
        # factories from config
        >>> source = create_source("s2l2a")

        # custom auth factory
        >>> source = create_source(
        ...     "s2-l2a",
        ...     auth_builder=lambda: ODataAuthenticator(username="test")
        ... )

        # both custom factories
        >>> source = create_source(
        ...     "s2-l2a",
        ...     auth_builder=configure_authenticator("odata", ...)
        ...     down_builder=configure_downloader("s3", ...)
        ... )
    """
    if not registry.is_registered(source_name):
        raise ValueError(f"Unknown source: {source_name}")
    # get global settings, if any, update with user-defined overrides
    config = get_settings()
    source_params = config.sources.get(source_name, {}).copy()
    source_params = {**source_params, **overrides}
    # let the base source init handle builders and such
    return registry.create(
        source_name,
        **source_params,
    )


__all__ = [
    "DataSource",
    "EarthDataSource",
    "OLCISource",
    "SLSTRSource",
    "Sentinel2L2ASource",
    "Sentinel2L1CSource",
    "Sentinel1GRDSource",
    "MTGSource",
    "VIIRSL1BSource",
    "MODISL1BSource",
    "create_source",
]
