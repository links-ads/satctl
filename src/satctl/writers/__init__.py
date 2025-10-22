"""Writer implementations for different output formats.

This package provides writers for exporting processed satellite data:
- GeoTIFFWriter: Exports data to GeoTIFF format with configurable compression

All writers implement the Writer interface and handle dataset metadata,
coordinate systems, and format-specific options.
"""

from satctl.registry import Registry
from satctl.writers.base import Writer
from satctl.writers.geotiff import GeoTIFFWriter

registry = Registry[Writer](name="writer")
registry.register("geotiff", GeoTIFFWriter)


def create_writer(writer_name: str, **config: dict) -> Writer:
    if not registry.is_registered(writer_name):
        raise ValueError(f"Unknown writer '{writer_name}', available: {registry.list()}")
    return registry.create(writer_name, **config)


__all__ = ["Writer", "GeoTIFFWriter"]
