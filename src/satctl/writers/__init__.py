from satctl.registry import Registry
from satctl.writers.base import Writer
from satctl.writers.geotiff import GeoTIFFWriter

registry = Registry[Writer]()
registry.register("geotiff", GeoTIFFWriter)


def create_writer(writer_name: str, **config: dict) -> Writer:
    if not registry.is_registered(writer_name):
        raise ValueError(f"Unknown writer '{writer_name}', available: {registry.list()}")
    return registry.create(writer_name, **config)


__all__ = ["Writer", "GeoTIFFWriter"]
