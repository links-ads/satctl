# satctl

Modular library and CLI utility to search, download and process satellite data from different sources, powered by [satpy](https://satpy.readthedocs.io/en/stable/).

[![test passing](https://img.shields.io/github/actions/workflow/status/link-ads/satctl/test.yml?branch=main)](https://github.com/links-ads/pydantic)
[![coverage](https://img.shields.io/codecov/c/gh/links-ads/satctl)](https://codecov.io/gh/links-ads/satctl)
[![pypi version](https://img.shields.io/pypi/v/satctl)](https://pypi.org/project/satctl/)
[![python versions](https://img.shields.io/pypi/pyversions/satctl)](https://github.com/links-ads/satctl)

[![license](https://img.shields.io/github/license/links-ads/satctl)](https://github.com/links-ads/satctl)
[![documentation](https://img.shields.io/badge/documentation-%F0%9F%93%9A-blue)](https://links-ads.github.io/satctl/)

## Quickstart

`satctl` can be installed via pip, or uv, in different flavors.

```shell
# via pip
$ pip install satctl
# via uv
$ uv add satctl
# including CLI tools
$ uv add satctl[console]
```

### Usage

`satctl` tries to be as modular as possible, giving the user the possibility to stop the process at any time in the pipeline, from searching to converting the raw data into a GeoTIFF.

Here's a simple example of usage.

```python
from datetime import datetime
from pathlib import Path

from satctl.model import ConversionParams, SearchParams
from satctl.sources import create_source
from satctl.writers import create_writer

if __name__ == "__main__":
    # search available satellites and products
    names = list_sources(name="s2*")

    # or directly create any source
    source = create_source("slstr")

    # Define a research area, from file or as a simple Polygon
    area_file = Path("my_aoi.geojson")

    # filter by space, time or source options
    params = SearchParams.from_file(path=area_file, datetime(2025, 8, 15), end=datetime(2025, 8, 16))
    items = source.search(params)

    # download the tiles locally
    downloaded, fail = source.download(items, destination=Path("downloads/"), num_workers=4)

    # ... load an item as a satpy Scene ...
    sene = source.load_scene(items[0], datasets=["S3", "S2", "S1"])

    # ... or store them directly on file
    writer = create_writer("geotiff")
    source.save(
        downloaded,
        params=ConversionParams.from_file(
            path=area_file,
            target_crs="4326",
            datasets=["all_bands_h"],
            resolution=500,
        ),
        destination=Path("results/"),
        writer=writer,
        num_workers=4,
    )
```

For more examples and use cases, see the documentation.

## Contributing

Contributing requires the following tools: `uv` for environment and project management, `ruff` for linting and formatting, `pyright` for standard type checking.
Formatting, linting and type checking is enforced at `pre-commit` and CI level.

The easiest way to quickstart is the following:
```
# prepare the environment, requires uv
$ make install
```

More information about contributing will be added in the main documentation.
