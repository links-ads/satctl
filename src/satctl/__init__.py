"""SatCtl: Unified satellite data access library.

SatCtl provides a unified interface for searching, downloading, and processing
satellite data from multiple providers including Copernicus Data Space, NASA
EarthData, and EUMETSAT.

The library aims to simplify satellite data workflows by providing:
- One unified entrypoint for different satellite data sources
- Minimal configuration complexity
- Simple handling of complex satellite data formats
- Workflows from raw data search -> download -> processing -> output

Example:
    >>> from satctl.sources import create_source
    >>> from satctl.model import SearchParams
    >>> from datetime import datetime
    >>>
    >>> source = create_source("s2l2a")
    >>> params = SearchParams(
    ...     area=my_polygon,
    ...     start=datetime(2023, 1, 1),
    ...     end=datetime(2023, 1, 31),
    ... )
    >>> granules = source.search(params)
    >>> source.download(granules, destination="data/downloads")
"""
