import logging
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import rasterio.crs
import rasterio.transform
from pyproj import CRS
from rasterio.transform import Affine
from xarray import DataArray

from satctl.writers import Writer

log = logging.getLogger(__name__)


class GeoTIFFWriter(Writer):
    """Writer for GeoTIFF format with configurable options."""

    def __init__(
        self,
        compress: str = "lzw",
        tiled: bool = True,
        fill_value: Any = None,
    ):
        """Initialize GeoTIFF writer.

        Args:
            compress (str): Compression method. Defaults to 'lzw'.
            tiled (bool): Whether to create tiled GeoTIFF. Defaults to True.
            dtype (Any | None): Output data type. Defaults to None (use source dtype).
            fill_value (Any): No-data fill value. Defaults to None (auto-determined).
        """
        super().__init__(extension="tif")
        self.compress = compress
        self.tiled = tiled
        self.fill_value = fill_value

    def _get_transform_gcps(self, data_arr: DataArray) -> tuple[CRS | None, Affine | None, Any]:
        """Extract CRS, transform, and GCPs from DataArray.

        Args:
            data_arr (DataArray): Input data array with area attribute

        Returns:
            tuple[CRS | None, Affine | None, Any]: Tuple of (CRS, transform, GCPs)
        """
        crs = None
        transform = None
        gcps = None

        try:
            area = data_arr.attrs["area"]
            # try AreaDefinition first (regular grid)
            if hasattr(area, "crs"):
                crs = rasterio.crs.CRS.from_wkt(area.crs.to_wkt(version="WKT2_2018"))
            elif hasattr(area, "proj_dict"):
                crs = rasterio.crs.CRS(area.proj_dict)
            # get transform for regular grids
            if hasattr(area, "area_extent") and hasattr(area, "shape"):
                west, south, east, north = area.area_extent
                height, width = area.shape
                transform = rasterio.transform.from_bounds(west, south, east, north, width, height)
                log.debug("Created transform from AreaDefinition: %s", transform)

        except (KeyError, AttributeError):
            # Fall back to SwathDefinition (irregular grid with GCPs)
            try:
                area = data_arr.attrs["area"]
                if hasattr(area, "lons") and hasattr(area.lons, "attrs"):
                    gcps = area.lons.attrs.get("gcps")
                    crs = area.lons.attrs.get("crs")
                    if gcps and crs:
                        log.debug("Using GCPs from SwathDefinition: %s points", len(gcps))
            except (KeyError, AttributeError):
                log.warning("Couldn't extract geospatial information from DataArray")

        return crs, transform, gcps

    def _create_profile(
        self,
        height: int,
        width: int,
        bands: int,
        dtype: Any,
        crs: CRS | None,
        transform: Affine | None,
        fill_value: Any = None,
    ) -> dict[str, Any]:
        """Create rasterio profile dictionary.

        Args:
            height (int): Image height in pixels
            width (int): Image width in pixels
            bands (int): Number of bands
            dtype (Any): Data type
            crs (CRS | None): Coordinate reference system
            transform (Affine | None): Affine transform
            fill_value (Any): No-data value. Defaults to None.

        Returns:
            dict[str, Any]: Rasterio profile dictionary
        """
        return {
            "driver": "GTiff",
            "height": height,
            "width": width,
            "count": bands,
            "dtype": dtype,
            "crs": crs,
            "transform": transform,
            "compress": self.compress,
            "tiled": self.tiled,
            "nodata": fill_value,
        }

    def write(
        self,
        dataset: DataArray,
        output_path: Path,
        dtype: type | np.dtype[Any] | None = None,
        **tags: Any,
    ) -> Path:
        """Write DataArray to GeoTIFF file.

        Args:
            dataset (DataArray): Data array to write
            output_path (Path): Output file path
            **tags (Any): Additional metadata tags to write

        Returns:
            Path: Output file path

        Raises:
            FileNotFoundError: If output parent directory doesn't exist or is invalid
            ValueError: If data dimensions are unsupported
        """
        if not output_path.parent.exists() or output_path.is_dir():
            raise FileNotFoundError(
                f"Invalid output path: parent directory '{output_path.parent}' does not exist or path is a directory"
            )
        crs, transform, gcps = self._get_transform_gcps(dataset)
        # Prepare data
        data = dataset.values
        if dataset.ndim == 2:
            data = data.reshape(1, data.shape[0], data.shape[1])
            num_bands = 1
        elif data.ndim == 3:
            if "bands" in dataset.dims:
                band_dim_idx = dataset.dims.index("bands")
                if band_dim_idx != 0:
                    axes = list(range(data.ndim))
                    axes[0], axes[band_dim_idx] = axes[band_dim_idx], axes[0]
                    data = np.transpose(data, axes)
            num_bands = dataset.shape[0]
        else:
            raise ValueError(f"Unsupported data dimensions: {dataset.shape} (expected 2D or 3D array)")
        height, width = dataset.shape[-2:]

        # determine dtype and fill_value
        dtype = dtype or dataset.dtype
        data = data.astype(dtype)
        fill_value = self.fill_value
        if fill_value is None and np.issubdtype(dtype, np.floating):
            fill_value = np.nan

        # get band names
        band_names = []
        if "bands" in dataset.coords:
            band_names = [str(name) for name in dataset.coords["bands"].values]
        else:
            band_names = [f"band_{i + 1}" for i in range(num_bands)]

        # add GCPs if available (for swath data)
        profile = self._create_profile(height, width, num_bands, dtype, crs, transform, fill_value)
        if gcps is not None and crs is not None:
            profile["gcps"] = gcps
            profile.pop("transform", None)

        # write geotiff
        log.debug("Saving %d-band GeoTIFF: %s", num_bands, output_path)
        log.debug("Data shape: %s, dtype: %s", dataset.shape, dtype)

        # write the GeoTIFF
        with rasterio.open(output_path, "w", **profile) as dst:
            for i in range(num_bands):
                dst.write(data[i], i + 1)
                dst.set_band_description(i + 1, band_names[i])
            # add metadata
            tags = tags or {}
            for key, value in dataset.attrs.items():
                if isinstance(value, (str, int, float)) and key not in ["area"]:
                    tags[key] = str(value)
            dst.update_tags(**tags)

        log.debug("Successfully saved: %s", output_path)
        return output_path
