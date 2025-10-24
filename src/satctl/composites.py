"""Custom satpy compositors for multi-band products.

This module extends satpy's compositor system to handle arbitrary numbers
of bands. The standard GenericCompositor is limited to 4 bands (RGBA),
but satellite data often requires stacking more bands for analysis.
"""

from typing import Optional, Sequence

import xarray as xr
from satpy.composites.core import CompositeBase, check_times
from satpy.dataset.metadata import combine_metadata


class MultiBandCompositor(CompositeBase):
    """Compositor for handling arbitrary number of bands.

    This compositor creates multi-band products by concatenating input datasets
    along the "bands" dimension. Unlike GenericCompositor which is limited to
    4 bands (L/LA/RGB/RGBA), this compositor handles any number of bands.

    Note:
        All input datasets must already be on the same grid. Use Scene.resample()
        before calling this compositor if your datasets have different resolutions.
    """

    def __call__(
        self,
        datasets: Sequence[xr.DataArray],
        optional_datasets: Sequence[xr.DataArray] | None = None,
        **attrs,
    ) -> xr.DataArray:
        """Build the multi-band composite.

        Args:
            datasets (Sequence[xr.DataArray]): Input data arrays to be combined
            optional_datasets (Sequence[xr.DataArray], optional): Optional additional data arrays (not currently used)
            **attrs (dict[str, Any]): Additional attributes to add to the result

        Returns:
            xr.DataArray: Combined multi-band data array

        Raises:
            ValueError: If no datasets are provided
            IncompatibleAreas: If datasets have incompatible dimensions or areas
        """
        if not datasets:
            raise ValueError("Invalid input: datasets list cannot be empty for composite creation")

        # single dataset case: just update attributes
        if len(datasets) == 1:
            result = datasets[0].copy()
            self._update_attributes(result.attrs, datasets, attrs)
            return result

        # ensure all datasets are compatible (same dimensions, areas, etc.)
        # this will raise IncompatibleAreas if there are issues
        matched_datasets = self.match_data_arrays(datasets)
        band_names = self._get_band_names(datasets)
        # concatenate along bands dimension
        combined = xr.concat(matched_datasets, dim="bands", coords="minimal")
        combined = combined.assign_coords(bands=band_names)
        # handle time coordinate: find common time if datasets have slightly different times
        time = check_times(datasets)
        if time is not None and "time" in combined.dims:
            combined["time"] = [time]
        new_attrs = self._create_combined_metadata(datasets, attrs)
        return xr.DataArray(data=combined.data, attrs=new_attrs, dims=combined.dims, coords=combined.coords)

    def _get_band_names(self, datasets: Sequence[xr.DataArray]) -> list[str]:
        """Extract meaningful band names from datasets.

        Args:
            datasets (Sequence[xr.DataArray]): Input datasets

        Returns:
            list[str]: List of band names
        """
        band_names = []
        for band_index, dataset in enumerate(datasets):
            # Try to get a meaningful name from the dataset
            name = dataset.attrs.get("name")
            if name is None:
                # Fallback to generic band naming
                name = f"band_{band_index + 1}"
            band_names.append(str(name))
        return band_names

    def _create_combined_metadata(self, datasets: Sequence[xr.DataArray], extra_attrs: dict) -> dict:
        """Create combined metadata for the composite.

        Args:
            datasets (Sequence[xr.DataArray]): Input datasets
            extra_attrs (dict[str, Any]): Additional attributes to include

        Returns:
            dict[str, Any]: Combined metadata dictionary
        """
        # combine metadata from all input datasets
        new_attrs = combine_metadata(*datasets)
        # remove metadata that doesn't make sense for composites
        for attr in ["wavelength", "units", "calibration", "modifiers"]:
            new_attrs.pop(attr, None)

        # add provided attributes (but don't override resolution if it exists)
        resolution = new_attrs.get("resolution")
        new_attrs.update(extra_attrs)
        if resolution is not None:
            new_attrs["resolution"] = resolution

        # add compositor's own attributes
        new_attrs.update(self.attrs)

        # set sensor information
        new_attrs["sensor"] = self._get_sensors(datasets)
        return new_attrs

    def _update_attributes(self, attrs: dict, datasets: Sequence[xr.DataArray], extra_attrs: dict) -> None:
        """Update attributes dictionary in-place for single dataset case.

        Args:
            attrs (dict): Attributes dictionary to update
            datasets (Sequence[xr.DataArray]): Input datasets for sensor info
            extra_attrs (dict): Additional attributes to add
        """
        attrs.update(extra_attrs)
        attrs.update(self.attrs)
        attrs["sensor"] = self._get_sensors(datasets)

    def _get_sensors(self, datasets: Sequence[xr.DataArray]) -> Optional[str | list[str]]:
        """Extract and combine sensor information from datasets.

        Args:
            datasets (Sequence[xr.DataArray]): Input datasets

        Returns:
            str | list[str] | None: Single sensor string, list of sensors, or None if no sensor info
        """
        sensors = set()
        for dataset in datasets:
            sensor = dataset.attrs.get("sensor")
            if sensor:
                if isinstance(sensor, (str, bytes)):
                    sensors.add(sensor)
                else:
                    # sensor is already a collection
                    sensors.update(sensor)

        if not sensors:
            return None
        elif len(sensors) == 1:
            return list(sensors)[0]
        else:
            return sorted(list(sensors))  # sort for deterministic output
