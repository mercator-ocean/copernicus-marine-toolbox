import os
from typing import Optional

import geopandas as gpd

from copernicusmarine import open_dataset


def load_gdf_and_bbox(polygon_file):
    """
    Loads a vector file and returns a GeoDataFrame and its bounding box (bbox_upload).

    Parameters
    ----------
    polygon_file : str
        Path to a supported vector file (GeoJSON, SHP, ZIP, GPKG, etc.)

    Returns
    -------
    gdf : GeoDataFrame
        The loaded geometries.

    bbox_upload : dict
        Dictionary with bounding box coordinates: lat/lon min/max.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    ValueError
        If the file cannot be read by GeoPandas.
    """

    if not os.path.isfile(polygon_file):
        raise FileNotFoundError(f"File not found: {polygon_file}")

    try:
        gdf = gpd.read_file(polygon_file)
    except Exception as e:
        raise ValueError(
            f"Unable to read the file '{polygon_file}'. "
            "Make sure the file format is supported (e.g. GeoJSON, Shapefile, GPKG). "
            f"Underlying error: {e}"
        )

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    lon_min, lat_min, lon_max, lat_max = gdf.total_bounds
    bbox_upload = {
        "lat_min": lat_min,
        "lat_max": lat_max,
        "lon_min": lon_min,
        "lon_max": lon_max,
    }

    return gdf, bbox_upload


def subset_and_clip_dataset(
    dataset_id: str,
    bbox: dict,
    start_date: str,
    end_date: str,
    variables: list,
    min_depth: Optional[float] = None,
    max_depth: Optional[float] = None,
    gdf=None,
):
    """
    Opens a Copernicus Marine dataset, applies spatial/temporal/variable subsetting,
    reprojects the data and applies a clip based on the GeoDataFrame.

    Parameters
    ----------
    dataset_id : str
        Copernicus Marine dataset ID.

    bbox : dict
        Dictionary containing lat_min, lat_max, lon_min, lon_max.

    start_date : str
        Start date (Format: "YYYY-MM-DD").

    end_date : str
        End date (Format: "YYYY-MM-DD").

    variables : list
        List of variables to extract.

    min_depth : float, optional
        Minimum depth (in meters). None to ignore.

    max_depth : float, optional
        Maximum depth (in meters). None to ignore.

    gdf : GeoDataFrame
        Polygon used to apply the spatial clip.

    Returns
    -------
    raster_clipped : xarray.Dataset
        Dataset clipped using the polygon and reprojected.
    """

    # Conditional construction of the depth_filter
    depth_filter = (
        {"minimum_depth": min_depth, "maximum_depth": max_depth}
        if min_depth is not None and max_depth is not None
        else {}
    )

    # Open the dataset
    DS_subset = open_dataset(
        dataset_id=dataset_id,
        minimum_longitude=bbox["lon_min"],
        maximum_longitude=bbox["lon_max"],
        minimum_latitude=bbox["lat_min"],
        maximum_latitude=bbox["lat_max"],
        start_datetime=start_date,
        end_datetime=end_date,
        variables=variables,
        **depth_filter,
    )

    # Set projection to EPSG:4326
    DS_subset_4326 = DS_subset.rio.write_crs("epsg:4326")

    # Clip using the polygon
    raster_clipped = DS_subset_4326.rio.clip(
        gdf.geometry.values, gdf.crs, drop=True
    )

    return raster_clipped


def encoding(dataset_clipped, output_file=None):
    prepare_encoding = {}

    # If no output file is specified, return the dataset without saving
    if output_file is None:
        return dataset_clipped

    # List of encoding attributes supported by netCDF4
    netcdf4_expected_encoding_value = [
        "scale_factor",
        "add_offset",
        "dtype",
        "complevel",
        "_FillValue",
        "fletcher32",
        "zlib",
        "chunksizes",
        "contiguous",
        "shuffle",
        "compression",
        "least_significant_digit",
    ]

    # Loop through variables and keep supported encodings
    for variable in dataset_clipped.data_vars:
        encoding = dataset_clipped[variable].encoding

        # Filter encoding options to keep only those expected by netCDF4
        filtered_encoding = {
            key: value
            for key, value in encoding.items()
            if key in netcdf4_expected_encoding_value
        }

        # Ensure key encoding options are set if not already defined
        if "zlib" not in filtered_encoding:  # Enables compression
            filtered_encoding["zlib"] = True
        if (
            "complevel" not in filtered_encoding
        ):  # Compression level (1 = fast, 9 = max)
            filtered_encoding["complevel"] = 1
        if (
            "contiguous" not in filtered_encoding
        ):  # Ensures chunk-based storage (faster for large files)
            filtered_encoding["contiguous"] = False
        if (
            "shuffle" not in filtered_encoding
        ):  # Enables shuffle filter for better compression
            filtered_encoding["shuffle"] = True

        # Store optimized encoding settings for each variable
        prepare_encoding[variable] = filtered_encoding

    # Ensure .nc extension is present
    if not output_file.endswith(".nc"):
        output_file += ".nc"

    # ================ Save the dataset as a compressed NetCDF file ================
    dataset_clipped.to_netcdf(
        path=output_file,
        mode="w",  # Overwrites if a new filename is chosen
        engine="netcdf4",
        encoding=prepare_encoding,  # Apply optimized encoding settings
    )
    print(f"Dataset successfully saved at: {output_file}")

    return dataset_clipped


def extract_clipped_dataset(
    polygon_file: str,
    dataset_id: str,
    start_date: str,
    end_date: str,
    variables: list,
    min_depth: Optional[float] = None,
    max_depth: Optional[float] = None,
    output_file: Optional[str] = None,
):
    """
    Load a polygon, extract the corresponding Copernicus Marine dataset,
    apply spatial clipping, compress and optionally save the result.

    Parameters
    ----------
    polygon_file : str
        Path to the GeoJSON/SHP/ZIP polygon file.

    dataset_id : str
        ID of the Copernicus Marine dataset.

    start_date : str
        Start date (ISO format).

    end_date : str
        End date (ISO format).

    variables : list
        Variables to extract from the dataset.

    min_depth : float, optional
        Minimum depth (in meters).

    max_depth : float, optional
        Maximum depth (in meters).

    output_file : str, optional
        Output NetCDF file path. If None, no file is written.

    Returns
    -------
    xarray.Dataset
        The final clipped and (optionally) saved dataset.
    """

    # Step 1: Load polygon and get bounding box
    gdf, bbox = load_gdf_and_bbox(polygon_file)

    # Step 2: Extract and clip dataset using polygon
    raster_clipped = subset_and_clip_dataset(
        dataset_id=dataset_id,
        bbox=bbox,
        start_date=start_date,
        end_date=end_date,
        variables=variables,
        min_depth=min_depth,
        max_depth=max_depth,
        gdf=gdf,
    )

    # Step 3: Apply compression and/or save
    final_dataset = encoding(raster_clipped, output_file=output_file)

    # Final user feedback
    print("Dataset was successfully clipped and is ready for use.")

    return final_dataset
