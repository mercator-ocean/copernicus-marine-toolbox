import importlib.util
import logging
import pathlib
from typing import TYPE_CHECKING

import xarray

from copernicusmarine.core_functions.exceptions import DependenciesNotAvailable

if TYPE_CHECKING:
    import pandas as pd
    from geopandas import GeoDataFrame

logger = logging.getLogger("copernicusmarine")

POLYGONS_EXTRA_DEPENDENCIES = ["geopandas", "rioxarray"]


def _check_polygons_dependencies() -> None:
    missing_dependencies = [
        dependency
        for dependency in POLYGONS_EXTRA_DEPENDENCIES
        if importlib.util.find_spec(dependency) is None
    ]
    if missing_dependencies:
        raise DependenciesNotAvailable(
            missing_dependencies=missing_dependencies,
            extra_name="extra",
        )


def extract_polygons_from_dataset(
    dataset: xarray.Dataset,
    polygons: pathlib.Path,
) -> xarray.Dataset:
    _check_polygons_dependencies()
    # rioxarray is imported for its side effect: it registers the
    # `.rio` accessor on xarray objects.
    import rioxarray  # noqa: F401

    gdf = load_polygons_from_user_input(polygons)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    # Set projection to EPSG:4326
    dataset = dataset.rio.write_crs("epsg:4326")

    # Clip using the polygon
    dataset = dataset.rio.clip(gdf.geometry.values, gdf.crs, drop=True)

    return dataset


def extract_polygons_from_dataframe(
    df: "pd.DataFrame",
    polygons: pathlib.Path,
    longitude_column: str = "longitude",
    latitude_column: str = "latitude",
) -> "pd.DataFrame":
    _check_polygons_dependencies()
    import geopandas as gpd

    gdf = load_polygons_from_user_input(polygons)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    points = gpd.GeoSeries(
        gpd.points_from_xy(df[longitude_column], df[latitude_column]),
        index=df.index,
        crs="EPSG:4326",
    )
    within_polygons = points.within(gdf.union_all())
    return df[within_polygons].reset_index(drop=True)


def get_bounding_box_from_polygons(
    polygons: pathlib.Path | str,
) -> tuple[float, float, float, float]:
    gdf = load_polygons_from_user_input(polygons)
    min_lon, min_lat, max_lon, max_lat = gdf.total_bounds
    return min_lon, max_lon, min_lat, max_lat


def load_polygons_from_user_input(
    polygons: pathlib.Path | str,
) -> "GeoDataFrame":
    _check_polygons_dependencies()
    import geopandas as gpd

    polygons = pathlib.Path(polygons)
    if not polygons.exists():
        raise FileNotFoundError(f"File not found: {polygons}")

    suffix = polygons.suffix.lower()
    supported_formats = {".geojson", ".shp", ".gpkg", ".kml"}

    if suffix not in supported_formats:
        raise ValueError(
            f"Unsupported file format: '{suffix}'. "
            f"Supported formats: {', '.join(sorted(supported_formats))}"
        )

    if suffix == ".kml":
        gdf = gpd.read_file(polygons, driver="KML")
    else:
        gdf = gpd.read_file(polygons)

    if gdf.empty:
        raise ValueError(f"No geometries found in file: {polygons}")

    return gdf
