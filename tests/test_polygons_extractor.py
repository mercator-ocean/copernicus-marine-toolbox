import importlib.util
import json
import pathlib

import numpy as np
import pytest
import xarray

from copernicusmarine.core_functions.exceptions import DependenciesNotAvailable
from copernicusmarine.core_functions.polygons_extractor import (
    _check_polygons_dependencies,
    extract_polygons_from_dataset,
    get_bounding_box_from_polygons,
    load_polygons_from_user_input,
)

SQUARE_POLYGON_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [0.0, 0.0],
                        [2.0, 0.0],
                        [2.0, 2.0],
                        [0.0, 2.0],
                        [0.0, 0.0],
                    ]
                ],
            },
        }
    ],
}

# Slightly larger polygon so that grid cell centers at 0, 1 and 2 are
# unambiguously inside (avoids rasterization edge effects on the boundary).
LARGER_SQUARE_POLYGON_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-0.5, -0.5],
                        [2.5, -0.5],
                        [2.5, 2.5],
                        [-0.5, 2.5],
                        [-0.5, -0.5],
                    ]
                ],
            },
        }
    ],
}


def _write_geojson(directory: pathlib.Path, content: dict) -> pathlib.Path:
    filepath = directory / "polygons.geojson"
    filepath.write_text(json.dumps(content))
    return filepath


def _make_lonlat_dataset() -> xarray.Dataset:
    lon = np.arange(-1.0, 4.0, 1.0)  # -1, 0, 1, 2, 3
    lat = np.arange(-1.0, 4.0, 1.0)
    data = np.ones((len(lat), len(lon)))
    dataset = xarray.Dataset(
        {"variable": (("latitude", "longitude"), data)},
        coords={"latitude": lat, "longitude": lon},
    )
    dataset["longitude"].attrs = {
        "standard_name": "longitude",
        "units": "degrees_east",
    }
    dataset["latitude"].attrs = {
        "standard_name": "latitude",
        "units": "degrees_north",
    }
    return dataset


def _fake_find_spec(missing: set[str]):
    real_find_spec = importlib.util.find_spec

    def fake(name: str, *args, **kwargs):
        if name in missing:
            return None
        return real_find_spec(name, *args, **kwargs)

    return fake


class TestPolygonsDependencyCheck:
    def test_check_passes_when_all_dependencies_available(self):
        pytest.importorskip("geopandas")
        pytest.importorskip("rioxarray")
        # Should not raise
        _check_polygons_dependencies()

    def test_raises_when_all_dependencies_missing(self, monkeypatch):
        monkeypatch.setattr(
            importlib.util,
            "find_spec",
            _fake_find_spec({"geopandas", "rioxarray"}),
        )
        with pytest.raises(DependenciesNotAvailable) as exc_info:
            _check_polygons_dependencies()

        exception = exc_info.value
        assert exception.missing_dependencies == ["geopandas", "rioxarray"]
        assert exception.extra_name == "extra"
        assert "pip install copernicusmarine[extra]" in str(exception)
        assert "dependencies are" in str(exception)

    def test_raises_when_single_dependency_missing(self, monkeypatch):
        monkeypatch.setattr(
            importlib.util,
            "find_spec",
            _fake_find_spec({"rioxarray"}),
        )
        with pytest.raises(DependenciesNotAvailable) as exc_info:
            _check_polygons_dependencies()

        exception = exc_info.value
        assert exception.missing_dependencies == ["rioxarray"]
        assert "dependency is" in str(exception)
        assert "rioxarray" in str(exception)

    def test_load_polygons_raises_clear_error_when_missing(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(
            importlib.util,
            "find_spec",
            _fake_find_spec({"geopandas", "rioxarray"}),
        )
        polygons_file = _write_geojson(tmp_path, SQUARE_POLYGON_GEOJSON)
        with pytest.raises(DependenciesNotAvailable):
            load_polygons_from_user_input(polygons_file)

    def test_get_bounding_box_raises_clear_error_when_missing(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(
            importlib.util,
            "find_spec",
            _fake_find_spec({"geopandas", "rioxarray"}),
        )
        polygons_file = _write_geojson(tmp_path, SQUARE_POLYGON_GEOJSON)
        with pytest.raises(DependenciesNotAvailable):
            get_bounding_box_from_polygons(polygons_file)

    def test_extract_polygons_raises_clear_error_when_missing(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            importlib.util,
            "find_spec",
            _fake_find_spec({"geopandas", "rioxarray"}),
        )
        # The dependency check happens before the dataset is used.
        with pytest.raises(DependenciesNotAvailable):
            extract_polygons_from_dataset(
                dataset=xarray.Dataset(),
                polygons=pathlib.Path("does_not_matter.geojson"),
            )


class TestLoadPolygonsFromUserInput:
    def test_load_valid_geojson(self, tmp_path):
        pytest.importorskip("geopandas")
        polygons_file = _write_geojson(tmp_path, SQUARE_POLYGON_GEOJSON)

        gdf = load_polygons_from_user_input(polygons_file)

        assert not gdf.empty
        assert len(gdf) == 1

    def test_load_accepts_string_path(self, tmp_path):
        pytest.importorskip("geopandas")
        polygons_file = _write_geojson(tmp_path, SQUARE_POLYGON_GEOJSON)

        gdf = load_polygons_from_user_input(str(polygons_file))

        assert not gdf.empty

    def test_raises_when_file_does_not_exist(self, tmp_path):
        pytest.importorskip("geopandas")
        missing_file = tmp_path / "i_do_not_exist.geojson"

        with pytest.raises(FileNotFoundError):
            load_polygons_from_user_input(missing_file)

    def test_raises_on_unsupported_format(self, tmp_path):
        pytest.importorskip("geopandas")
        unsupported_file = tmp_path / "polygons.txt"
        unsupported_file.write_text("not a polygon")

        with pytest.raises(ValueError, match="Unsupported file format"):
            load_polygons_from_user_input(unsupported_file)

    @pytest.mark.parametrize(
        "suffix,driver",
        [
            (".geojson", "GeoJSON"),
            (".shp", "ESRI Shapefile"),
            (".gpkg", "GPKG"),
            (".kml", "KML"),
        ],
    )
    def test_load_supported_formats(self, tmp_path, suffix, driver):
        gpd = pytest.importorskip("geopandas")
        shapely_geometry = pytest.importorskip("shapely.geometry")

        geometry = shapely_geometry.shape(
            SQUARE_POLYGON_GEOJSON["features"][0]["geometry"]
        )
        gdf = gpd.GeoDataFrame({"geometry": [geometry]}, crs="EPSG:4326")
        polygons_file = tmp_path / f"polygons{suffix}"

        try:
            gdf.to_file(polygons_file, driver=driver)
        except Exception as exception:
            pytest.skip(
                f"Driver for '{suffix}' not available in this "
                f"environment: {exception}"
            )

        loaded = load_polygons_from_user_input(polygons_file)

        assert len(loaded) == 1
        min_lon, min_lat, max_lon, max_lat = loaded.total_bounds
        assert (min_lon, min_lat, max_lon, max_lat) == pytest.approx(
            (0.0, 0.0, 2.0, 2.0)
        )


class TestGetBoundingBoxFromPolygons:
    def test_returns_expected_bounds(self, tmp_path):
        pytest.importorskip("geopandas")
        polygons_file = _write_geojson(tmp_path, SQUARE_POLYGON_GEOJSON)

        min_lon, min_lat, max_lon, max_lat = get_bounding_box_from_polygons(
            polygons_file
        )

        assert min_lon == pytest.approx(0.0)
        assert min_lat == pytest.approx(0.0)
        assert max_lon == pytest.approx(2.0)
        assert max_lat == pytest.approx(2.0)


class TestExtractPolygonsFromDataset:
    def test_clip_keeps_only_points_within_polygon(self, tmp_path):
        pytest.importorskip("geopandas")
        pytest.importorskip("rioxarray")
        polygons_file = _write_geojson(tmp_path, LARGER_SQUARE_POLYGON_GEOJSON)
        dataset = _make_lonlat_dataset()

        clipped = extract_polygons_from_dataset(
            dataset=dataset, polygons=polygons_file
        )

        assert isinstance(clipped, xarray.Dataset)
        # Points outside the polygon (-1 and 3) are dropped.
        assert float(clipped["longitude"].min()) >= 0.0
        assert float(clipped["longitude"].max()) <= 2.0
        assert float(clipped["latitude"].min()) >= 0.0
        assert float(clipped["latitude"].max()) <= 2.0
        # The CRS has been written on the dataset.
        assert clipped.rio.crs is not None
