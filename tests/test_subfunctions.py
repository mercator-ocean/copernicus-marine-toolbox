import datetime
import random

import copernicusmarine as cm
from copernicusmarine.download_functions.subset_xarray import (
    _dataset_custom_sel,
    longitude_modulus,
)


class TestSubfunctions:
    def test_longitude_modulus_values(self):
        longitude_test_examples = [
            {"raw": 520, "expected": 160},
            {"raw": -200, "expected": 160},
            {"raw": 880, "expected": 160},
            {"raw": 240.5, "expected": -119.5},
            {"raw": 360, "expected": 0},
            {"raw": -360, "expected": 0},
            {"raw": -1.0, "expected": -1},
            {"raw": 180, "expected": -180},
            {"raw": -180, "expected": -180},
            {"raw": -1234.56, "expected": -154.56},
        ]
        for longitude in longitude_test_examples:
            assert longitude_modulus(longitude["raw"]) == longitude["expected"]

    def test_longitude_modulus_range(self):
        random_values = [random.random() * 10000 + 360 for _ in range(50)]
        for random_value in random_values:
            modulus_value = longitude_modulus(random_value)
            assert modulus_value >= -180 and modulus_value < 180

    def test_custom_dataset_selection(self, tmp_path):
        dataset = cm.open_dataset(
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            minimum_longitude=0,
            maximum_longitude=50,
            minimum_latitude=0,
            maximum_latitude=50,
            minimum_depth=0,
            maximum_depth=100,
            start_datetime="2023-01-01",
            end_datetime="2023-01-03",
        )
        min_value = 1
        max_value = 49
        coord_selection = slice(min_value, max_value)
        dataset_1 = _dataset_custom_sel(
            dataset, "longitude", coord_selection, "strict-inside"
        )
        assert dataset_1.longitude.values.min() >= min_value
        assert dataset_1.longitude.max().values <= max_value
        dataset_1 = _dataset_custom_sel(
            dataset_1, "latitude", coord_selection, "strict-inside"
        )
        assert dataset_1.latitude.values.min() >= min_value
        assert dataset_1.latitude.values.max() <= max_value
        dataset_1 = _dataset_custom_sel(
            dataset_1, "depth", coord_selection, "strict-inside"
        )
        assert dataset_1.depth.values.min() >= min_value
        assert dataset_1.depth.values.max() <= max_value
        coord_selection = slice(
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 1, 3),
        )
        dataset_1 = _dataset_custom_sel(
            dataset_1, "time", coord_selection, "strict-inside"
        )
        assert datetime.datetime.strptime(
            str(dataset_1.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.datetime.strptime("2023-01-01", "%Y-%m-%d")
        assert datetime.datetime.strptime(
            str(dataset_1.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.datetime.strptime("2023-01-03", "%Y-%m-%d")

        min_value = 20
        max_value = 39.9
        coord_selection = slice(min_value, max_value)
        dataset_1 = _dataset_custom_sel(
            dataset_1, "longitude", coord_selection, "outside"
        )
        assert dataset_1.longitude.values.min() <= min_value
        assert dataset_1.longitude.max().values >= max_value
        dataset_1 = _dataset_custom_sel(
            dataset_1, "latitude", coord_selection, "outside"
        )
        assert dataset_1.latitude.values.min() <= min_value
        assert dataset_1.latitude.values.max() >= max_value
        dataset_1 = _dataset_custom_sel(
            dataset_1, "depth", coord_selection, "outside"
        )
        assert dataset_1.depth.values.min() <= min_value
        assert dataset_1.depth.values.max() >= max_value
        coord_selection = slice(
            datetime.datetime(2023, 1, 2),
            datetime.datetime(2023, 1, 2),
        )
        dataset_1 = _dataset_custom_sel(
            dataset_1, "time", coord_selection, "outside"
        )
        assert datetime.datetime.strptime(
            str(dataset_1.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.datetime.strptime("2023-01-02", "%Y-%m-%d")
        assert datetime.datetime.strptime(
            str(dataset_1.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.datetime.strptime("2023-01-02", "%Y-%m-%d")

        # Check also that when asking for values outside the dataset,
        # the returned makes sense

        coord_selection = slice(10, 45)
        dataset_1 = _dataset_custom_sel(
            dataset_1, "longitude", coord_selection, "outside"
        )
        assert dataset_1.longitude.values.min() >= 20  # the old values
        assert dataset_1.longitude.max().values <= 39.92
