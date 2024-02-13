import random

from copernicusmarine.download_functions.subset_xarray import longitude_modulus


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
