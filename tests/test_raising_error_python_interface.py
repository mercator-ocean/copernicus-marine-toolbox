import unittest
from datetime import datetime, timedelta

from copernicusmarine import core_functions, subset


class TestPythonInterface(unittest.TestCase):
    def test_error_Coord_out_of_dataset_bounds(self):
        with self.assertRaises(
            core_functions.exceptions.CoordinatesOutOfDatasetBounds
        ) as context:
            subset(
                dataset_id="cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
                start_datetime=datetime.today() + timedelta(10),
                force_download=True,
                end_datetime=datetime.today()
                + timedelta(days=10, hours=23, minutes=59),
            )

        assert "Some or all of your subset selection" in str(context.exception)
