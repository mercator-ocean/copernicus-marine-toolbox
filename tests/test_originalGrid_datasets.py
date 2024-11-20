import pathlib
from json import loads

import pendulum
import xarray

from copernicusmarine.core_functions.utils import datetime_parser
from tests.test_utils import execute_in_terminal


class TestOriginalGridDatasets:
    def test_cmt_identifies_originalGrid_datasets(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_phy_anfc_6km_detided_P1M-m",
            "-v",
            "so",
            "--dataset-part",
            "originalGrid",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        returned_value = loads(self.output.stdout)
        assert returned_value["status"] == "001"
        assert b"WARNING" in self.output.stderr
        assert self.output.returncode == 0
        assert (
            b"Dataset part has the original projection" in self.output.stderr
        )

    def test_originalGrid_error_when_geospatial(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_phy_anfc_6km_detided_P1M-m",
            "-v",
            "so",
            "--dataset-part",
            "originalGrid",
            "-x",
            "0",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 1
        assert b"WARNING" in self.output.stderr
        assert (
            b"Dataset part has the original projection" in self.output.stderr
        )
        assert b"ERROR" in self.output.stderr
        assert (
            b"Geospatial subset not available for original projection: The geospatial"
            b" subset of datasets in a projection that is not in latitude and "
            b"longitude is not yet available. We are "
            b"developing such feature and will be supported in future versions."
        ) in self.output.stderr

    def test_originalGrid_error_when_timepart(self, tmp_path):
        output_filename = "output.nc"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_phy_anfc_6km_detided_P1M-m",
            "-v",
            "so",
            "--dataset-part",
            "originalGrid",
            "-t",
            "2022-01-01",
            "-T",
            "2022-01-01",
            "-z",
            "0",
            "-Z",
            "100",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]

        self.output = execute_in_terminal(command)

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        assert self.output.returncode == 0
        assert b"WARNING" in self.output.stderr
        assert (
            b"Dataset part has the original projection" in self.output.stderr
        )
        assert pendulum.parse("2022-01-01") == datetime_parser(
            dataset.time.values[0]
        )
        assert len(dataset.x.values) > 0
