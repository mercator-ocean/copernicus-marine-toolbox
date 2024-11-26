import pathlib
from json import loads

import pendulum
import xarray

from copernicusmarine.core_functions.utils import datetime_parser
from tests.test_utils import execute_in_terminal

dataset_name = "cmems_mod_arc_bgc_my_ecosmo_P1D-m"
variable = "po4"


class TestOriginalGridDatasets:
    def test_toolbox_identifies_originalGrid_datasets(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "-v",
            variable,
            "--dataset-part",
            "originalGrid",
            "--dry-run",
            "--log-level",
            "DEBUG",
            "--staging",
        ]
        self.output = execute_in_terminal(command)
        returned_value = loads(self.output.stdout)
        assert returned_value["status"] == "001"
        assert b"DEBUG" in self.output.stderr
        assert self.output.returncode == 0
        assert (
            b"Dataset part has the non lat lon projection."
            in self.output.stderr
        )

    def test_originalGrid_error_when_geospatial(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "-v",
            variable,
            "--dataset-part",
            "originalGrid",
            "-x",
            "0",
            "--dry-run",
            "--log-level",
            "DEBUG",
            "--staging",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 1
        assert b"DEBUG" in self.output.stderr
        assert (
            b"Dataset part has the non lat lon projection."
            in self.output.stderr
        )
        assert b"ERROR" in self.output.stderr
        assert (
            b"Geospatial subset not available for non lat lon: The "
            b"geospatial subset of datasets in a projection that is not in "
            b"latitude and longitude is not yet available. We are "
            b"developing such feature and will be supported in future versions."
        ) in self.output.stderr

    def test_originalGrid_works_when_time_and_depth_subsetting(self, tmp_path):
        output_filename = "output.nc"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            dataset_name,
            "-v",
            variable,
            "--dataset-part",
            "originalGrid",
            "-t",
            "2020-01-01",
            "-T",
            "2020-01-01",
            "-z",
            "3",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--log-level",
            "DEBUG",
            "--response-fields",
            "all",
            "--staging",
        ]

        self.output = execute_in_terminal(command)

        dataset = xarray.open_dataset(pathlib.Path(tmp_path, output_filename))
        assert self.output.returncode == 0
        assert b"DEBUG" in self.output.stderr
        assert (
            b"Dataset part has the non lat lon projection."
            in self.output.stderr
        )
        assert pendulum.parse("2020-01-01") == datetime_parser(
            dataset.time.values[0]
        )
        assert len(dataset.x.values) > 0
        assert len(dataset.y.values) > 0
        returned_value = loads(self.output.stdout)
        assert returned_value["coordinates_extent"][0]["coordinate_id"] == "y"
        assert returned_value["coordinates_extent"][1]["coordinate_id"] == "x"
        assert (
            returned_value["coordinates_extent"][2]["coordinate_id"] == "time"
        )
        assert (
            returned_value["coordinates_extent"][3]["coordinate_id"] == "depth"
        )
        assert len(returned_value["coordinates_extent"]) == 4
