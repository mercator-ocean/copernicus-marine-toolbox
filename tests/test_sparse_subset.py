import pathlib
from json import loads

import pandas as pd

from copernicusmarine import read_dataframe
from copernicusmarine.core_functions.exceptions import (
    MinimumLongitudeGreaterThanMaximumLongitude,
)
from copernicusmarine.download_functions.download_sparse import (
    COLUMNS_ORDER_DEPTH,
    COLUMNS_ORDER_ELEVATION,
)
from tests.test_utils import execute_in_terminal

BASIC_COMMAND = [
    "copernicusmarine",
    "subset",
    "--dataset-id",
    "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
    "--dataset-part",
    "history",
    "--variable",
    "PSAL",
    "--variable",
    "TEMP",
    "--minimum-latitude",
    "45",
    "--maximum-latitude",
    "90",
    "--minimum-longitude",
    "-146.99",
    "--maximum-longitude",
    "180",
    "--minimum-depth",
    "0",
    "--maximum-depth",
    "10",
    "--start-datetime",
    "2023-11-25T00:00:00",
    "--end-datetime",
    "2023-11-26T03:00:00",
    "-r",
    "all",
]

BASIC_COMMAND_DICT = {
    "dataset_id": "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
    "dataset_part": "history",
    "variables": ["PSAL", "TEMP"],
    "minimum_latitude": 45,
    "maximum_latitude": 90,
    "minimum_longitude": -146.99,
    "maximum_longitude": 180,
    "minimum_depth": 0,
    "maximum_depth": 10,
    "start_datetime": "2023-11-25T00:00:00",
    "end_datetime": "2023-11-26T03:00:00",
}


class TestSparseSubset:
    def test_I_can_subset_sparse_data(self, tmp_path):
        command = BASIC_COMMAND + [
            "--output-directory",
            tmp_path,
            "-r",
            "all",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        filename = response["filename"]
        assert (tmp_path / filename).exists()
        df = pd.read_csv(tmp_path / filename)
        assert not df.empty
        assert list(df.columns) == COLUMNS_ORDER_DEPTH

    def test_I_can_subset_on_platform_ids_in_parquet(self, tmp_path):
        command = BASIC_COMMAND + [
            "--platform-id",
            "B-Sulafjorden___MO",
            "--platform-id",
            "F-Vartdalsfjorden___MO",
            "--output-directory",
            tmp_path,
            "--output-filename",
            "sparse_data",
            "--file-format",
            "parquet",
            "-r",
            "all",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        assert response["filename"] == "sparse_data.parquet"
        output_path = pathlib.Path(response["file_path"])
        assert (
            output_path == tmp_path / "sparse_data.parquet"
            and output_path.exists()
        )

    def test_skip_existing_overwrite_default(self, tmp_path):
        command = BASIC_COMMAND + [
            "--output-directory",
            tmp_path,
            "--output-filename",
            "sparse_data",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (tmp_path / "sparse_data.csv").exists()

        command_skip_existing = command + ["--skip-existing"]
        self.output = execute_in_terminal(command_skip_existing)
        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        assert response["status"] == "000"
        assert response["file_status"] == "IGNORED"

        command_overwrite = command + ["--overwrite"]
        self.output = execute_in_terminal(command_overwrite)
        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        assert response["status"] == "000"
        assert response["file_status"] == "DOWNLOADED"

        command_default = command
        self.output = execute_in_terminal(command_default)
        assert self.output.returncode == 0
        assert (tmp_path / "sparse_data_(1).csv").exists()

    def test_can_download_in_csv_format(self, tmp_path):
        command = BASIC_COMMAND + [
            "--output-directory",
            tmp_path,
            "--output-filename",
            "sparse_data",
            "--file-format",
            "csv",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (tmp_path / "sparse_data.csv").exists()

    def test_can_read_dataframe(self):
        df = read_dataframe(**BASIC_COMMAND_DICT)
        assert not df.empty
        assert "value" in df.columns
        assert list(df.columns) == COLUMNS_ORDER_DEPTH

    def test_can_download_with_elevation(self):
        df = read_dataframe(
            vertical_axis="elevation",
            **BASIC_COMMAND_DICT,
        )
        assert not df.empty
        assert "value" in df.columns
        assert list(df.columns) == COLUMNS_ORDER_ELEVATION

    def test_error_raises_for_inverted_longitude(self):
        try:
            _ = read_dataframe(
                dataset_id="cmems_obs-wave_glo_phy-swh_nrt_cfo-l3_PT1S",
                variables=["VAVH", "VAVH_UNFILTERED", "WIND_SPEED"],
                minimum_latitude=30.1875,
                maximum_latitude=45.97916793823242,
                minimum_longitude=36.29166793823242,
                maximum_longitude=31.9,
                start_datetime="01-01-2023",
                end_datetime="02-01-2023",
            )
        except MinimumLongitudeGreaterThanMaximumLongitude as e:
            assert (
                "--minimum-longitude option must be smaller "
                "or equal to --maximum-longitude" in e.__str__()
            )

    def test_if_ask_for_empty_dataframe_it_works(self, caplog):
        df = read_dataframe(
            dataset_id="cmems_obs-wave_glo_phy-swh_nrt_cfo-l3_PT1S",
            variables=["VAVH", "VAVH_UNFILTERED", "WIND_SPEED"],
            maximum_latitude=30.1875,
            minimum_latitude=30.188,
            maximum_longitude=32,
            minimum_longitude=31.9,
            start_datetime="01-01-2023",
            end_datetime="02-01-2023",
        )
        assert df.empty
        assert "No data found for the given parameters" in caplog.text
