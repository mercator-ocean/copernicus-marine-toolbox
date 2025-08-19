import pathlib
from json import loads

import pandas as pd
import pytest

from copernicusmarine import read_dataframe
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
    def test_i_can_subset_sparse_data(self, tmp_path):
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

    def test_i_can_subset_on_platform_ids_in_parquet(self, tmp_path):
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

    def test_can_download_in_different_format(self):
        # parquet done in another test
        command = BASIC_COMMAND + [
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert "in csv format" in self.output.stderr
        response = loads(self.output.stdout)
        assert response["filename"].endswith(".csv")

        wrong_command = BASIC_COMMAND + [
            "--file-format",
            "netcdf",
        ]
        self.output = execute_in_terminal(wrong_command)
        assert self.output.returncode == 1
        assert "Wrong format requested" in self.output.stderr

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
        with pytest.raises(
            ValueError,
            match="Minimum longitude greater than maximum longitude",
        ):
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

    def test_if_ask_for_empty_dataframe_it_works(self, caplog):
        df = read_dataframe(
            dataset_id="cmems_obs-wave_glo_phy-swh_nrt_cfo-l3_PT1S",
            variables=["VAVH", "VAVH_UNFILTERED", "WIND_SPEED"],
            minimum_latitude=30.1876,
            maximum_latitude=30.1877,
            minimum_longitude=31.9,
            maximum_longitude=32,
            start_datetime="01-01-2023",
            end_datetime="02-01-2023",
        )
        assert df.empty
        assert "No data found for the given parameters" in caplog.text

    def test_can_subset_along_track_data(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-sl_eur_phy-ssh_nrt_swon-l3-duacs_PT0.2S",
            "-t",
            "2025-01-04",
            "-T",
            "2025-01-05",
            "--output-directory",
            tmp_path,
            "-r",
            "all",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        output_path = pathlib.Path(response["file_path"])
        assert output_path.exists()
