import pathlib
from json import loads

import pandas as pd

from copernicusmarine import read_dataframe
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

EXPECTED_COLUMNS = [
    "platform_id",
    "platform_type",
    "time",
    "longitude",
    "latitude",
    "depth",
    "is_approx_elevation",
    # "pressure", is removed cause full of NaN in the example # noqa
    "value",
    "value_qc",
    "variable",
]


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
        assert list(df.columns) == EXPECTED_COLUMNS

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
        assert list(df.columns) == EXPECTED_COLUMNS
