import os
from json import loads
from pathlib import Path

from tests.test_utils import execute_in_terminal

BINARY = os.getenv("BINARY_NAME")


class TestBasicCommandsBinaries:
    def test_help(self):
        self.output = execute_in_terminal([BINARY, "describe", "--help"])
        assert self.output.returncode == 0
        self.output = execute_in_terminal([BINARY, "get", "-h"])
        assert self.output.returncode == 0
        self.output = execute_in_terminal([BINARY, "subset", "-h"])
        assert self.output.returncode == 0
        self.output = execute_in_terminal([BINARY, "login", "-h"])
        assert self.output.returncode == 0

    def test_describe(self):
        command = [
            BINARY,
            "describe",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_subset(self):
        command = [
            BINARY,
            "subset",
            "-i",
            "med-hcmr-wav-rean-h",
            "-x",
            "13.723",
            "-X",
            "13.724",
            "-y",
            "38.007",
            "-Y",
            "40.028",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-01T06:00:00",
            "-v",
            "VHM0",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_get(self):
        command = [
            BINARY,
            "get",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
            "--filter",
            "*/2023/08/*",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0
        assert b"No data to download" not in self.output.stderr
        returned_value = loads(self.output.stdout)
        assert self.output.returncode == 0
        assert len(returned_value["files"]) != 0
        assert returned_value["total_size"]
        assert returned_value["status"]
        assert returned_value["message"]
        for get_file in returned_value["files"]:
            assert get_file["s3_url"] is not None
            assert get_file["https_url"] is not None
            assert get_file["file_size"] is not None
            assert get_file["last_modified_datetime"] is not None
            assert get_file["etag"] is not None
            assert get_file["file_format"] is not None
            assert get_file["output_directory"] is not None
            assert get_file["filename"] is not None
            assert get_file["file_path"] is not None
            assert not os.path.exists(get_file["file_path"])

    def test_get_download(self, tmp_path):
        command = [
            BINARY,
            "get",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
            "--filter",
            "*/2023/09/*",
            "--output-directory",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0

    def test_login(self, tmp_path):
        assert os.getenv("COPERNICUSMARINE_SERVICE_USERNAME") is not None
        assert os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD") is not None

        non_existing_directory = Path(tmp_path, "i_dont_exist")
        command = [
            BINARY,
            "login",
            "--force-overwrite",
            "--configuration-file-directory",
            f"{non_existing_directory}",
            "--username",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}",
            "--password",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert non_existing_directory.is_dir()
