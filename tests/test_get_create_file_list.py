import os

import pytest

from copernicusmarine import get
from tests.test_utils import execute_in_terminal


class TestGetCreateFileList:
    def test_get_create_file_list_without_extension_raises(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy-hflux_my_0.027deg_P1M-m",
            "--create-file-list",
            "hello",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            "Assertion error: Download file list must be a '.txt' or '.csv' file."
            in self.output.stderr
        )

    def test_get_create_file_list(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m",
            "--create-file-list",
            "hello.txt",
            "--output-directory",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(self.command)
        assert os.path.isfile(tmp_path / "hello.txt")
        with open(tmp_path / "hello.txt") as file:
            assert file.read() != ""

    def test_get_create_file_list_csv(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy-mflux_my_0.027deg_P1M-m",
            "--create-file-list",
            "hello.csv",
            "--output-directory",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(self.command)
        assert os.path.isfile(tmp_path / "hello.csv")
        with open(tmp_path / "hello.csv") as file:
            assert (
                file.readline()
                == "filename,size,last_modified_datetime,etag\n"
            )

    def test_get_create_file_list_without_extension_raises_python(self):
        with pytest.raises(AssertionError) as e:
            get(
                dataset_id="cmems_mod_ibi_phy-mld_my_0.027deg_P1M-m",
                create_file_list="hello",
                dry_run=True,
            )
        assert (
            str(e.value)
            == "Download file list must be a '.txt' or '.csv' file. "
        )

    def test_get_create_file_list_python(self, tmp_path):
        get(
            dataset_id="cmems_mod_ibi_phy-sal_my_0.027deg_P1M-m",
            create_file_list="hello_python.txt",
            output_directory=tmp_path,
        )
        assert os.path.isfile(tmp_path / "hello_python.txt")
        with open(tmp_path / "hello_python.txt") as file:
            assert file.read() != ""

    def test_get_create_file_list_csv_python(self, tmp_path):
        get(
            dataset_id="cmems_mod_ibi_phy-ssh_my_0.027deg_P1M-m",
            create_file_list="hello_python.csv",
            output_directory=tmp_path,
        )
        assert os.path.isfile(tmp_path / "hello_python.csv")
        with open(tmp_path / "hello_python.csv") as file:
            assert (
                file.readline()
                == "filename,size,last_modified_datetime,etag\n"
            )

    def test_get_create_file_with_overwrite_option(self, tmp_path):
        get(
            dataset_id="cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m",
            create_file_list="hello_python_overwrite.txt",
            output_directory=tmp_path,
        )
        get(
            dataset_id="cmems_mod_ibi_phy-temp_my_0.027deg_P1M-m",
            create_file_list="hello_python_overwrite.txt",
            overwrite=True,
            output_directory=tmp_path,
        )
        assert not os.path.isfile(tmp_path / "hello_python_overwrite_(1).txt")

        get(
            dataset_id="cmems_mod_ibi_phy-wcur_my_0.027deg_P1M-m",
            create_file_list="hello_python_overwrite.txt",
            output_directory=tmp_path,
        )
        assert os.path.isfile(tmp_path / "hello_python_overwrite_(1).txt")
