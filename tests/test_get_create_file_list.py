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
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--create-file-list",
            "hello",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            "Assertion error: Download file list must be a '.txt' or '.csv' file."
            in self.output.stderr
        )

    def test_get_create_file_list(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--create-file-list",
            "hello.txt",
        ]
        self.output = execute_in_terminal(self.command)
        assert os.path.isfile("hello.txt")
        with open("hello.txt") as file:
            assert file.read() != ""

    def test_get_create_file_list_csv(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--create-file-list",
            "hello.csv",
        ]
        self.output = execute_in_terminal(self.command)
        assert os.path.isfile("hello.csv")
        with open("hello.csv") as file:
            assert (
                file.readline()
                == "filename,size,last_modified_datetime,etag\n"
            )

    def test_get_create_file_list_without_extension_raises_python(self):
        with pytest.raises(AssertionError) as e:
            get(
                dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
                create_file_list="hello",
                dry_run=True,
            )
        assert "Download file list must be a '.txt' or '.csv' file. " in str(
            e.value
        )

    def test_get_create_file_list_python(self):
        get(
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            create_file_list="hello_python.txt",
        )
        assert os.path.isfile("hello_python.txt")
        with open("hello_python.txt") as file:
            assert file.read() != ""

    def test_get_create_file_list_csv_python(self):
        get(
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            create_file_list="hello_python.csv",
        )
        assert os.path.isfile("hello_python.csv")
        with open("hello_python.csv") as file:
            assert (
                file.readline()
                == "filename,size,last_modified_datetime,etag\n"
            )

    def test_get_create_file_with_overwrite_option(self):
        get(
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            create_file_list="hello_python_overwrite.txt",
        )
        get(
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            create_file_list="hello_python_overwrite.txt",
            overwrite=True,
        )
        assert not os.path.isfile("hello_python_overwrite_(1).txt")

        get(
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            create_file_list="hello_python_overwrite.txt",
        )

        assert os.path.isfile("hello_python_overwrite_(1).txt")
