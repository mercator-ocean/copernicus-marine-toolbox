import os

from copernicusmarine import get
from tests.test_utils import execute_in_terminal


class TestGetListFiles:
    def test_get_download_file_list_is_deprecated(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--download-file-list",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"'--download-file-list' has been deprecated, use '--list-files' instead"
            in self.output.stdout
        )

    def test_get_list_files_without_extension_raises(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--list-files",
            "hello",
        ]
        self.output = execute_in_terminal(self.command)
        assert (
            b"Assertion error: Download file list must be a .txt or .csv file."
            in self.output.stdout
        )

    def test_get_list_files(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--list-files",
            "hello.txt",
        ]
        self.output = execute_in_terminal(self.command)
        assert os.path.isfile("hello.txt")
        with open("hello.txt") as file:
            assert file.read() != ""

    def test_get_list_files_csv(self):
        self.command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
            "--list-files",
            "hello.csv",
        ]
        self.output = execute_in_terminal(self.command)
        assert os.path.isfile("hello.csv")
        with open("hello.csv") as file:
            assert (
                file.readline()
                == "filename,size,last_modified_datetime,etag\n"
            )

    def test_get_list_files_without_extension_raises_python(self):
        try:
            get(
                dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
                list_files="hello",
            )
        except AssertionError as e:
            assert str(e) == "Download file list must be a .txt or .csv file. "

    def test_get_list_files_python(self):
        try:
            get(
                dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
                list_files="hello_python.txt",
            )
        except SystemExit:
            pass
        assert os.path.isfile("hello_python.txt")
        with open("hello_python.txt") as file:
            assert file.read() != ""

    def test_get_list_files_csv_python(self):
        try:
            get(
                dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1M-m",
                list_files="hello_python.csv",
            )
            assert os.path.isfile("hello_python.csv")
        except SystemExit:
            pass
        with open("hello_python.csv") as file:
            assert (
                file.readline()
                == "filename,size,last_modified_datetime,etag\n"
            )
