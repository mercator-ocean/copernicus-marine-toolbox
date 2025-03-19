import pathlib
from pathlib import Path
from typing import Optional

from copernicusmarine.core_functions.utils import get_unique_filepath
from tests.test_utils import execute_in_terminal


class TestOverwriteOutputData:
    def expected_downloaded_filepath_with_counter(
        self, counter: Optional[int] = None
    ):
        if counter is None:
            return self.expected_downloaded_filepath
        else:
            parent = self.expected_downloaded_filepath.parent
            filename = self.expected_downloaded_filepath.stem
            extension = self.expected_downloaded_filepath.suffix
            return parent / (filename + "_(" + str(counter) + ")" + extension)

    def test_download_original_files(self, tmp_path):
        self.service = "original-files"
        self.filename = "data.nc"
        self.tmp_path = tmp_path
        self.expected_downloaded_filepath = pathlib.Path(
            "IBI_MULTIYEAR_PHY_005_002/cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20120101_20121231_R20221101_RE01.nc"  # noqa
        )
        self._test_download_with_overwrite_option()
        self._test_download_without_overwrite_option()

    def test_download_zarr(self, tmp_path):
        self.service = "arco-geo-series"
        self.filename = "data.zarr"
        self.tmp_path = tmp_path
        self.expected_downloaded_filepath = pathlib.Path(self.filename)
        self._test_download_with_overwrite_option()
        self._test_download_without_overwrite_option()

    def _test_download_with_overwrite_option(self):
        self.given_output_data_already_exists()
        self.when_overwrite_option_is_provided()
        self.then_output_data_is_overwritten()

    def _test_download_without_overwrite_option(self):
        self.given_output_data_already_exists()
        self.when_overwrite_option_is_not_provided()
        self.then_output_data_is_not_overwritten_and_new_files_are_created(
            counter=1
        )
        self.when_overwrite_option_is_not_provided()
        self.then_output_data_is_not_overwritten_and_new_files_are_created(
            counter=2
        )

    def given_output_data_already_exists(self):
        self.request_data_download(
            service=self.service,
            filename=self.filename,
            overwrite_option=True,
            init_download=True,
        )

    def when_overwrite_option_is_provided(self):
        self.request_data_download(
            service=self.service,
            filename=self.filename,
            overwrite_option=True,
            init_download=False,
        )

    def when_overwrite_option_is_not_provided(self):
        self.request_data_download(
            service=self.service,
            filename=self.filename,
            overwrite_option=False,
            init_download=False,
        )

    def then_output_data_is_overwritten(self):
        assert (
            pathlib.Path(
                self.tmp_path, self.expected_downloaded_filepath
            ).exists()
            is True
        )
        assert (
            pathlib.Path(
                self.tmp_path,
                self.expected_downloaded_filepath_with_counter(counter=1),
            ).exists()
            is False
        )

    def then_output_data_is_not_overwritten_and_new_files_are_created(
        self, counter
    ):
        assert (
            self.overwritten_output_data_modification_time
            == self.initial_output_data_modification_time
        )
        assert (
            pathlib.Path(
                self.tmp_path, self.expected_downloaded_filepath
            ).exists()
            is True
        )
        assert (
            pathlib.Path(
                self.tmp_path,
                self.expected_downloaded_filepath_with_counter(
                    counter=counter
                ),
            ).exists()
            is True
        )

    def request_data_download(
        self,
        service: str,
        filename: str,
        init_download: bool,
        overwrite_option: bool,
    ):
        folder = self.tmp_path

        command = command_from_service(service=service)
        full_command = []
        if command == "get":
            full_command = [
                "copernicusmarine",
                f"{command}",
                "-i",
                "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
                "--regex",
                ".*20120101.*",
                "-o",
                f"{folder}",
            ]
        elif command == "subset":
            full_command = [
                "copernicusmarine",
                f"{command}",
                "--service",
                f"{service}",
                "--dataset-id",
                "cmems_mod_bal_phy_my_P1M-m",
                "--variable",
                "sla",
                "--minimum-longitude",
                "9.8",
                "--maximum-longitude",
                "9.9",
                "--minimum-latitude",
                "54.1",
                "--maximum-latitude",
                "54.2",
                "--minimum-depth",
                "1.5",
                "--maximum-depth",
                "1.6",
                "--start-datetime",
                "2020-12-15",
                "--end-datetime",
                "2021-01-01",
                "-o",
                f"{folder}",
                "-f",
                f"{filename}",
            ]

        if overwrite_option:
            full_command.append("--overwrite")

        self.output = execute_in_terminal(full_command)
        assert self.output.returncode == 0, self.output.stderr

        if command == "get":
            if service == "original-files":
                last_modification_time = (
                    pathlib.Path(folder, self.expected_downloaded_filepath)
                    .stat()
                    .st_mtime
                )
        elif command == "subset":
            if service == "arco-time-series":
                last_modification_time = (
                    pathlib.Path(folder, self.expected_downloaded_filepath)
                    .stat()
                    .st_mtime
                )
            elif service == "arco-geo-series":
                last_modification_time = (
                    pathlib.Path(folder, self.expected_downloaded_filepath)
                    .stat()
                    .st_mtime
                )

        attribute = (
            "initial_output_data_modification_time"
            if init_download
            else "overwritten_output_data_modification_time"
        )
        setattr(self, attribute, last_modification_time)

        assert self.output.returncode == 0, self.output.stderr

    def test_that_overwrite_option_does_not_create_subdirectory(
        self, tmp_path
    ):
        """Unit test that verify that sub-directory is not created if data is
        overwritten and provided filepath is relative.

        JIRA ticket IOD-623 (https://mercator-ocean.atlassian.net/browse/IOD-623)
        """
        relative_folder = pathlib.Path(
            f"{tmp_path}/test_that_overwrite_option_does_not_create_subdirectory"
        )  # noqa
        pathlib.Path.mkdir(relative_folder, parents=True, exist_ok=True)
        filename = "test_file"
        file_extension = ".txt"
        relative_filepath = pathlib.Path(
            relative_folder, filename + file_extension
        )
        Path(relative_filepath).touch()
        unique_filepath = get_unique_filepath(
            filepath=relative_filepath,
        )
        assert unique_filepath == pathlib.Path(
            relative_folder, filename + "_(1)" + file_extension
        )


def command_from_service(service: str) -> Optional[str]:
    if service in ["original-files"]:
        return "get"
    elif service in [
        "arco-time-series",
        "arco-geo-series",
    ]:
        return "subset"
    return None
