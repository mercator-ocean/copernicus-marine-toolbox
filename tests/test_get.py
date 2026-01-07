import fnmatch
import logging
import os
import pathlib
import re
from datetime import datetime, timedelta
from json import loads
from pathlib import Path
from unittest import mock

from copernicusmarine import get
from tests.test_utils import execute_in_terminal, get_all_files_in_folder_tree

logger = logging.getLogger()


class TestGet:
    def test_get_download_s3_without_regex(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy-cur_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 30

    def test_get_download_s3_with_regex(self, tmp_path):
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--regex",
            f"{regex}",
            "--output-directory",
            f"{tmp_path}",
            "--skip-existing",
        ]

        self.output = execute_in_terminal(command, safe_quoting=True)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 3

        for filename in downloaded_files:
            assert re.match(regex, filename) is not None

    def test_get_something_and_skip_existing(self, tmp_path):
        self.when_get_by_default_returns_status_message(tmp_path)
        self.and_i_do_skip_existing(tmp_path)

    def when_get_by_default_returns_status_message(self, tmp_path):
        filter_option = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter_option}",
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert returned_value["status"]
        assert returned_value["message"]

    def and_i_do_skip_existing(self, tmp_path):
        filter_option = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter_option}",
            "--output-directory",
            f"{tmp_path}",
            "--skip-existing",
            "-r",
            "all",
        ]
        self.output2 = execute_in_terminal(command)
        assert self.output2.returncode == 0
        returned_value = loads(self.output2.stdout)
        assert returned_value["status"] == "003"
        assert returned_value["message"]
        start_path = (
            f"{tmp_path}/"
            f"IBI_MULTIYEAR_PHY_005_002/"
            f"cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m_202511/"
            f"CMEMS_v6r1_IBI_PHY_MY_NL_01yav_temp_"
        )
        assert os.path.exists(
            start_path + "20010101_20011231_R20251125_RE01.nc"
        )
        assert not os.path.exists(
            start_path + "20010101_20011231_R20251125_RE01_(1).nc"
        )
        assert returned_value["total_size"] == 0

    def test_get_download_with_dry_run_option(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy-cur_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--output-directory",
            f"{tmp_path}",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)
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
            assert str(tmp_path) in get_file["file_path"]
            assert not os.path.exists(get_file["file_path"])

    def test_get_can_choose_return_fields(self, tmp_path):
        filter_ = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy-mld_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter_}",
            "--output-directory",
            f"{tmp_path}",
            "-r",
            "https_url",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert "status" not in returned_value
        assert "message" not in returned_value
        assert "files" in returned_value
        for get_file in returned_value["files"]:
            assert "s3_url" not in get_file
            assert "https_url" in get_file
            assert "https://" in get_file["https_url"]

    def test_get_wrong_input_response_fields_warning_and_error(self):
        dataset_id = "cmems_mod_ibi_phy-mld_my_0.027deg_P1Y-m"
        response_fields = "https_url, wrong_field"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--dry-run",
            "-r",
            response_fields,
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            "Some ``--response-fields`` fields are invalid:"
            " wrong_field" in self.output.stderr
        )

        command[-1] = "wrong_field1, wrong_field2"
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            "Wrong fields error: All ``--response-fields`` "
            "fields are invalid: wrong_field1, wrong_field2"
            in self.output.stderr
        )

    def test_get_download_s3_with_wildcard_filter(self, tmp_path):
        filter_ = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter_}",
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 3

        for filename in downloaded_files:
            assert fnmatch.fnmatch(filename, filter_)

    def test_get_download_s3_with_wildcard_filter_and_regex(self, tmp_path):
        filter_option = "*_200[45]*.nc"
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy-cur_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter_option}",
            "--regex",
            f"{regex}",
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command, safe_quoting=True)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 5

        for filename in downloaded_files:
            assert (
                fnmatch.fnmatch(filename, filter_option)
                or re.match(regex, filename) is not None
            )

    def test_get_download_no_files(self):
        regex = "toto"
        dataset_id = "cmems_mod_ibi_phy-cur_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--regex",
            f"{regex}",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)
        assert "No data to download" in self.output.stderr
        assert self.output.returncode == 0

    def when_i_run_copernicus_marine_command_using_no_directories_option(
        self, tmp_path, output_directory=None
    ):
        download_folder = (
            tmp_path
            if not output_directory
            else str(Path(tmp_path) / Path(output_directory))
        )

        filter_ = "*_200[12]*.nc"
        dataset_id = "cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter_}",
            "--output-directory",
            f"{download_folder}",
            "--no-directories",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0

    def then_files_are_created_without_tree_folder(
        self, tmp_path, output_directory=None
    ):
        expected_files = [
            "CMEMS_v6r1_IBI_PHY_MY_NL_01yav_temp_20010101_20011231_R20251125_RE01.nc",
            "CMEMS_v6r1_IBI_PHY_MY_NL_01yav_temp_20020101_20021231_R20251125_RE01.nc",
        ]

        download_folder = (
            Path(tmp_path)
            if not output_directory
            else Path(tmp_path) / Path(output_directory)
        )

        downloaded_files = [path.name for path in download_folder.iterdir()]

        assert set(expected_files).issubset(downloaded_files)

    def test_no_directories_option_original_files(self, tmp_path):
        self.when_i_run_copernicus_marine_command_using_no_directories_option(
            tmp_path
        )
        self.then_files_are_created_without_tree_folder(tmp_path)
        self.when_i_run_copernicus_marine_command_using_no_directories_option(
            tmp_path, output_directory="test"
        )
        self.then_files_are_created_without_tree_folder(
            tmp_path, output_directory="test"
        )

    def test_get_2023_08_original_files(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
            "--filter",
            "*/2023/08/*",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0
        assert "No data to download" not in self.output.stderr

    def test_file_list_filter(self, tmp_path):
        dataset_id = "cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.25deg_P1D"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--file-list",
            "./tests/resources/file_list_examples/file_list_example.txt",
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 2

        for filename in downloaded_files:
            assert (
                re.search(
                    (
                        r"nrt_global_allsat_phy_l4_20240101_20240107\.nc|"
                        r"nrt_global_allsat_phy_l4_20240102_20240108\.nc"
                    ),
                    filename,
                )
                is not None
            )

    def test_get_download_file_list(self, tmp_path):
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy-ssh_my_0.027deg_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--regex",
            f"{regex}",
            "--create-file-list",
            "files_to_download.txt",
            "--output-directory",
            f"{tmp_path}",
        ]

        output_filename = pathlib.Path(tmp_path) / "files_to_download.txt"

        self.output = execute_in_terminal(command, safe_quoting=True)
        print(f"Output filename: {self.output}")
        assert self.output.returncode == 0
        assert output_filename.is_file()
        with open(output_filename) as file:
            lines = file.read().splitlines()
            assert len(lines) == 3
            assert (
                "CMEMS_v6r1_IBI_PHY_MY_NL_01yav_ssh_20010101_20011231_R20251216_RE01.nc"
                in lines[0]
            )
            assert (
                "CMEMS_v6r1_IBI_PHY_MY_NL_01yav_ssh_20020101_20021231_R20251216_RE01.nc"
                in lines[1]
            )
            assert (
                "CMEMS_v6r1_IBI_PHY_MY_NL_01yav_ssh_20030101_20031231_R20251216_RE01.nc"
                in lines[2]
            )

    def test_last_modified_date_is_set_with_s3(self, tmp_path):
        command = [
            "copernicusmarine",
            "get",
            "-i",
            "METOFFICE-GLO-SST-L4-REP-OBS-SST",
            "--filter",
            "*2022053112000*",
            "--output-directory",
            f"{tmp_path}",
            "--no-directories",
        ]
        self.output = execute_in_terminal(command)
        output_file = pathlib.Path(
            tmp_path,
            "20220531120000-UKMO-L4_GHRSST-SSTfnd-OSTIA-GLOB_REP-v02.0-fv02.0.nc",
        )
        five_minutes_ago = datetime.now() - timedelta(minutes=5)

        assert self.output.returncode == 0
        assert datetime.fromtimestamp(os.path.getmtime(output_file)) < (
            five_minutes_ago
        )

    def test_get_goes_to_staging(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy-sal_my_0.027deg_P1Y-m",
            "--staging",
            "--log-level",
            "DEBUG",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert (
            "mdl-metadata-dta/dataset_product_id_mapping.json"
            in self.output.stderr
        )

    def test_get_function(self, tmp_path):
        get_result = get(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_ibi_phy-ssh_my_0.027deg_P1Y-m",
            output_directory=tmp_path,
        )
        assert get_result is not None
        assert all(result.file_path.exists() for result in get_result.files)

    @mock.patch("os.utime", side_effect=PermissionError)
    def test_permission_denied_for_modification_date(
        self, mock_utime, tmp_path, caplog
    ):
        get(
            dataset_id="METOFFICE-GLO-SST-L4-REP-OBS-SST",
            filter="*2022053112000*",
            output_directory=f"{tmp_path}",
            no_directories=True,
        )
        assert "Permission to modify the last modified date" in caplog.text
        assert "is denied" in caplog.text
        output_file = Path(
            tmp_path,
            "20220531120000-UKMO-L4_GHRSST-SSTfnd-OSTIA-GLOB_REP-v02.0-fv02.0.nc",
        )
        five_minutes_ago = datetime.now() - timedelta(minutes=5)
        assert datetime.fromtimestamp(os.path.getmtime(output_file)) > (
            five_minutes_ago
        )

    def test_static_are_correctly_printed_in_get(self):
        command = [
            "copernicusmarine",
            "get",
            "-i",
            "cmems_mod_glo_phy_anfc_0.083deg_static",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        response_get = loads(self.output.stdout)
        assert len(response_get["files"]) == 1
        assert response_get["number_of_files_to_download"] == 1
