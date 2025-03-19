import datetime
import fnmatch
import itertools
import logging
import os
import pathlib
import re
from dataclasses import dataclass
from json import loads
from pathlib import Path
from typing import List, Literal, Optional, Union

import xarray

from tests.test_utils import (
    execute_in_terminal,
    main_checks_when_file_is_downloaded,
    remove_extra_logging_prefix_info,
)

logger = logging.getLogger()


def get_all_files_in_folder_tree(folder: str) -> list[str]:
    downloaded_files = []
    for _, _, files in os.walk(folder):
        for filename in files:
            downloaded_files.append(filename)
    return downloaded_files


def get_file_size(filepath):
    file_path = Path(filepath)
    file_stats = file_path.stat()
    return file_stats.st_size


class TestCommandLineInterface:

    # -------------------------#
    # Test on subset requests #
    # -------------------------#

    @dataclass(frozen=True)
    class SubsetServiceToTest:
        name: str
        subpath: str
        dataset_url: str

    GEOSERIES = SubsetServiceToTest(
        "geoseries",
        "download_zarr",
        (
            "https://s3.waw3-1.cloudferro.com/mdl-arco-time/arco/"
            "GLOBAL_ANALYSISFORECAST_PHY_001_024/"
            "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m_202211/timeChunked.zarr"
        ),
    )
    TIMESERIES = SubsetServiceToTest(
        "timeseries",
        "download_zarr",
        (
            "https://s3.waw2-1.cloudferro.com/swift/v1/"
            "AUTH_04a16d35c9e7451fa5894a700508c003/mdl-arco-geo/"
            "arco/GLOBAL_ANALYSISFORECAST_PHY_001_024/"
            "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m_202211/geoChunked.zarr"
        ),
    )

    def flatten_request_dict(
        self, request_dict: dict[str, Optional[Union[str, Path]]]
    ) -> List:
        flatten_list = list(
            itertools.chain.from_iterable(
                [[key, val] for key, val in request_dict.items()]
            )
        )
        flatten_list = list(filter(lambda x: x is not None, flatten_list))
        return flatten_list

    def _test_subset_functionnalities(
        self, subset_service_to_test: SubsetServiceToTest, tmp_path
    ):
        self.base_request_dict = {
            "--dataset-id": "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            "--variable": "so",
            "--start-datetime": "2022-01-05",
            "--end-datetime": "2022-01-06",
            "--minimum-latitude": "0.0",
            "--maximum-latitude": "0.1",
            "--minimum-longitude": "0.2",
            "--maximum-longitude": "0.3",
            "--service": subset_service_to_test.name,
            "--output-directory": tmp_path,
        }
        self.check_default_subset_request(
            subset_service_to_test.subpath, tmp_path
        )
        self.check_subset_request_with_dataset_not_in_catalog()
        self.check_subset_request_with_no_subsetting()

    def check_default_subset_request(self, function_name, tmp_path):
        folder = pathlib.Path(tmp_path, function_name)
        if not folder.is_dir():
            pathlib.Path.mkdir(folder, parents=True)

        command = [
            "copernicusmarine",
            "subset",
        ] + self.flatten_request_dict(self.base_request_dict)

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def check_subset_request_with_dataset_not_in_catalog(self):
        self.base_request_dict["--dataset-id"] = "FAKE_ID"
        self.base_request_dict.pop("--dataset-url")

        unknown_dataset_request = [
            "copernicusmarine",
            "subset",
        ] + self.flatten_request_dict(self.base_request_dict)

        self.output = execute_in_terminal(unknown_dataset_request)
        assert (
            b"Key error: The requested dataset 'FAKE_ID' was not found in the "
            b"catalogue, you can use 'copernicusmarine describe "
            b"--include-datasets --contains <search_token>' to find datasets"
        ) in self.output.stderr

    def check_subset_request_with_no_subsetting(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            f"{dataset_id}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Missing subset option. Try 'copernicusmarine subset --help'."
            in self.output.stderr
        )
        assert (
            b"To retrieve a complete dataset, please use instead: "
            b"copernicusmarine get --dataset-id " + bytes(dataset_id, "utf-8")
        ) in self.output.stderr

    def test_retention_period_works(self, tmp_path):
        self.command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-oc_atl_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D",
            "--dataset-version",
            "202311",
            "--variable",
            "CHL",
            "--minimum-longitude",
            "-36.29005445972566",
            "--maximum-longitude",
            "-35.14832052107781",
            "--minimum-latitude",
            "47.122926204435295",
            "--maximum-latitude",
            "48.13780081656672",
            "--output-directory",
            tmp_path,
            "--output-filename",
            "dataset.nc",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        assert (
            b"time       (time) datetime64[ns] 2023" not in self.output.stderr
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(tmp_path / "dataset.nc", response)

    def test_retention_period_works_when_only_values_in_metadata(
        self, tmp_path
    ):
        self.command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-oc_atl_bgc-pp_nrt_l4-multi-1km_P1M",
            "--variable",
            "PP",
            "--minimum-longitude",
            "-36.29005445972566",
            "--maximum-longitude",
            "-35.14832052107781",
            "--minimum-latitude",
            "47.122926204435295",
            "--maximum-latitude",
            "48.13780081656672",
            "--output-directory",
            tmp_path,
            "--output-filename",
            "dataset.nc",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(self.command)
        assert self.output.returncode == 0
        assert (
            b"time       (time) datetime64[ns] 2023" not in self.output.stderr
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(tmp_path / "dataset.nc", response)

    # -------------------------#
    # Test on get requests #
    # -------------------------#

    def test_get_original_files_functionality(
        self, tmp_path
    ):  # TODO: check this test and what does it do
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--output-directory",
            f"{tmp_path}",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_get_download_s3_without_regex(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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
        assert len(downloaded_files) == 31

    def test_get_download_s3_with_regex(self, tmp_path):
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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

        self.output = execute_in_terminal(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 3

        for filename in downloaded_files:
            assert re.match(regex, filename) is not None

    def test_get_something_and_skip_existing(self, tmp_path):
        self.when_get_by_default_returns_status_message(tmp_path)
        self.and_I_do_skip_existing(tmp_path)

    def when_get_by_default_returns_status_message(self, tmp_path):
        filter_option = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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

    def and_I_do_skip_existing(self, tmp_path):
        filter_option = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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
            f"cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m_202211/"
            f"CMEMS_v5r1_IBI_PHY_MY_NL_01yav_"
        )
        assert os.path.exists(
            start_path + "20010101_20011231_R20221101_RE01.nc"
        )
        assert not os.path.exists(
            start_path + "20010101_20011231_R20221101_RE01_(1).nc"
        )
        assert returned_value["total_size"] == 0

    def test_get_download_with_dry_run_option(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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
        filter = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter}",
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
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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
            b"Some ``--response-fields`` fields are invalid:"
            b" wrong_field" in self.output.stderr
        )

        command[-1] = "wrong_field1, wrong_field2"
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Wrong fields error: All ``--response-fields`` "
            b"fields are invalid: wrong_field1, wrong_field2"
            in self.output.stderr
        )

    def test_subset_wrong_input_response_fields_warning_and_error(self):
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
        response_fields = "status, wrong_field"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--dry-run",
            "-r",
            response_fields,
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            b"Some ``--response-fields`` fields are invalid:"
            b" wrong_field" in self.output.stderr
        )

        command[-1] = "wrong_field1, wrong_field2"
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Wrong fields error: All ``--response-fields`` "
            b"fields are invalid: wrong_field1, wrong_field2"
            in self.output.stderr
        )

    def test_subset_with_dry_run_option(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--dry-run",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert str(tmp_path) in returned_value["file_path"]
        assert not os.path.exists(returned_value["file_path"])

    def test_subset_by_default_returns_status_message(
        self, tmp_path
    ):  # TODO: it feels like we can just add this test into another one!
        # so, do we need to download a whole dataset for this?
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "-o",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert returned_value["status"]
        assert returned_value["message"]
        assert "file_path" not in returned_value
        assert "coordinates_extent" not in returned_value

    def test_subset_can_choose_return_fields(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "-o",
            f"{tmp_path}",
            "--dry-run",
            "-r",
            "file_path",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        returned_value = loads(self.output.stdout)
        assert "status" not in returned_value
        assert "message" not in returned_value
        assert "file_path" in returned_value
        assert "coordinates_extent" not in returned_value

    def test_subset_output_file_as_netcdf(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        output_filename = "test_subset_output_file_as_netcdf.nc"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "34.2",
            "--minimum-depth",
            "0.5",
            "--maximum-depth",
            "1.6",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--service",
            f"{self.GEOSERIES.name}",
        ]

        self.output = execute_in_terminal(command)
        is_file = pathlib.Path(tmp_path, output_filename).is_file()
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )
        assert self.output.returncode == 0
        assert is_file

    def test_get_download_s3_with_wildcard_filter(self, tmp_path):
        filter = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter}",
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 3

        for filename in downloaded_files:
            assert fnmatch.fnmatch(filename, filter)

    def test_get_download_s3_with_wildcard_filter_and_regex(self, tmp_path):
        filter_option = "*_200[45]*.nc"
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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

        self.output = execute_in_terminal(command)
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
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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
        assert b"No data to download" in self.output.stderr
        assert self.output.returncode == 0

    # TODO: separate tests for each service
    # SUBSET, GET, DESCRIBE
    def test_subset_error_when_forced_service_does_not_exist(self):
        self.when_I_run_copernicus_marine_subset_forcing_a_service_not_available()
        self.then_I_got_a_clear_output_with_available_service_for_subset()

    def when_I_run_copernicus_marine_subset_forcing_a_service_not_available(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1D-m",
            "--variable",
            "thetao",
            "--service",
            "unavailable-service",
        ]

        self.output = execute_in_terminal(command)

    def then_I_got_a_clear_output_with_available_service_for_subset(self):
        assert (
            b"Service unavailable-service does not exist for command subset. "
            b"Possible services: ['arco-geo-series', 'geoseries', "
            b"'arco-time-series', 'timeseries', 'omi-arco', 'static-arco', "
            b"'arco-platform-series', 'platformseries']"
        ) in self.output.stderr

    def when_I_request_subset_dataset_with_zarr_service(
        self,
        tmp_path,
        vertical_axis: Literal["depth", "elevation"] = "depth",
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "-t",
            "2023-05-10",
            "-T",
            "2023-05-12",
            "-x",
            "-18",
            "-X",
            "-10",
            "-y",
            "35",
            "-Y",
            "40",
            "-z",
            "1",
            "-Z",
            "10",
            "-v",
            "thetao",
            "--vertical-axis",
            f"{vertical_axis}",
            "--service",
            "arco-time-series",
            "-o",
            f"{tmp_path}",
            "-f",
            "data.zarr",
        ]

        self.output = execute_in_terminal(command)

    def then_I_have_correct_sign_for_depth_coordinates_values(
        self, output_path, sign
    ):
        filepath = pathlib.Path(output_path, "data.zarr")
        dataset = xarray.open_dataset(filepath, engine="zarr")

        assert self.output.returncode == 0
        if sign == "positive":
            assert dataset.depth.min() <= 10
            assert dataset.depth.max() >= 0
        elif sign == "negative":
            assert dataset.elevation.min() >= -10
            assert dataset.elevation.max() <= 0

    def then_I_have_correct_attribute_value(
        self, output_path, dimention_name, attribute_value
    ):
        filepath = pathlib.Path(output_path, "data.zarr")
        dataset = xarray.open_dataset(filepath, engine="zarr")
        assert dataset[dimention_name].attrs["standard_name"] == dimention_name
        assert dataset[dimention_name].attrs["positive"] == attribute_value

    def test_conversion_between_elevation_and_depth(self, tmp_path):
        self.when_I_request_subset_dataset_with_zarr_service(tmp_path, "depth")
        self.then_I_have_correct_sign_for_depth_coordinates_values(
            tmp_path, "positive"
        )
        self.then_I_have_correct_attribute_value(tmp_path, "depth", "down")

    def test_force_no_conversion_between_elevation_and_depth(self, tmp_path):
        self.when_I_request_subset_dataset_with_zarr_service(
            tmp_path, "elevation"
        )
        self.then_I_have_correct_sign_for_depth_coordinates_values(
            tmp_path, "negative"
        )
        self.then_I_have_correct_attribute_value(tmp_path, "elevation", "up")

    def when_I_run_copernicus_marine_command_using_no_directories_option(
        self, tmp_path, output_directory=None
    ):
        download_folder = (
            tmp_path
            if not output_directory
            else str(Path(tmp_path) / Path(output_directory))
        )

        filter = "*_200[12]*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--filter",
            f"{filter}",
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
            "CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc",
            "CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc",
        ]

        download_folder = (
            Path(tmp_path)
            if not output_directory
            else Path(tmp_path) / Path(output_directory)
        )

        downloaded_files = list(
            map(lambda path: path.name, download_folder.iterdir())
        )

        assert set(expected_files).issubset(downloaded_files)

    def test_no_directories_option_original_files(self, tmp_path):
        self.when_I_run_copernicus_marine_command_using_no_directories_option(
            tmp_path
        )
        self.then_files_are_created_without_tree_folder(tmp_path)
        self.when_I_run_copernicus_marine_command_using_no_directories_option(
            tmp_path, output_directory="test"
        )
        self.then_files_are_created_without_tree_folder(
            tmp_path, output_directory="test"
        )

    def test_default_service_for_subset_command(self):
        self.when_I_run_copernicus_marine_subset_with_default_service()
        self.then_I_can_see_the_arco_geo_series_service_is_choosen()

    def when_I_run_copernicus_marine_subset_with_default_service(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1M-m",
            "--variable",
            "thetao",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)

    def then_I_can_see_the_arco_geo_series_service_is_choosen(self):
        assert b'Selected service: "arco-geo-series"' in self.output.stderr

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
        assert b"No data to download" not in self.output.stderr

    def test_subset_with_chunking(
        self, tmp_path
    ):  # TODO: it says subset with chunking but looks kind of 'normal'
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
            "-t",
            "2024-01-01T00:00:00",
            "-T",
            "2024-01-05T23:59:59",
            "-v",
            "uo",
            "-x",
            "0",
            "-X",
            "180",
            "-y",
            "-80",
            "-Y",
            "90",
            "-z",
            "0.49",
            "-Z",
            "8",
            "-o",
            f"{tmp_path}",
            "-f",
            "output.nc",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(tmp_path / "output.nc", response)

    def test_short_option_for_copernicus_marine_command_helper(self):
        short_option_command = [
            "copernicusmarine",
            "-h",
        ]
        long_option_command = [
            "copernicusmarine",
            "--help",
        ]

        self.short_option_output = execute_in_terminal(short_option_command)
        self.long_option_output = execute_in_terminal(long_option_command)

        assert (
            self.short_option_output.stderr == self.long_option_output.stderr
        )

    def test_short_option_for_copernicus_marine_subcommand_helper(self):
        short_option_command = [
            "copernicusmarine",
            "subset",
            "-h",
        ]
        long_option_command = [
            "copernicusmarine",
            "subset",
            "--help",
        ]

        self.short_option_output = execute_in_terminal(short_option_command)
        self.long_option_output = execute_in_terminal(long_option_command)

        assert (
            self.short_option_output.stderr == self.long_option_output.stderr
        )

    def test_subset_create_template(self):
        self.when_created_is_created()
        self.and_it_runs_correctly()

    def when_created_is_created(self):
        command = ["copernicusmarine", "subset", "--create-template"]

        self.output = execute_in_terminal(command)

        assert (
            b"Template created at: subset_template.json"
            == remove_extra_logging_prefix_info(self.output.stderr)
        )
        assert Path("subset_template.json").is_file()

    def and_it_runs_correctly(self):
        command = [
            "copernicusmarine",
            "subset",
            "--request-file",
            "./subset_template.json",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 0

    def test_get_template_creation(self):
        command = ["copernicusmarine", "get", "--create-template"]

        self.output = execute_in_terminal(command)

        assert (
            b"Template created at: get_template.json"
            == remove_extra_logging_prefix_info(self.output.stderr)
        )
        assert Path("get_template.json").is_file()

    def test_get_template_creation_with_extra_arguments(self):
        command = [
            "copernicusmarine",
            "get",
            "--create-template",
            "--no-directories",
        ]

        self.output = execute_in_terminal(command)

        assert (
            b"Other options passed with create template: no_directories"
            == remove_extra_logging_prefix_info(self.output.stderr)
        )

    def test_error_log_for_variable_that_does_not_exist(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            "-v",
            "theta",
        ]

        self.output = execute_in_terminal(command)

        assert (
            b"The variable 'theta' is neither a "
            b"variable or a standard name in the dataset" in self.output.stderr
        )

    def test_error_log_for_service_that_does_not_exist(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            "-t",
            "2023-01-01",
            "-T",
            "2023-01-03",
            "--service",
            "ft",
        ]

        self.output = execute_in_terminal(command)

        assert (
            b"Service ft does not exist for command subset"
            in self.output.stderr
        )

    def then_I_can_read_copernicusmarine_version_in_the_dataset_attributes(
        self, filepath
    ):
        dataset = xarray.open_dataset(filepath)
        assert "copernicusmarine_version" in dataset.attrs

    def test_copernicusmarine_version_in_dataset_attributes_with_arco(
        self, tmp_path
    ):
        self.when_I_request_subset_dataset_with_zarr_service(tmp_path)
        self.then_I_can_read_copernicusmarine_version_in_the_dataset_attributes(
            tmp_path / "data.zarr"
        )

    def test_subset_filter_by_standard_name(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        output_filename = "data.zarr"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "sea_water_potential_temperature",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "34.2",
            "--minimum-depth",
            "0.5",
            "--maximum-depth",
            "1.6",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--service",
            f"{self.GEOSERIES.name}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            "thetao"
            in xarray.open_zarr(f"{tmp_path}/{output_filename}").variables
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )

    def test_log_level_debug(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        output_filename = "data.zarr"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "sea_water_potential_temperature",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "34.2",
            "--minimum-depth",
            "0.5",
            "--maximum-depth",
            "1.6",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--service",
            f"{self.GEOSERIES.name}",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert b"DEBUG - " in self.output.stderr
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )

    def test_arco_subset_is_fast(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Y",
            "43.1",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "-o",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command, timeout_second=10)
        assert self.output.returncode == 0, self.output.stderr

    def test_name_dataset_with_subset_parameters(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Z",
            "100",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "--file-format",
            "zarr",
            "-o",
            f"{tmp_path}",
        ]
        expected_dataset_id = "med-cmcc-cur-rean-h"
        expected_variables = "uo-vo"
        expected_longitude = "3.08E-3.17E"
        expected_latitude = "42.94N-45.98N"
        expected_datetime = "1993-01-01-1993-01-31"
        expected_extension = ".zarr"
        expected_filename = (
            expected_dataset_id
            + "_"
            + expected_variables
            + "_"
            + expected_longitude
            + "_"
            + expected_latitude
            + "_"
            + expected_datetime
            + expected_extension
        )
        expected_filepath = Path(tmp_path, expected_filename)
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert expected_filepath.is_dir()

    def then_I_can_read_dataset_size_in_the_response(self):
        response_subset = loads(self.output.stdout)
        assert "file_size" in response_subset
        assert "data_transfer_size" in response_subset
        assert response_subset["file_size"] > 0
        assert int(response_subset["data_transfer_size"]) == 50

    def test_dataset_size_is_displayed_when_downloading_with_arco_service(
        self, tmp_path
    ):
        self.when_I_request_subset_dataset_with_zarr_service(tmp_path)
        self.then_I_can_read_dataset_size_in_the_response()

    def test_dataset_has_always_every_dimensions(self, tmp_path):
        output_filename = "data.nc"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i",
            "-v",
            "uo",
            "-v",
            "vo",
            "-x",
            "-12",
            "-X",
            "-12",
            "-y",
            "30",
            "-Y",
            "30",
            "-t",
            "2023-11-20 00:00:00",
            "-T",
            "2023-11-20 00:00:00",
            "-z",
            "0.5",
            "-Z",
            "0.5",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            len(
                xarray.open_dataset(
                    Path(tmp_path) / output_filename
                ).sizes.keys()
            )
            == 4
        )
        response = loads(self.output.stdout)
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, response
        )

    def test_netcdf_compression_option(self, tmp_path):
        filename_without_option = "without_option.nc"
        filename_with_option = "with_option.nc"
        filename_zarr_without_option = "filename_without_option.zarr"
        filename_zarr_with_option = "filename_with_option.zarr"

        netcdf_compression_option = "--netcdf-compression-level"

        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Y",
            "43.1",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "-o",
            f"{tmp_path}",
        ]

        output_without_option = execute_in_terminal(
            base_command + ["-f", filename_without_option]
        )
        output_with_option = execute_in_terminal(
            base_command
            + ["-f", filename_with_option, netcdf_compression_option]
        )
        output_zarr_without_option = execute_in_terminal(
            base_command + ["-f", filename_zarr_without_option]
        )
        output_zarr_with_option = execute_in_terminal(
            base_command
            + ["-f", filename_zarr_with_option, netcdf_compression_option]
        )

        assert output_without_option.returncode == 0
        assert output_with_option.returncode == 0
        assert output_zarr_without_option.returncode == 0
        assert output_zarr_with_option.returncode != 0

        filepath_without_option = Path(tmp_path / filename_without_option)
        filepath_with_option = Path(tmp_path / filename_with_option)

        size_without_option = get_file_size(filepath_without_option)
        size_with_option = get_file_size(filepath_with_option)
        logger.info(f"{size_without_option=}, {size_with_option=}")
        assert size_with_option < size_without_option

        dataset_without_option = xarray.open_dataset(filepath_without_option)
        dataset_with_option = xarray.open_dataset(filepath_with_option)
        logger.info(
            f"{dataset_without_option.uo.encoding=}, {dataset_with_option.uo.encoding=}"
        )
        assert dataset_without_option.uo.encoding["zlib"] is False
        assert dataset_without_option.uo.encoding["complevel"] == 0

        assert dataset_with_option.uo.encoding["zlib"] is True
        assert dataset_with_option.uo.encoding["complevel"] == 1
        assert dataset_with_option.uo.encoding["contiguous"] is False
        assert dataset_with_option.uo.encoding["shuffle"] is True

    def test_netcdf_compression_with_optimised_files(self, tmp_path):
        filename_without_option = "without_option.nc"
        filename_with_option = "with_option.nc"

        netcdf_compression_option = "--netcdf-compression-level"

        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "-35",
            "-Y",
            "-30",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1995-01-31T00:00:00",
            "-v",
            "thetao",
            "-o",
            f"{tmp_path}",
        ]

        output_without_option = execute_in_terminal(
            base_command + ["-f", filename_without_option]
        )
        output_with_option = execute_in_terminal(
            base_command
            + ["-f", filename_with_option, netcdf_compression_option]
        )

        assert output_without_option.returncode == 0
        assert output_with_option.returncode == 0

        filepath_without_option = Path(tmp_path / filename_without_option)
        filepath_with_option = Path(tmp_path / filename_with_option)

        size_without_option = get_file_size(filepath_without_option)
        size_with_option = get_file_size(filepath_with_option)
        logger.info(f"{size_without_option=}, {size_with_option=}")
        assert 1.6 * size_with_option < size_without_option

    def test_omi_arco_service(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "blksea_omi_circulation_rim_current_index",
            "-v",
            "BSRCI",
            "-o",
            f"{tmp_path}",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(base_command)
        assert self.output.returncode == 0
        assert b'Selected service: "omi-arco"' in self.output.stderr

        self.output = execute_in_terminal(base_command + ["-s", "omi-arco"])
        assert self.output.returncode == 0
        assert b'Selected service: "omi-arco"' in self.output.stderr

    def test_static_arco_service(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_blk_phy_anfc_2.5km_static",
            "-v",
            "deptho",
            "--dataset-part",
            "bathy",
            "-o",
            f"{tmp_path}",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(base_command)
        assert self.output.returncode == 0
        assert b'Selected service: "static-arco"' in self.output.stderr

        self.output = execute_in_terminal(base_command + ["-s", "static-arco"])
        assert self.output.returncode == 0
        assert b'Selected service: "static-arco"' in self.output.stderr

    def test_subset_dataset_part_option(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_blk_phy_anfc_2.5km_static",
            "-v",
            "deptho",
            "-o",
            f"{tmp_path}",
            "--dry-run",
        ]

        self.output = execute_in_terminal(
            base_command + ["--dataset-part", "bathy"]
        )
        assert self.output.returncode == 0

    def test_netcdf_compression_level(self, tmp_path):
        forced_comp_level = 4

        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-cmcc-cur-rean-h",
            "-x",
            "3.08",
            "-X",
            "3.17",
            "-y",
            "42.9",
            "-Y",
            "43.1",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-31T00:00:00",
            "-v",
            "uo",
            "-v",
            "vo",
            "-o",
            f"{tmp_path}",
            "-f",
            "data.nc",
            "--netcdf-compression-level",
            f"{forced_comp_level}",
        ]

        output_with_netcdf_compression_enabled = execute_in_terminal(
            base_command
        )

        assert output_with_netcdf_compression_enabled.returncode == 0

        filepath = Path(tmp_path / "data.nc")
        dataset = xarray.open_dataset(filepath)
        logger.info(f"{dataset.uo.encoding=}, {dataset.uo.encoding=}")

        assert dataset.uo.encoding["zlib"] is True
        assert dataset.uo.encoding["complevel"] == forced_comp_level
        assert dataset.uo.encoding["contiguous"] is False
        assert dataset.uo.encoding["shuffle"] is True

    def test_subset_approximation_of_big_data_that_needs_to_be_downloaded(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-v",
            "thetao_oras",
            "-v",
            "uo_oras",
            "-v",
            "vo_oras",
            "-v",
            "so_oras",
            "-v",
            "zos_oras",
            "-x",
            "50",
            "-X",
            "110",
            "-y",
            "-10.0",
            "-Y",
            "30.0",
            "-t",
            "2010-03-01T00:00:00",
            "-T",
            "2010-06-30T00:00:00",
            "-z",
            "0.5057600140571594",
            "-Z",
            "500",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        response_subset = loads(self.output.stdout)
        assert int(response_subset["data_transfer_size"]) == 71691

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
                        r"nrt_global_allsat_phy_l4_20220119_20220125\.nc|"
                        r"nrt_global_allsat_phy_l4_20220120_20220126\.nc"
                    ),
                    filename,
                )
                is not None
            )

    def test_get_download_file_list(self, tmp_path):
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
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

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert output_filename.is_file()
        with open(output_filename) as file:
            lines = file.read().splitlines()
            assert len(lines) == 3
            assert (
                "CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20010101_20011231_R20221101_RE01.nc"
                in lines[0]
            )
            assert (
                "CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20020101_20021231_R20221101_RE01.nc"
                in lines[1]
            )
            assert (
                "CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20030101_20031231_R20221101_RE01.nc"
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
        five_minutes_ago = datetime.datetime.now() - datetime.timedelta(
            minutes=5
        )

        assert self.output.returncode == 0
        assert datetime.datetime.fromtimestamp(
            os.path.getmtime(output_file)
        ) < (five_minutes_ago)

    def test_netcdf3_option(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_PT6H-i",
            "-v",
            "thetao",
            "-t",
            "2022-01-01T00:00:00",
            "-T",
            "2022-12-31T23:59:59",
            "-x",
            "-6.17",
            "-X",
            "-5.08",
            "-y",
            "35.75",
            "-Y",
            "36.30",
            "-z",
            "0.0",
            "-Z",
            "5.0",
            "-f",
            "dataset.nc",
            "-o",
            f"{tmp_path}",
            "--netcdf3-compatible",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        output_netcdf_format = execute_in_terminal(
            ["ncdump", "-k", f"{tmp_path / 'dataset.nc'}"]
        )
        assert output_netcdf_format.returncode == 0
        assert output_netcdf_format.stdout == b"classic\n"

    def test_that_requested_interval_fully_included_with_coords_sel_method_outside(
        self, tmp_path
    ):
        output_filename = "output.nc"
        min_longitude = 0.01
        max_longitude = 1.55
        min_latitude = 0.01
        max_latitude = 1.1
        min_depth = 30.5
        max_depth = 50.0
        start_datetime = "2023-12-01T01:00:00"
        end_datetime = "2023-12-12T01:00:00"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--coordinates-selection-method",
            "outside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))
        assert dataset.longitude.values.min() <= min_longitude
        assert dataset.longitude.values.max() >= max_longitude
        assert dataset.latitude.values.min() <= min_latitude
        assert dataset.latitude.values.max() >= max_latitude
        assert dataset.depth.values.min() <= min_depth
        assert dataset.depth.values.max() >= max_depth
        assert datetime.datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
        assert datetime.datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")

    def test_that_requested_interval_is_correct_with_coords_sel_method_inside(
        self, tmp_path
    ):
        output_filename = "output.nc"
        min_longitude = 0.01
        max_longitude = 1.567
        min_latitude = 0.013
        max_latitude = 1.123
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-12-01T01:00:23"
        end_datetime = "2023-12-12T01:10:03"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--coordinates-selection-method",
            "inside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))
        assert dataset.longitude.values.min() >= min_longitude
        assert dataset.longitude.values.max() <= max_longitude
        assert dataset.latitude.values.min() >= min_latitude
        assert dataset.latitude.values.max() <= max_latitude
        assert dataset.depth.values.min() >= min_depth
        assert dataset.depth.values.max() <= max_depth
        assert datetime.datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
        assert datetime.datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S")

    def test_that_requested_interval_is_correct_with_coords_sel_method_nearest(
        self, tmp_path
    ):
        output_filename = "output.nc"
        min_longitude = 0.08
        max_longitude = 1.567
        min_latitude = 0.013
        max_latitude = 1.123
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-01-01T00:00:00"
        end_datetime = "2023-01-03T23:04:00"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--coordinates-selection-method",
            "nearest",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))

        assert dataset.longitude.values.min() == 0.083343505859375
        assert dataset.longitude.max().values == 1.583343505859375
        assert dataset.latitude.values.min() == 0.0
        assert dataset.latitude.values.max() == 1.0833358764648438
        assert dataset.depth.values.min() == 29.444730758666992
        assert dataset.depth.values.max() == 47.37369155883789
        assert datetime.datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) == datetime.datetime.strptime("2023-01-01", "%Y-%m-%d")
        assert datetime.datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) == datetime.datetime.strptime("2023-01-04", "%Y-%m-%d")

    def test_coordinates_selection_method_outside_w_elevation(self, tmp_path):
        """dataset characteristics:
        * depth      (depth) float32 500B 1.018 3.166 5.465 ... 4.062e+03 4.153e+03
        * latitude   (latitude) float32 2kB 30.19 30.23 30.27 ... 45.9 45.94 45.98
        * longitude  (longitude) float32 4kB -5.542 -5.5 -5.458 ... 36.21 36.25 36.29
        * time       (time) datetime64[ns] 14kB 2020-01-01 2020-01-02 ... 2024-09-13
        """
        output_filename = "output.nc"
        min_longitude = -6
        max_longitude = -5
        min_latitude = 40
        max_latitude = 50
        min_depth = 1.1
        max_depth = 2.3
        start_datetime = "2023-01-01T00:00:00"
        end_datetime = "2023-01-03T23:04:00"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_med_bgc-bio_anfc_4.2km_P1D-m",
            "--variable",
            "nppv",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--coordinates-selection-method",
            "outside",
            "--vertical-axis",
            "elevation",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        dataset = xarray.open_dataset(Path(tmp_path, output_filename))

        assert dataset.longitude.values.min() <= -5.5416  # dataset limit
        assert dataset.longitude.max().values >= -5.0  # our limit
        assert dataset.latitude.values.min() <= 40  # our limit
        assert dataset.latitude.values.max() >= 45.9791  # dataset limit
        assert dataset.elevation.values.max() >= -1.01823665  # dataset limit
        assert dataset.elevation.values.min() <= -2.3  # our limit
        assert datetime.datetime.strptime(
            str(dataset.time.values.min()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) <= datetime.datetime.strptime("2023-01-01", "%Y-%m-%d")
        assert datetime.datetime.strptime(
            str(dataset.time.values.max()), "%Y-%m-%dT%H:%M:%S.000%f"
        ) >= datetime.datetime.strptime("2023-01-03", "%Y-%m-%d")

    def test_get_goes_to_staging(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--staging",
            "--log-level",
            "DEBUG",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert (
            b"mdl-metadata-dta/dataset_product_id_mapping.json"
            in self.output.stderr
        )

    def test_subset_goes_to_staging(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--staging",
            "--log-level",
            "DEBUG",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert (
            b"mdl-metadata-dta/dataset_product_id_mapping.json"
            in self.output.stderr
        )

    def test_subset_optimise_chunks(self, tmp_path):
        """
        This command can take several minutes or even end up in a memory issue
        because of the dask graph

        copernicusmarine subset -i cmems_mod_glo_phy_my_0.083deg_P1D-m -t "2013-08-01" -T "2013-08-01" -x 113.896034  -y -11.045679 -Y -6.366948 -z 0 -Z 5000
        """  # noqa
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
            "-t",
            "2013-08-01",
            "-T",
            "2013-08-01",
            "-x",
            "113.896034",
            "-y",
            "-11.045679",
            "-Y",
            "-6.366948",
            "-z",
            "0",
            "-Z",
            "5000",
            "--output-directory",
            f"{tmp_path}",
        ]
        self.output = execute_in_terminal(command, timeout_second=60)
        assert self.output.returncode == 0

    def test_that_requested_interval_is_correct_w_weird_windowing(
        self, tmp_path
    ):
        output_filename = "output.nc"
        min_longitude = -180.001
        max_longitude = -178.001
        min_latitude = 34.001
        max_latitude = 37.001
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-12-01T01:00:23"
        end_datetime = "2023-12-01T01:10:03"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--coordinates-selection-method",
            "inside",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--dry-run",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        response = loads(output.stdout)
        assert response["coordinates_extent"][0]["minimum"] >= min_longitude
        assert (
            response["coordinates_extent"][0]["maximum"] <= max_longitude + 360
        )
        assert response["coordinates_extent"][1]["minimum"] >= min_latitude
        assert response["coordinates_extent"][1]["maximum"] <= max_latitude
        assert response["coordinates_extent"][3]["minimum"] >= min_depth
        assert response["coordinates_extent"][3]["maximum"] <= max_depth

    def test_nearest_works_correctly_when_moving_windows(self, tmp_path):
        output_filename = "output.nc"
        min_longitude = 179.92
        max_longitude = 181.999
        min_latitude = 34.001
        max_latitude = 37.001
        min_depth = 30.554
        max_depth = 50.023
        start_datetime = "2023-12-01T01:00:23"
        end_datetime = "2023-12-01T01:10:03"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--minimum-latitude",
            f"{min_latitude}",
            "--maximum-latitude",
            f"{max_latitude}",
            "--start-datetime",
            f"{start_datetime}",
            "--end-datetime",
            f"{end_datetime}",
            "--minimum-depth",
            f"{min_depth}",
            "--maximum-depth",
            f"{max_depth}",
            "--coordinates-selection-method",
            "nearest",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--dry-run",
        ]
        output = execute_in_terminal(command)
        assert output.returncode == 0

        response = loads(output.stdout)
        assert response["coordinates_extent"][0]["minimum"] <= min_longitude
        assert response["coordinates_extent"][0]["maximum"] >= max_longitude
        assert response["coordinates_extent"][1]["minimum"] <= min_latitude
        assert response["coordinates_extent"][1]["maximum"] <= max_latitude
        assert response["coordinates_extent"][3]["minimum"] <= min_depth
        assert response["coordinates_extent"][3]["maximum"] <= max_depth
