import datetime
import fnmatch
import itertools
import logging
import os
import pathlib
import re
import shutil
import subprocess
from dataclasses import dataclass
from json import loads
from pathlib import Path
from typing import List, Optional, Union

import xarray

from copernicusmarine.catalogue_parser.catalogue_parser import (
    PART_DEFAULT,
    REGEX_PATTERN_DATE_YYYYMM,
    VERSION_DEFAULT,
    CopernicusMarineDatasetServiceType,
)
from tests.test_utils import (
    execute_in_terminal,
    remove_extra_logging_prefix_info,
)

logger = logging.getLogger()

DOWNLOAD_TEST_FOLDER = "tests/downloads"


def get_all_files_in_folder_tree(folder: str) -> list[str]:
    downloaded_files = []
    for _, _, files in os.walk(folder):
        for filename in files:
            downloaded_files.append(filename)
    return downloaded_files


def get_environment_without_crendentials():
    environment_without_crendentials = os.environ.copy()
    environment_without_crendentials.pop(
        "COPERNICUS_MARINE_SERVICE_USERNAME", None
    )
    environment_without_crendentials.pop(
        "COPERNICUS_MARINE_SERVICE_PASSWORD", None
    )
    return environment_without_crendentials


def get_file_size(filepath):
    file_path = Path(filepath)
    file_stats = file_path.stat()
    return file_stats.st_size


class TestCommandLineInterface:
    def test_describe_overwrite_metadata_cache(self):
        self.when_I_run_copernicus_marine_describe_with_overwrite_cache()
        self.then_stdout_can_be_load_as_json()

    def test_describe_default(self):
        self.when_I_run_copernicus_marine_describe_with_default_arguments()
        self.then_I_can_read_the_default_json()
        self.and_there_are_no_warnings_about_backend_versions()

    def test_describe_including_datasets(self):
        self.when_I_run_copernicus_marine_describe_including_datasets()
        self.then_I_can_read_it_does_not_contain_weird_symbols()
        self.then_I_can_read_the_json_including_datasets()
        self.then_omi_services_are_not_in_the_catalog()
        self.then_products_from_marine_data_store_catalog_are_available()
        self.then_all_dataset_parts_are_filled()

    def test_describe_contains_option(self):
        self.when_I_run_copernicus_marine_describe_with_contains_option()
        self.then_I_can_read_the_filtered_json()

    def test_describe_with_staging_flag(self):
        self.when_I_use_staging_environment_in_debug_logging_level()
        self.then_I_check_that_the_urls_contains_only_dta()

    def when_I_run_copernicus_marine_describe_with_overwrite_cache(self):
        command = [
            "copernicusmarine",
            "describe",
            "--overwrite-metadata-cache",
        ]
        self.output = subprocess.run(command, capture_output=True)

    def when_I_run_copernicus_marine_describe_with_default_arguments(self):
        command = ["copernicusmarine", "describe"]
        self.output = subprocess.run(command, capture_output=True)

    def then_stdout_can_be_load_as_json(self):
        loads(self.output.stdout.decode("utf-8"))

    def then_I_can_read_the_default_json(self):
        json_result = loads(self.output.stdout.decode("utf-8"))
        assert len(json_result["products"]) >= 270
        for product in json_result["products"]:
            assert product["title"] is not None
            assert product["product_id"] is not None
            assert product["thumbnail_url"] is not None
            assert "digital_object_identifier" in product
            assert product["sources"] is not None
            assert "processing_level" in product
            assert product["production_center"] is not None

    def and_there_are_no_warnings_about_backend_versions(self):
        assert (
            b"Please update to the latest client version."
            not in self.output.stdout
        )

    def then_omi_services_are_not_in_the_catalog(self):
        json_result = loads(self.output)
        for product in json_result["products"]:
            for dataset in product["datasets"]:
                for version in dataset["versions"]:
                    for part in version["parts"]:
                        assert "omi" not in list(
                            map(
                                lambda x: x["service_type"]["service_name"],
                                part["services"],
                            )
                        )

    def then_products_from_marine_data_store_catalog_are_available(self):
        expected_product_id = "NWSHELF_ANALYSISFORECAST_PHY_004_013"
        expected_dataset_id = "cmems_mod_nws_phy_anfc_0.027deg-2D_PT15M-i"
        expected_services = [
            "original-files",
            "arco-geo-series",
            "arco-time-series",
        ]

        json_result = loads(self.output)
        expected_product = list(
            filter(
                lambda product: product["product_id"] == expected_product_id,
                json_result["products"],
            )
        )
        assert len(expected_product) == 1
        product = expected_product[0]
        product_datasets = product["datasets"]
        expected_dataset = list(
            filter(
                lambda product: product["dataset_id"] == expected_dataset_id,
                product_datasets,
            )
        )
        assert len(expected_dataset) == 1
        dataset = expected_dataset[0]
        expected_dataset_services = list(
            map(
                lambda x: x["service_type"]["service_name"],
                dataset["versions"][0]["parts"][0]["services"],
            )
        )
        assert all(
            map(lambda x: x in expected_services, expected_dataset_services)
        )

    def then_all_dataset_parts_are_filled(self):
        expected_product_id = "BALTICSEA_ANALYSISFORECAST_BGC_003_007"
        expected_dataset_id = "cmems_mod_bal_bgc_anfc_static"

        json_result = loads(self.output)
        expected_product = list(
            filter(
                lambda product: product["product_id"] == expected_product_id,
                json_result["products"],
            )
        )
        assert len(expected_product) == 1
        product = expected_product[0]

        expected_dataset = list(
            filter(
                lambda product: product["dataset_id"] == expected_dataset_id,
                product["datasets"],
            )
        )

        assert len(expected_dataset) == 1
        dataset = expected_dataset[0]

        for version in dataset["versions"]:
            non_default_parts = list(
                filter(
                    lambda part: part["name"] != PART_DEFAULT, version["parts"]
                )
            )

            assert len(non_default_parts) > 0

        version_ordered = sorted(
            dataset["versions"],
            key=lambda x: (
                x["label"] if x["label"] != VERSION_DEFAULT else "110001"
            ),
            reverse=True,
        )

        latest_version = version_ordered[0]
        maybe_default_part = list(
            filter(
                lambda part: part["name"] == PART_DEFAULT,
                latest_version["parts"],
            )
        )
        assert len(maybe_default_part) == 0

    def when_I_run_copernicus_marine_describe_with_contains_option(self):
        filter_token = "OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_n"
        command = f"copernicusmarine describe --contains {filter_token}"
        self.output = subprocess.check_output(command, shell=True)

    def then_I_can_read_the_filtered_json(self):
        json_result = loads(self.output)
        assert len(json_result["products"]) == 2
        assert (
            json_result["products"][0]["product_id"]
            == "OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_nag_area_mean"
        )
        assert json_result["products"][0]["production_center"] == "PML (UK)"
        assert (
            json_result["products"][0]["thumbnail_url"]
            == "https://catalogue.marine.copernicus.eu/documents/IMG/OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_nag_area_mean.png"  # noqa
        )
        assert (
            json_result["products"][0]["title"]
            == "North Atlantic Gyre Area Chlorophyll-a time series and trend from Observations Reprocessing"  # noqa
        )

        assert (
            json_result["products"][1]["product_id"]
            == "OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_npg_area_mean"
        )
        assert json_result["products"][1]["production_center"] == "PML (UK)"
        assert (
            json_result["products"][1]["thumbnail_url"]
            == "https://catalogue.marine.copernicus.eu/documents/IMG/OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_npg_area_mean.png"  # noqa
        )
        assert (
            json_result["products"][1]["title"]
            == "North Pacific Gyre Area Chlorophyll-a time series and trend from Observations Reprocessing"  # noqa
        )

    def when_I_run_copernicus_marine_describe_including_datasets(self):
        command = "copernicusmarine describe --include-datasets"
        self.output = subprocess.check_output(command, shell=True)

    def then_I_can_read_it_does_not_contain_weird_symbols(self):
        assert b"__" not in self.output
        assert b" _" not in self.output
        # assert b"_ " not in self.output
        assert b'"_' not in self.output
        assert b'_"' not in self.output

    def then_I_can_read_the_json_including_datasets(self):
        json_result = loads(self.output)
        assert len(json_result["products"]) >= 270
        for product in json_result["products"]:
            assert product["title"] is not None
            assert product["product_id"] is not None
            assert product["thumbnail_url"] is not None
            assert "digital_object_identifier" in product
            assert product["sources"] is not None
            assert "processing_level" in product
            assert product["production_center"] is not None
            assert "datasets" in product
            assert product[
                "datasets"
            ], f"No datasets found for product {product['product_id']}"
            for dataset in product["datasets"]:
                assert dataset["dataset_id"] is not None
                assert dataset["dataset_name"] is not None
                version_labels = list(
                    map(lambda x: x["label"], dataset["versions"])
                )
                assert len(version_labels) == len(set(version_labels))
                for version in dataset["versions"]:
                    assert re.match(
                        rf"({VERSION_DEFAULT}|{REGEX_PATTERN_DATE_YYYYMM})",
                        version["label"],
                    )
                    parts = version["parts"]
                    assert len(parts) != 0
                    has_default_part = (
                        len(
                            list(
                                filter(
                                    lambda x: x["name"] == PART_DEFAULT, parts
                                )
                            )
                        )
                        > 0
                    )
                    if has_default_part:
                        # If there is a "default" part, then it is the only one
                        assert len(parts) == 1
                    else:
                        # Else, there is no "default" part at all
                        assert all(
                            map(lambda x: x["name"] != PART_DEFAULT, parts)
                        )
                    part_names = list(
                        map(lambda x: x["name"], version["parts"])
                    )
                    assert len(part_names) == len(set(part_names))
                    for part in parts:
                        assert part["name"] is not None
                        assert part["name"] != ""
                        services = part["services"]
                        assert len(services) != 0, dataset["dataset_id"]
                        service_names = list(
                            map(
                                lambda x: x["service_type"]["service_name"],
                                services,
                            )
                        )
                        assert len(service_names) == len(set(service_names))
                        if (
                            CopernicusMarineDatasetServiceType.OMI_ARCO.service_name.value  # noqa
                            in service_names
                        ):
                            assert (
                                CopernicusMarineDatasetServiceType.GEOSERIES.service_name.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineDatasetServiceType.TIMESERIES.service_name.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineDatasetServiceType.STATIC_ARCO.service_name.value  # noqa
                                not in service_names
                            )
                        if (
                            CopernicusMarineDatasetServiceType.STATIC_ARCO.service_name.value  # noqa
                            in service_names
                        ):
                            assert (
                                CopernicusMarineDatasetServiceType.GEOSERIES.service_name.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineDatasetServiceType.TIMESERIES.service_name.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineDatasetServiceType.OMI_ARCO.service_name.value  # noqa
                                not in service_names
                            )
                        if service_names in (
                            CopernicusMarineDatasetServiceType.GEOSERIES,
                            CopernicusMarineDatasetServiceType.TIMESERIES,
                        ):
                            assert (
                                CopernicusMarineDatasetServiceType.OMI_ARCO.service_name.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineDatasetServiceType.STATIC_ARCO.service_name.value  # noqa
                                not in service_names
                            )

    def when_I_use_staging_environment_in_debug_logging_level(self):
        command = [
            "copernicusmarine",
            "describe",
            "--staging",
            "--log-level",
            "DEBUG",
            "--no-metadata-cache",
        ]
        self.output = subprocess.check_output(command, shell=True)

    def then_I_check_that_the_urls_contains_only_dta(self):
        assert b"https://stac.marine.copernicus.eu" not in self.output

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
        self.check_subset_request_with_dataseturl(
            subset_service_to_test.subpath,
            subset_service_to_test.dataset_url,
            tmp_path,
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
            "--force-download",
        ] + self.flatten_request_dict(self.base_request_dict)

        output = subprocess.run(command)
        assert output.returncode == 0

    def check_subset_request_with_dataseturl(
        self, function_name, dataset_url, tmp_path
    ):
        folder = pathlib.Path(tmp_path, function_name)
        if not folder.is_dir():
            pathlib.Path.mkdir(folder, parents=True)

        self.base_request_dict.pop("--dataset-id")
        self.base_request_dict["--dataset-url"] = f"{dataset_url}"

        command = [
            "copernicusmarine",
            "subset",
            "--force-download",
        ] + self.flatten_request_dict(self.base_request_dict)

        output = subprocess.run(command)
        assert output.returncode == 0

    def check_subset_request_with_dataset_not_in_catalog(self):
        self.base_request_dict["--dataset-id"] = "FAKE_ID"
        self.base_request_dict.pop("--dataset-url")

        unknown_dataset_request = [
            "copernicusmarine",
            "subset",
            "--force-download",
        ] + self.flatten_request_dict(self.base_request_dict)

        output = subprocess.run(unknown_dataset_request, capture_output=True)
        assert (
            b"Key error: The requested dataset 'FAKE_ID' was not found in the "
            b"catalogue, you can use 'copernicusmarine describe "
            b"--include-datasets --contains <search_token>' to find datasets"
        ) in output.stdout

    def check_subset_request_with_no_subsetting(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            f"{dataset_id}",
        ]

        output = subprocess.run(command, stdout=subprocess.PIPE)
        assert output.returncode == 1
        assert (
            b"Missing subset option. Try 'copernicusmarine subset --help'."
            in output.stdout
        )
        assert (
            b"To retrieve a complete dataset, please use instead: "
            b"copernicusmarine get --dataset-id " + bytes(dataset_id, "utf-8")
        ) in output.stdout

    def test_if_dataset_coordinate_valid_minmax_attributes_are_setted(
        self, tmp_path
    ):
        self.base_request_dict = {
            "--dataset-id": "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            "--variable": "so",
            "--start-datetime": "2021-01-01",
            "--end-datetime": "2021-01-02",
            "--minimum-latitude": "0.0",
            "--maximum-latitude": "0.1",
            "--minimum-longitude": "0.2",
            "--maximum-longitude": "0.3",
            "--minimum-depth": "0.0",
            "--maximum-depth": "5.0",
            "-f": "output.nc",
            "--output-directory": tmp_path,
        }

        self.check_default_subset_request(self.GEOSERIES.subpath, tmp_path)

        dataset_path = pathlib.Path(tmp_path) / "output.nc"
        dataset = xarray.open_dataset(dataset_path)

        assert dataset.latitude.attrs["valid_min"] >= 0
        assert dataset.latitude.attrs["valid_max"] <= 0.1
        assert dataset.longitude.attrs["valid_min"] >= 0.2
        assert dataset.longitude.attrs["valid_max"] <= 0.3
        assert dataset.depth.attrs["valid_min"] >= 0
        assert dataset.depth.attrs["valid_max"] <= 5
        assert (
            dataset.time.attrs["valid_min"] == "2021-01-01T00:00:00.000000000"
        )
        assert (
            dataset.time.attrs["valid_max"] == "2021-01-02T00:00:00.000000000"
        )

    def test_retention_period_works(self):
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
            "--force-download",
        ]

        self.output = execute_in_terminal(self.command)
        assert (
            b"time       (time) datetime64[ns] 2023" not in self.output.stdout
        )

    # -------------------------#
    # Test on get requests #
    # -------------------------#

    @dataclass(frozen=True)
    class GetServiceToTest:
        name: str

    FILES = GetServiceToTest("files")

    def test_get_original_files_functionnality(self, tmp_path):
        self._test_get_functionalities(self.FILES, tmp_path)

    def _test_get_functionalities(
        self, get_service_to_test: GetServiceToTest, tmp_path
    ):
        self.base_get_request_dict: dict[str, Optional[Union[str, Path]]] = {
            "--dataset-id": "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--output-directory": str(tmp_path),
            "--no-directories": None,
            "--service": get_service_to_test.name,
        }
        self.check_default_get_request(get_service_to_test, tmp_path)

    def check_default_get_request(
        self, get_service_to_test: GetServiceToTest, tmp_path
    ):
        folder = pathlib.Path(tmp_path, get_service_to_test.name)
        if not folder.is_dir():
            pathlib.Path.mkdir(folder, parents=True)

        command = [
            "copernicusmarine",
            "get",
            "--force-download",
            "--output-directory",
            f"{folder}",
        ] + self.flatten_request_dict(self.base_get_request_dict)

        output = subprocess.run(command)
        assert output.returncode == 0

    def test_get_download_s3_without_regex(self, tmp_path):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--service",
            f"{self.FILES.name}",
            "--force-download",
            "--output-directory",
            f"{tmp_path}",
        ]

        output = subprocess.run(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert output.returncode == 0
        assert len(downloaded_files) == 29

    def test_get_download_s3_with_regex(self, tmp_path):
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--service",
            f"{self.FILES.name}",
            "--regex",
            f"{regex}",
            "--force-download",
            "--output-directory",
            f"{tmp_path}",
        ]

        output = subprocess.run(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert output.returncode == 0
        assert len(downloaded_files) == 3

        for filename in downloaded_files:
            assert re.match(regex, filename) is not None

    def test_files_to_download_are_displayed(self, tmp_path):
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--service",
            f"{self.FILES.name}",
            "--regex",
            f"{regex}",
            "--output-directory",
            f"{tmp_path}",
        ]

        output = subprocess.run(command, capture_output=True)
        assert (
            b"You requested the download of the following files"
            in output.stdout
        )

    def test_downloaded_files_are_not_displayed_with_force_download_option(
        self, tmp_path
    ):
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--service",
            f"{self.FILES.name}",
            "--regex",
            f"{regex}",
            "--force-download",
            "--output-directory",
            f"{tmp_path}",
        ]

        output = subprocess.run(command, capture_output=True)
        assert (
            b"You requested the download of the following files"
            not in output.stdout
        )

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
            "--force-download",
        ]

        output = subprocess.run(command)
        is_file = pathlib.Path(tmp_path, output_filename).is_file()
        assert output.returncode == 0
        assert is_file

    def test_process_is_stopped_when_credentials_are_invalid(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

        command = [
            "copernicusmarine",
            "subset",
            "--username",
            "toto",
            "--password",
            "tutu",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--force-download",
        ]

        output = subprocess.run(command, capture_output=True)

        assert output.returncode == 1
        assert b"Invalid username or password" in output.stdout

    def test_login_is_prompt_when_configuration_file_doest_not_exist(
        self, tmp_path
    ):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        credentials_file = Path(tmp_path, "i_do_not_exist")

        environment_without_crendentials = (
            get_environment_without_crendentials()
        )

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
            "--credentials-file",
            f"{credentials_file}",
        ]

        output = subprocess.run(
            command, capture_output=True, env=environment_without_crendentials
        )
        assert output.returncode == 1
        assert b"username:" in output.stdout

    def test_login_command(self, tmp_path):
        self.check_credentials_username_specified_password_prompt(tmp_path)

    def check_credentials_username_specified_password_prompt(self, tmp_path):
        assert os.getenv("COPERNICUS_MARINE_SERVICE_USERNAME") is not None
        assert os.getenv("COPERNICUS_MARINE_SERVICE_PASSWORD") is not None

        environment_without_crendentials = (
            get_environment_without_crendentials()
        )

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--start-datetime",
            "2023-04-26 00:00:00",
            "--end-datetime",
            "2023-04-28 23:59:59",
            "--minimum-longitude",
            "-9.8",
            "--maximum-longitude",
            "-4.8",
            "--minimum-latitude",
            "33.9",
            "--maximum-latitude",
            "38.0",
            "--minimum-depth",
            "9.573",
            "--maximum-depth",
            "11.4",
            "--username",
            f"{os.getenv('COPERNICUS_MARINE_SERVICE_USERNAME')}",
            "--force-download",
            "-o",
            f"{tmp_path}",
        ]

        output = subprocess.run(
            command,
            env=environment_without_crendentials,
            input=bytes(
                os.getenv("COPERNICUS_MARINE_SERVICE_PASSWORD"), "utf-8"
            ),
        )
        assert output.returncode == 0, output.stdout
        shutil.rmtree(Path(tmp_path))

    def test_get_download_s3_with_wildcard_filter(self, tmp_path):
        filter = "*_200[123]*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--service",
            f"{self.FILES.name}",
            "--filter",
            f"{filter}",
            "--force-download",
            "--output-directory",
            f"{tmp_path}",
        ]

        output = subprocess.run(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert output.returncode == 0
        assert len(downloaded_files) == 3

        for filename in downloaded_files:
            assert fnmatch.fnmatch(filename, filter)

    def test_get_download_s3_with_wildcard_filter_and_regex(self, tmp_path):
        filter = "*_200[45]*.nc"
        regex = ".*_(2001|2002|2003).*.nc"
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--service",
            f"{self.FILES.name}",
            "--filter",
            f"{filter}",
            "--regex",
            f"{regex}",
            "--force-download",
            "--output-directory",
            f"{tmp_path}",
        ]

        output = subprocess.run(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert output.returncode == 0
        assert len(downloaded_files) == 5

        for filename in downloaded_files:
            assert (
                fnmatch.fnmatch(filename, filter)
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
        ]

        output = subprocess.run(command, stdout=subprocess.PIPE)
        assert b"No data to download" in output.stdout
        assert output.returncode == 0

    def test_login(self, tmp_path):
        non_existing_directory = Path(tmp_path, "i_dont_exist")
        command = [
            "copernicusmarine",
            "login",
            "--overwrite-configuration-file",
            "--configuration-file-directory",
            f"{non_existing_directory}",
            "--username",
            f"{os.getenv('COPERNICUS_MARINE_SERVICE_USERNAME')}",
            "--password",
            f"{os.getenv('COPERNICUS_MARINE_SERVICE_PASSWORD')}",
        ]

        output = subprocess.run(command)
        assert output.returncode == 0
        assert non_existing_directory.is_dir()

        command_with_skip = [
            "copernicusmarine",
            "login",
            "--configuration-file-directory",
            f"{non_existing_directory}",
            "--skip-if-user-logged-in",
        ]
        output_with_skip = subprocess.run(command_with_skip)
        assert output_with_skip.returncode == 0

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

        self.run_output = subprocess.run(
            command, stderr=subprocess.PIPE, stdout=subprocess.PIPE
        )

    def then_I_got_a_clear_output_with_available_service_for_subset(self):
        assert (
            b"Service unavailable-service does not exist for command subset. "
            b"Possible services: ['arco-geo-series', 'geoseries', "
            b"'arco-time-series', 'timeseries', 'omi-arco', 'static-arco']"
        ) in self.run_output.stdout

    def test_mutual_exclusivity_of_cache_options_for_describe(self):
        self.when_I_run_copernicus_marine_command_with_both_cache_options(
            "describe"
        )
        self.then_I_got_an_error_regarding_mutual_exclusivity()

    def test_mutual_exclusivity_of_cache_options_for_get(self):
        self.when_I_run_copernicus_marine_command_with_both_cache_options(
            "get"
        )
        self.then_I_got_an_error_regarding_mutual_exclusivity()

    def test_mutual_exclusivity_of_cache_options_for_subset(self):
        self.when_I_run_copernicus_marine_command_with_both_cache_options(
            "subset"
        )
        self.then_I_got_an_error_regarding_mutual_exclusivity()

    def when_I_run_copernicus_marine_command_with_both_cache_options(
        self, command_option
    ):
        command = [
            "copernicusmarine",
            f"{command_option}",
            "--overwrite-metadata-cache",
            "--no-metadata-cache",
        ]
        self.run_output = subprocess.run(
            command, stderr=subprocess.PIPE, stdout=subprocess.PIPE
        )

    def then_I_got_an_error_regarding_mutual_exclusivity(self):
        assert self.run_output.returncode == 2
        assert self.run_output.stdout == b""
        assert self.run_output.stderr == (
            b"Error: Illegal usage: `overwrite-metadata-cache` is mutually "
            b"exclusive with arguments `no-metadata-cache`.\n"
        )

    def test_describe_without_using_cache(self):
        command = ["copernicusmarine", "describe", "--no-metadata-cache"]
        output = execute_in_terminal(command=command, timeout_second=30)
        assert output.returncode == 0

    def when_I_request_subset_dataset_with_zarr_service(
        self, output_path, vertical_dimension_as_originally_produced
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
            "0",
            "-Z",
            "10",
            "-v",
            "thetao",
            "--vertical-dimension-as-originally-produced",
            f"{vertical_dimension_as_originally_produced}",
            "--service",
            "arco-time-series",
            "-o",
            f"{output_path}",
            "-f",
            "data.zarr",
            "--force-download",
        ]

        self.run_output = subprocess.run(
            command, stderr=subprocess.PIPE, stdout=subprocess.PIPE
        )

    def then_I_have_correct_sign_for_depth_coordinates_values(
        self, output_path, sign
    ):
        filepath = pathlib.Path(output_path, "data.zarr")
        dataset = xarray.open_dataset(filepath, engine="zarr")

        assert self.run_output.returncode == 0
        if sign == "positive":
            assert dataset.depth.min() <= 10
            assert dataset.depth.max() >= 0
        elif sign == "negative":
            assert dataset.elevation.min() >= -10
            assert dataset.elevation.max() <= 0

    def test_conversion_between_elevation_and_depth(self, tmp_path):
        self.when_I_request_subset_dataset_with_zarr_service(tmp_path, True)
        self.then_I_have_correct_sign_for_depth_coordinates_values(
            tmp_path, "positive"
        )

    def test_force_no_conversion_between_elevation_and_depth(self, tmp_path):
        self.when_I_request_subset_dataset_with_zarr_service(tmp_path, False)
        self.then_I_have_correct_sign_for_depth_coordinates_values(
            tmp_path, "negative"
        )

    def when_I_run_copernicus_marine_command_using_no_directories_option(
        self, tmp_path, force_service: GetServiceToTest, output_directory=None
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
            "--force-download",
            "--output-directory",
            f"{download_folder}",
            "--service",
            f"{force_service.name}",
            "--no-directories",
        ]

        output = subprocess.run(command)

        assert output.returncode == 0

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
            tmp_path, force_service=self.FILES
        )
        self.then_files_are_created_without_tree_folder(tmp_path)
        self.when_I_run_copernicus_marine_command_using_no_directories_option(
            tmp_path, force_service=self.FILES, output_directory="test"
        )
        self.then_files_are_created_without_tree_folder(
            tmp_path, output_directory="test"
        )

    def test_default_prompt_for_get_command(self, tmp_path):
        command = [
            "copernicusmarine",
            "get",
            "-i",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--service",
            f"{self.FILES.name}",
            "-nd",
            "--filter",
            "*20120101_20121231_R20221101_RE01*",
            "-o",
            f"{tmp_path}",
        ]
        output = subprocess.run(command, input=b"y")

        assert output.returncode == 0
        assert (
            Path(tmp_path)
            / "CMEMS_v5r1_IBI_PHY_MY_NL_01yav_20120101_20121231_R20221101_RE01.nc"
        )

    def test_default_service_for_get_command(self, tmp_path):
        self.when_I_run_copernicus_marine_get_with_default_service()
        self.then_I_can_see_the_original_files_service_is_choosen()

    def when_I_run_copernicus_marine_get_with_default_service(
        self,
    ):
        command = [
            "copernicusmarine",
            "get",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1D-m",
        ]

        self.run_output = subprocess.run(
            command, stderr=subprocess.PIPE, stdout=subprocess.PIPE
        )

    def then_I_can_see_the_original_files_service_is_choosen(self):
        assert (
            b"Downloading using service original-files..."
            in self.run_output.stdout
        )

    def test_default_service_for_subset_command(self, tmp_path):
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
        ]

        self.run_output = subprocess.run(
            command, stderr=subprocess.PIPE, stdout=subprocess.PIPE
        )

    def then_I_can_see_the_arco_geo_series_service_is_choosen(self):
        assert (
            b"Downloading using service arco-geo-series..."
            in self.run_output.stdout
        )

    def test_subset_with_dataset_id_and_url(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_anfc_ecosmo_P1M-m",
            "-u",
            "https://nrt.cmems-du.eu/thredds/dodsC/METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2",
            "--variable",
            "thetao",
        ]

        output = subprocess.run(command, capture_output=True)

        assert output.returncode == 1
        assert (
            b"Must specify only one of 'dataset_url' or 'dataset_id' options"
        ) in output.stdout

    def test_no_traceback_is_printed_on_dataset_url_error(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-url",
            "https://s3.waw3-1.cloudferro.com/mdl-arco-time-013/arco/"
            "GLOBAL_ANALYSISFORECAST_PHY_XXXXXXX/"
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m/2023",
        ]

        output = subprocess.run(command, capture_output=True)

        assert output.returncode == 1
        assert not (b"Traceback") in output.stderr

    def test_get_2023_08_original_files(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
            "--filter",
            "*/2023/08/*",
        ]
        output = subprocess.run(command, capture_output=True)

        assert output.returncode == 1
        assert not (b"No data to download") in output.stdout

    def test_subset_with_chunking(self, tmp_path):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
            "-t",
            "2021-01-01T00:00:00",
            "-T",
            "2021-01-05T23:59:59",
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
            "--force-download",
            "-o",
            f"{tmp_path}",
        ]

        output = subprocess.run(command, capture_output=True)

        assert output.returncode == 0

    def test_dataset_url_suffix_path_are_used_as_filter(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-url",
            "https://s3.waw3-1.cloudferro.com/mdl-native-14/native/"
            "GLOBAL_ANALYSISFORECAST_PHY_001_024/"
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m_202211/2023/11",
        ]

        output = subprocess.run(command, capture_output=True)

        assert b"Printed 20 out of 30 files" in output.stdout

    def test_short_option_for_copernicus_marine_command_helper(self):
        short_option_command = [
            "copernicusmarine",
            "-h",
        ]
        long_option_command = [
            "copernicusmarine",
            "--help",
        ]

        short_option_output = subprocess.run(
            short_option_command, capture_output=True
        )
        long_option_output = subprocess.run(
            long_option_command, capture_output=True
        )

        assert short_option_output.stdout == long_option_output.stdout

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

        short_option_output = subprocess.run(
            short_option_command, capture_output=True
        )
        long_option_output = subprocess.run(
            long_option_command, capture_output=True
        )

        assert short_option_output.stdout == long_option_output.stdout

    def test_subset_template_creation(self):
        command = ["copernicusmarine", "subset", "--create-template"]

        output = subprocess.run(command, capture_output=True)

        assert (
            b"Template created at: subset_template.json"
            == remove_extra_logging_prefix_info(output.stdout)
        )
        assert Path("subset_template.json").is_file()

    def test_get_template_creation(self):
        command = ["copernicusmarine", "get", "--create-template"]

        output = subprocess.run(command, capture_output=True)

        assert (
            b"Template created at: get_template.json"
            == remove_extra_logging_prefix_info(output.stdout)
        )
        assert Path("get_template.json").is_file()

    def test_get_template_creation_with_extra_arguments(self):
        command = [
            "copernicusmarine",
            "get",
            "--create-template",
            "--force-download",
        ]

        output = subprocess.run(command, capture_output=True)

        assert (
            b"Other options passed with create template: force_download"
            == remove_extra_logging_prefix_info(output.stdout)
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

        output = subprocess.run(command, capture_output=True)

        assert (
            b"The variable 'theta' is neither a "
            b"variable or a standard name in the dataset" in output.stdout
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

        output = subprocess.run(command, capture_output=True)

        assert b"Service ft does not exist for command subset" in output.stdout

    def then_I_can_read_copernicusmarine_version_in_the_dataset_attributes(
        self, filepath
    ):
        dataset = xarray.open_dataset(filepath)
        assert "copernicusmarine_version" in dataset.attrs

    def test_copernicusmarine_version_in_dataset_attributes_with_arco(
        self, tmp_path
    ):
        self.when_I_request_subset_dataset_with_zarr_service(tmp_path, True)
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
            "--force-download",
        ]

        output = subprocess.run(command)
        assert output.returncode == 0
        assert (
            "thetao"
            in xarray.open_zarr(f"{tmp_path}/{output_filename}").variables
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
            "--force-download",
            "--log-level",
            "DEBUG",
        ]

        output = subprocess.run(command, capture_output=True)
        assert output.returncode == 0
        assert b"DEBUG - " in output.stdout

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
            "--force-download",
            "-o",
            f"{tmp_path}",
        ]

        output = execute_in_terminal(command, timeout_second=10)
        assert output.returncode == 0, output.stderr

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
            "--force-download",
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
        output = subprocess.run(command)
        assert output.returncode == 0
        assert expected_filepath.is_dir()

    def then_I_can_read_dataset_size(self):
        assert (
            b"Estimated size of the dataset file is" in self.run_output.stdout
        )

    def test_dataset_size_is_displayed_when_downloading_with_arco_service(
        self, tmp_path
    ):
        self.when_I_request_subset_dataset_with_zarr_service(tmp_path, True)
        self.then_I_can_read_dataset_size()

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
            "0",
            "-Z",
            "0",
            "-o",
            f"{tmp_path}",
            "-f",
            f"{output_filename}",
            "--force-download",
        ]

        output = subprocess.run(command, capture_output=True)
        assert output.returncode == 0
        assert (
            len(
                xarray.open_dataset(
                    Path(tmp_path) / output_filename
                ).dims.keys()
            )
            == 4
        )

    def when_I_request_a_dataset_with_subset_method_option(
        self, subset_method
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-hcmr-wav-rean-h",
            "-x",
            "-19",
            "-X",
            "-17",
            "-y",
            "38.007",
            "-Y",
            "38.028",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-01T06:00:00",
            "-v",
            "VHM0",
            "--force-download",
            "--subset-method",
            f"{subset_method}",
        ]

        self.output = subprocess.run(command, capture_output=True)

    def then_I_can_read_an_error_in_stdout(self):
        assert self.output.returncode == 1
        assert b"ERROR" in self.output.stdout
        assert (
            b"Some or all of your subset selection [-19.0, -17.0] for "
            b"the longitude dimension  exceed the dataset coordinates"
        ) in self.output.stdout

    def then_I_can_read_a_warning_in_stdout(self):
        assert self.output.returncode == 0
        assert b"WARNING" in self.output.stdout
        assert (
            b"Some or all of your subset selection [-19.0, -17.0] for "
            b"the longitude dimension  exceed the dataset coordinates"
        ) in self.output.stdout

    def test_subset_strict_method(self):
        self.when_I_request_a_dataset_with_subset_method_option("strict")
        self.then_I_can_read_an_error_in_stdout()

    def test_subset_nearest_method(self):
        self.when_I_request_a_dataset_with_subset_method_option("nearest")
        self.then_I_can_read_a_warning_in_stdout()

    def test_netcdf_compression_option(self, tmp_path):
        filename_without_option = "without_option.nc"
        filename_with_option = "with_option.nc"
        filename_zarr_without_option = "filename_without_option.zarr"
        filename_zarr_with_option = "filename_with_option.zarr"

        netcdf_compression_option = "--netcdf-compression-enabled"

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
            "--force-download",
            "-o",
            f"{tmp_path}",
        ]

        output_without_option = subprocess.run(
            base_command + ["-f", filename_without_option]
        )
        output_with_option = subprocess.run(
            base_command
            + ["-f", filename_with_option, netcdf_compression_option]
        )
        output_zarr_without_option = subprocess.run(
            base_command + ["-f", filename_zarr_without_option]
        )
        output_zarr_with_option = subprocess.run(
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

    def test_omi_arco_service(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "blksea_omi_circulation_rim_current_index",
            "-v",
            "BSRCI",
            "--force-download",
            "-o",
            f"{tmp_path}",
        ]

        output = subprocess.run(base_command, capture_output=True)
        assert output.returncode == 0
        assert b"Downloading using service omi-arco..." in output.stdout

        output = subprocess.run(
            base_command + ["-s", "omi-arco"], capture_output=True
        )
        assert output.returncode == 0
        assert b"Downloading using service omi-arco..." in output.stdout

    def test_static_arco_service(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_blk_phy_anfc_2.5km_static",
            "-v",
            "deptho",
            "--force-download",
            "--dataset-part",
            "bathy",
            "-o",
            f"{tmp_path}",
        ]

        output = subprocess.run(base_command, capture_output=True)
        assert output.returncode == 0
        assert b"Downloading using service static-arco..." in output.stdout

        output = subprocess.run(
            base_command + ["-s", "static-arco"], capture_output=True
        )
        assert output.returncode == 0
        assert b"Downloading using service static-arco..." in output.stdout

    def test_subset_dataset_part_option(self, tmp_path):
        base_command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_blk_phy_anfc_2.5km_static",
            "-v",
            "deptho",
            "--force-download",
            "-o",
            f"{tmp_path}",
        ]

        output = subprocess.run(
            base_command + ["--dataset-part", "bathy"], capture_output=True
        )
        assert output.returncode == 0

    def test_netcdf_compression_level(self, tmp_path):
        netcdf_compression_enabled_option = "--netcdf-compression-enabled"
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
            "--force-download",
            "-o",
            f"{tmp_path}",
            "-f",
            "data.nc",
            "--netcdf-compression-level",
            f"{forced_comp_level}",
        ]

        output_without_netcdf_compression_enabled = subprocess.run(
            base_command
        )
        output_with_netcdf_compression_enabled = subprocess.run(
            base_command + [netcdf_compression_enabled_option]
        )

        assert output_without_netcdf_compression_enabled.returncode != 0
        assert output_with_netcdf_compression_enabled.returncode == 0

        filepath = Path(tmp_path / "data.nc")
        dataset = xarray.open_dataset(filepath)
        logger.info(f"{dataset.uo.encoding=}, {dataset.uo.encoding=}")

        assert dataset.uo.encoding["zlib"] is True
        assert dataset.uo.encoding["complevel"] == forced_comp_level
        assert dataset.uo.encoding["contiguous"] is False
        assert dataset.uo.encoding["shuffle"] is True

    def test_that_cache_folder_isnt_created_when_no_metadata_cache_option_was_provided(
        self, tmp_path
    ):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        output_filename = "test_subset_output_file_as_netcdf.nc"
        cache_directory = f"{tmp_path}"

        os.environ["COPERNICUSMARINE_CACHE_DIRECTORY"] = cache_directory

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
            "--force-download",
        ]

        subprocess.run(command + ["--no-metadata-cache"])
        cache_path = Path(tmp_path) / Path(".copernicusmarine") / Path("cache")
        assert cache_path.is_dir() is False

        subprocess.run(command)
        assert cache_path.is_dir() is True

        del os.environ["COPERNICUSMARINE_CACHE_DIRECTORY"]

    def test_file_list_filter(self, tmp_path):
        dataset_id = "cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.25deg_P1D"
        command = [
            "copernicusmarine",
            "get",
            "-i",
            f"{dataset_id}",
            "--service",
            f"{self.FILES.name}",
            "--file-list",
            "./tests/resources/file_list_example.txt",
            "--force-download",
            "--output-directory",
            f"{tmp_path}",
        ]

        output = subprocess.run(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert output.returncode == 0
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
            "--service",
            "files",
            "--regex",
            f"{regex}",
            "--download-file-list",
            "--output-directory",
            f"{tmp_path}",
        ]

        output_filename = pathlib.Path(tmp_path) / "files_to_download.txt"

        output = subprocess.run(command)
        assert output.returncode == 0
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
            "--force-download",
            "--filter",
            "*2022053112000*",
            "--force-service",
            "original-files",
            "--output-directory",
            f"{tmp_path}",
            "--no-directories",
        ]
        output = subprocess.run(command)
        output_file = pathlib.Path(
            tmp_path,
            "20220531120000-UKMO-L4_GHRSST-SSTfnd-OSTIA-GLOB_REP-v02.0-fv02.0.nc",
        )
        five_minutes_ago = datetime.datetime.now() - datetime.timedelta(
            minutes=5
        )

        assert output.returncode == 0
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
            "--force-download",
        ]
        output = subprocess.run(command)
        assert output.returncode == 0

        output_netcdf_format = subprocess.run(
            ["ncdump", "-k", f"{tmp_path / 'dataset.nc'}"], capture_output=True
        )
        assert output_netcdf_format.returncode == 0
        assert output_netcdf_format.stdout == b"classic\n"
