import logging
import re
from json import loads
from unittest import mock

import pytest
from pydantic import ValidationError

from copernicusmarine import (
    CopernicusMarineCatalogue,
    CopernicusMarineServiceNames,
    describe,
)
from copernicusmarine.catalogue_parser.models import (
    PART_DEFAULT,
    REGEX_PATTERN_DATE_YYYYMM,
    VERSION_DEFAULT,
)
from tests.resources.mock_stac_catalog_WAW3.mock_marine_data_store_stac_metadata import (  # noqa: E501
    mocked_stac_requests_get,
)
from tests.test_utils import execute_in_terminal


class TestDescribe:
    # CLI (Command Line Interface) tests
    def test_describe_default_fast_with_timeout(self):
        self.when_I_run_copernicus_marine_describe_with_default_arguments()
        self.then_stdout_can_be_load_as_json()
        self.then_I_can_read_the_default_json()
        self.and_there_are_no_warnings_about_backend_versions()

    def test_describe_return_fields_datasets_fast_with_timeout(self, snapshot):
        self.when_I_run_copernicus_marine_describe_including_datasets()
        self.then_I_can_read_it_does_not_contain_weird_symbols()
        self.then_I_can_read_the_json_including_datasets()
        self.then_omi_services_are_not_in_the_catalog()
        self.then_products_from_marine_data_store_catalog_are_available()
        self.then_datasets_variables_are_correct(snapshot)
        self.then_all_dataset_parts_are_filled()

    def test_describe_timeout_product_id_dataset_id(self):
        dataset_id = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
        product_id = "GLOBAL_MULTIYEAR_PHY_001_030"
        different_product_id = "ANTARCTIC_OMI_SI_extent"
        self.when_I_run_copernicus_marine_describe_with_product_id_and_dataset_id(
            product_id, None
        )
        self.then_stdout_can_be_load_as_json()
        self.then_I_have_only_one_product()
        self.when_I_run_copernicus_marine_describe_with_product_id_and_dataset_id(
            None, dataset_id
        )
        self.then_stdout_can_be_load_as_json()
        self.then_I_have_only_one_product_and_one_dataset()
        self.when_I_run_copernicus_marine_describe_with_product_id_and_dataset_id(
            product_id, dataset_id
        )
        self.then_stdout_can_be_load_as_json()
        self.then_I_have_only_one_product_and_one_dataset()
        self.when_I_run_copernicus_marine_describe_with_product_id_and_dataset_id(
            different_product_id, dataset_id
        )
        self.then_I_have_an_error_message_about_dataset_id_and_product_id()

    def test_describe_contains_option_fast_with_timeout(self):
        self.when_I_run_copernicus_marine_describe_with_contains_option()
        self.then_I_can_read_the_filtered_json()

    def test_describe_with_staging_flag(self):
        self.when_I_use_staging_environment_in_debug_logging_level()
        self.then_I_check_that_the_urls_contains_only_dta()

    def test_describe_timeout_function_with_return_fields(self):
        self.when_I_run_copernicus_marine_describe_with_return_fields()
        self.then_stdout_can_be_load_as_json()
        self.then_only_the_queried_fields_are_returned()

    def test_describe_timeout_exclude_datasets(self):
        product_id = "GLOBAL_MULTIYEAR_PHY_001_030"
        self.when_I_run_copernicus_marine_describe_with_product_id_and_dataset_id(
            product_id, None, exclude="services"
        )
        json_result = loads(self.output.stdout)
        for product in json_result["products"]:
            for dataset in product["datasets"]:
                for version in dataset["versions"]:
                    for part in version["parts"]:
                        assert "services" not in part

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_describe_with_raise_on_error_error_in_stac(
        self, mocked_requests, caplog
    ):
        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            with pytest.raises(Exception):
                describe(
                    raise_on_error=True,
                    product_id="NWSHELF_MULTIYEAR_BGC_004_011",
                )
            assert "Stopping describe" in caplog.text

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_describe_with_raise_on_error_unavailable_dataset(
        self, mocked_requests, caplog
    ):
        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            with pytest.raises(Exception):
                describe(
                    raise_on_error=True,
                    product_id="GLOBAL_ANALYSISFORECAST_PHY_001_024",
                )
            assert (
                "Failed to fetch or parse JSON for dataset URL:" in caplog.text
            )

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_describe_with_raise_on_error_unavailable_product(
        self, mocked_requests, caplog
    ):
        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            with pytest.raises(Exception):
                describe(
                    raise_on_error=True,
                    product_id="UNAVAILABLE_PRODUCT",
                )
            assert (
                "Failed to fetch or parse JSON for product URL:" in caplog.text
            )
            assert "UNAVAILABLE_PRODUCT" in caplog.text

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_describe_with_raise_on_error_product_w_errors(
        self, mocked_requests, caplog
    ):

        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            with pytest.raises(ValidationError):
                describe(
                    raise_on_error=True,
                    product_id="PRODUCT_W_ERRORS",
                )
            assert "Error while parsing product" in caplog.text

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_describe_without_raise_on_error(self, mocked_requests, caplog):
        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            describe(
                raise_on_error=False,
                show_all_versions=True,
            )
            assert "Failed to parse part" in caplog.text
            assert "Skipping part." in caplog.text
            assert (
                "Failed to fetch or parse JSON for product URL: "
                "https://s3.waw3-1.cloudferro.com/mdl-metadata/metadata"
                "/UNAVAILABLE_PRODUCT/product.stac.json" in caplog.text
            )
            assert (
                "Failed to fetch or parse JSON for dataset URL: "
                "https://s3.waw3-1.cloudferro.com/mdl-metadata/meta"
                "data/GLOBAL_ANALYSISFORECAST_PHY_001_024/unavailable_"
                "dataset_202012/dataset.stac.json" in caplog.text
            )
            assert "UNAVAILABLE_PRODUCT" in caplog.text
            assert "unavailable_dataset" in caplog.text

    def when_I_run_copernicus_marine_describe_with_default_arguments(self):
        command = ["copernicusmarine", "describe"]
        self.output = execute_in_terminal(command, timeout_second=30)

    def then_stdout_can_be_load_as_json(self):
        loads(self.output.stdout)

    def then_I_can_read_the_default_json(self):
        json_result = loads(self.output.stdout)
        # TODO: increase number after November release
        assert len(json_result["products"]) >= 270
        seen_processing_level = False
        seen_digital_object_identifier = False
        for product in json_result["products"]:
            assert product["title"] is not None
            assert product["product_id"] is not None
            assert product["thumbnail_url"] is not None
            seen_digital_object_identifier = (
                seen_digital_object_identifier
                or ("digital_object_identifier" in product)
            )
            assert product["sources"] is not None
            seen_processing_level = (
                seen_processing_level or "processing_level" in product
            )
            assert product["production_center"] is not None
        assert seen_processing_level
        assert seen_digital_object_identifier

    def and_there_are_no_warnings_about_backend_versions(self):
        assert (
            "Please update to the latest client version."
            not in self.output.stderr
        )

    def then_omi_services_are_not_in_the_catalog(self):
        json_result = loads(self.output.stdout)
        for product in json_result["products"]:
            for dataset in product["datasets"]:
                for version in dataset["versions"]:
                    for part in version["parts"]:
                        assert "omi" not in [
                            x["service_name"] for x in part["services"]
                        ]

    def then_products_from_marine_data_store_catalog_are_available(self):
        expected_product_id = "NWSHELF_ANALYSISFORECAST_PHY_004_013"
        expected_dataset_id = "cmems_mod_nws_phy-bottomt_anfc_1.5km-2D_P1D-m"
        expected_services = [
            "original-files",
            "arco-geo-series",
            "arco-time-series",
            "wmts",
        ]

        json_result = loads(self.output.stdout)
        expected_product = [
            product
            for product in json_result["products"]
            if product["product_id"] == expected_product_id
        ]
        assert len(expected_product) == 1
        product = expected_product[0]
        product_datasets = product["datasets"]
        expected_dataset = [
            product
            for product in product_datasets
            if product["dataset_id"] == expected_dataset_id
        ]
        assert len(expected_dataset) == 1
        dataset = expected_dataset[0]
        expected_dataset_services = [
            x["service_name"]
            for x in dataset["versions"][0]["parts"][0]["services"]
        ]
        assert all(
            map(lambda x: x in expected_services, expected_dataset_services)
        )

    def remove_maximum_time_from_service(self, service: dict):
        """Remove fields that are not relevant for the snapshot testing."""
        for variable in service["variables"]:
            for coordinate in variable.get("coordinates", []):
                if coordinate["coordinate_id"] == "time":
                    coordinate.pop("maximum_value", None)
        return service

    def then_datasets_variables_are_correct(self, snapshot):
        expected_product_id = "GLOBAL_MULTIYEAR_PHY_ENS_001_031"
        expected_dataset_id = "cmems_mod_glo_phy-all_my_0.25deg_P1D-m"
        wanted_services = [
            "original-files",
            "arco-geo-series",
            "arco-time-series",
        ]
        json_result = loads(self.output.stdout)
        expected_product = [
            product
            for product in json_result["products"]
            if product["product_id"] == expected_product_id
        ]
        product = expected_product[0]
        product_datasets = product["datasets"]
        expected_dataset = [
            product
            for product in product_datasets
            if product["dataset_id"] == expected_dataset_id
        ]
        dataset = expected_dataset[0]
        wanted_services_in_dataset = [
            self.remove_maximum_time_from_service(x)
            for x in dataset["versions"][0]["parts"][0]["services"]
            if x["service_name"] in wanted_services
        ]
        assert snapshot == wanted_services_in_dataset

    def then_all_dataset_parts_are_filled(self):
        expected_product_id = "BALTICSEA_ANALYSISFORECAST_BGC_003_007"
        expected_dataset_id = "cmems_mod_bal_bgc_anfc_static"

        json_result = loads(self.output.stdout)
        expected_product = list(
            filter(
                lambda product: product["product_id"] == expected_product_id,
                json_result["products"],
            )
        )
        assert len(expected_product) == 1
        product = expected_product[0]

        expected_dataset = [
            product
            for product in product["datasets"]
            if product["dataset_id"] == expected_dataset_id
        ]

        assert len(expected_dataset) == 1
        dataset = expected_dataset[0]

        for version in dataset["versions"]:
            non_default_parts = [
                part
                for part in version["parts"]
                if part["name"] != PART_DEFAULT
            ]

            assert len(non_default_parts) > 0

        version_ordered = sorted(
            dataset["versions"],
            key=lambda x: x["label"]
            if x["label"] != VERSION_DEFAULT
            else "110001",
            reverse=True,
        )

        latest_version = version_ordered[0]
        maybe_default_part = [
            part
            for part in latest_version["parts"]
            if part["name"] == PART_DEFAULT
        ]

        assert len(maybe_default_part) == 0

    def when_I_run_copernicus_marine_describe_with_contains_option(self):
        filter_token = "OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_n"
        command = [
            "copernicusmarine",
            "describe",
            "--contains",
            f"{filter_token}",
        ]
        self.output = execute_in_terminal(command, timeout_second=30)

    def then_I_can_read_the_filtered_json(self):
        json_result = loads(self.output.stdout)
        assert len(json_result["products"]) == 2
        assert (
            json_result["products"][0]["product_id"]
            == "OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_nag_area_mean"
        )
        # TODO: replace by the values when we know they are right
        # assert json_result["products"][0]["production_center"] == "PML (UK)"
        # assert (
        #     json_result["products"][0]["thumbnail_url"]
        #     == "https://catalogue.marine.copernicus.eu/documents/IMG/OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_nag_area_mean.png"  # noqa
        # )
        assert (
            json_result["products"][0]["title"]
            == "North Atlantic Gyre Area Chlorophyll-a time series and trend from Observations Reprocessing"  # noqa
        )

        assert (
            json_result["products"][1]["product_id"]
            == "OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_npg_area_mean"
        )
        # assert json_result["products"][1]["production_center"] == "PML (UK)"
        # assert (
        #     json_result["products"][1]["thumbnail_url"]
        #     == "https://catalogue.marine.copernicus.eu/documents/IMG/OMI_HEALTH_CHL_GLOBAL_OCEANCOLOUR_oligo_npg_area_mean.png"  # noqa
        # )
        assert (
            json_result["products"][1]["title"]
            == "North Pacific Gyre Area Chlorophyll-a time series and trend from Observations Reprocessing"  # noqa
        )

    def when_I_run_copernicus_marine_describe_including_datasets(self):
        command = [
            "copernicusmarine",
            "describe",
            "--exclude-fields",
            "keywords,description",
        ]
        self.output = execute_in_terminal(command, timeout_second=30)

    def then_I_can_read_it_does_not_contain_weird_symbols(self):
        assert "__" not in self.output.stdout
        # TODO: remove this check after they are fixed
        # assert " _" not in self.output.stdout
        # assert "_ " not in self.output.stdout
        assert '"_' not in self.output.stdout
        assert '_"' not in self.output.stdout

    def then_I_can_read_the_json_including_datasets(self):
        json_result = loads(self.output.stdout)
        assert len(json_result["products"]) >= 270
        seen_processing_level = False
        seen_digital_object_identifier = False
        for product in json_result["products"]:
            assert product["title"] is not None
            assert product["product_id"] is not None
            assert product["thumbnail_url"] is not None
            seen_digital_object_identifier = (
                seen_digital_object_identifier
                or ("digital_object_identifier" in product)
            )
            assert product["sources"] is not None
            seen_processing_level = (
                seen_processing_level or "processing_level" in product
            )
            assert product["production_center"] is not None
            assert "datasets" in product
            assert product[
                "datasets"
            ], f"No datasets found for product {product['product_id']}"
            for dataset in product["datasets"]:
                assert dataset["dataset_id"] is not None
                assert dataset["dataset_name"] is not None
                version_labels = [x["label"] for x in dataset["versions"]]
                assert len(version_labels) == len(set(version_labels))
                for version in dataset["versions"]:
                    assert re.match(
                        rf"({VERSION_DEFAULT}|{REGEX_PATTERN_DATE_YYYYMM})",
                        version["label"],
                    )
                    parts = version["parts"]
                    assert len(parts) != 0
                    part_names = [x["name"] for x in version["parts"]]
                    assert len(part_names) == len(set(part_names))
                    for part in parts:
                        assert part["name"] is not None
                        assert part["name"] != ""
                        services = part["services"]
                        assert len(services) != 0, dataset["dataset_id"]
                        service_names = [x["service_name"] for x in services]
                        assert len(service_names) == len(set(service_names))
                        if (
                            CopernicusMarineServiceNames.OMI_ARCO.value  # noqa
                            in service_names
                        ):
                            assert (
                                CopernicusMarineServiceNames.GEOSERIES.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineServiceNames.TIMESERIES.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineServiceNames.STATIC_ARCO.value  # noqa
                                not in service_names
                            )
                        if (
                            CopernicusMarineServiceNames.STATIC_ARCO.value  # noqa
                            in service_names
                        ):
                            assert (
                                CopernicusMarineServiceNames.GEOSERIES.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineServiceNames.TIMESERIES.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineServiceNames.OMI_ARCO.value  # noqa
                                not in service_names
                            )
                        if service_names in (
                            CopernicusMarineServiceNames.GEOSERIES,
                            CopernicusMarineServiceNames.TIMESERIES,
                        ):
                            assert (
                                CopernicusMarineServiceNames.OMI_ARCO.value  # noqa
                                not in service_names
                            )
                            assert (
                                CopernicusMarineServiceNames.STATIC_ARCO.value  # noqa
                                not in service_names
                            )
        assert seen_processing_level
        assert seen_digital_object_identifier

    def when_I_run_copernicus_marine_describe_with_product_id_and_dataset_id(
        self, product_id, dataset_id, exclude=None
    ):
        command = ["copernicusmarine", "describe", "--return-fields", "all"]
        if product_id:
            command.extend(["--product-id", product_id])
        if dataset_id:
            command.extend(["--dataset-id", dataset_id])
        if exclude:
            command.extend(["--exclude-fields", exclude])
        self.output = execute_in_terminal(command, timeout_second=30)

    def then_I_have_only_one_product(self):
        json_result = loads(self.output.stdout)
        assert len(json_result["products"]) == 1

    def then_I_have_only_one_product_and_one_dataset(self):
        json_result = loads(self.output.stdout)
        assert len(json_result["products"]) == 1
        assert len(json_result["products"][0]["datasets"]) == 1

    def then_I_have_an_error_message_about_dataset_id_and_product_id(self):
        assert self.output.returncode == 1
        assert "Dataset is not part of the product" in self.output.stderr

    def when_I_use_staging_environment_in_debug_logging_level(self):
        command = [
            "copernicusmarine",
            "describe",
            "--staging",
            "--log-level",
            "DEBUG",
        ]
        self.output = execute_in_terminal(command, safe_quoting=True)

    def then_I_check_that_the_urls_contains_only_dta(self):
        assert (
            "https://s3.waw3-1.cloudferro.com/mdl-metadata/"
            not in self.output.stdout
        )

    def when_I_run_copernicus_marine_describe_with_return_fields(self):
        command = [
            "copernicusmarine",
            "describe",
            "-i",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
            "--return-fields",
            "product_id,datasets",
            "--exclude-fields",
            "services",
        ]
        self.output = execute_in_terminal(command, timeout_second=10)

    def then_only_the_queried_fields_are_returned(self):
        json_result = loads(self.output.stdout)
        for product in json_result["products"]:
            assert set(product.keys()) == {"product_id", "datasets"}
            for dataset in product["datasets"]:
                assert set(dataset.keys()) == {
                    "dataset_id",
                    "versions",
                    "dataset_name",
                    "digital_object_identifier",
                }
                for version in dataset["versions"]:
                    assert set(version.keys()) == {"parts", "label"}
                    for part in version["parts"]:
                        assert "services" not in set(part.keys())

    def test_describe_fails_or_warns_with_wrong_return_fields(self):
        # Test with one wrong invalid return fields
        return_fields = "product_id,datasets, invalid_field"
        self.when_I_describe_with_invalid_return_fields(
            include_fields=return_fields
        )
        assert self.output.returncode == 0
        assert (
            "Some ``--return-fields`` fields are invalid: invalid_field"
            in self.output.stderr
        )

        # Test with multiple all invalid return fields
        return_fields = "invalid_field1, invalid_field2"
        self.when_I_describe_with_invalid_return_fields(
            include_fields=return_fields
        )
        assert self.output.returncode == 1
        assert (
            "All ``--return-fields`` fields are invalid: "
            "invalid_field1, invalid_field2" in self.output.stderr
        )

        # Test with one wrong invalid exclude fields
        exclude_fields = "product_id,wrong_field"
        self.when_I_describe_with_invalid_return_fields(
            exclude_fields=exclude_fields
        )
        assert self.output.returncode == 0
        assert (
            "Some ``--exclude-fields`` fields are invalid: wrong_field"
            in self.output.stderr
        )

        # Test with multiple all invalid exclude fields
        exclude_fields = "wrong_field1, wrong_field2"
        self.when_I_describe_with_invalid_return_fields(
            exclude_fields=exclude_fields
        )
        assert self.output.returncode == 1
        assert (
            "All ``--exclude-fields`` fields are invalid: "
            "wrong_field1, wrong_field2" in self.output.stderr
        )

    def when_I_describe_with_invalid_return_fields(
        self, include_fields: str = "", exclude_fields: str = ""
    ):
        command = [
            "copernicusmarine",
            "describe",
            "-i",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
        ]
        if include_fields:
            command.extend(["--return-fields", include_fields])
        if exclude_fields:
            command.extend(["--exclude-fields", exclude_fields])
        self.output = execute_in_terminal(command)

    # ######################
    # Python API tests
    # ######################
    def test_describe_function(self):
        describe_result = describe()
        assert describe_result is not None
        assert isinstance(describe_result, CopernicusMarineCatalogue)

    def test_describe_function_with_contains(self):
        nwshelf_catalog = describe(contains=["NWSHELF"])
        assert (
            len([product.product_id for product in nwshelf_catalog.products])
            == 8
        )
