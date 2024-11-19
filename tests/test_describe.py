import re
from json import loads

from copernicusmarine import (
    CopernicusMarineCatalogue,
    CopernicusMarineServiceNames,
    describe,
)
from copernicusmarine.catalogue_parser.models import (
    PART_DEFAULT,
    PART_ORIGINAL,
    REGEX_PATTERN_DATE_YYYYMM,
    VERSION_DEFAULT,
)
from tests.test_utils import execute_in_terminal


class TestDescribe:
    # CLI (Command Line Interface) tests
    def test_describe_default(self):
        self.when_I_run_copernicus_marine_describe_with_default_arguments()
        self.then_stdout_can_be_load_as_json()
        self.then_I_can_read_the_default_json()
        self.and_there_are_no_warnings_about_backend_versions()

    def test_describe_return_fields_datasets(self, snapshot):
        self.when_I_run_copernicus_marine_describe_including_datasets()
        self.then_I_can_read_it_does_not_contain_weird_symbols()
        self.then_I_can_read_the_json_including_datasets()
        self.then_omi_services_are_not_in_the_catalog()
        self.then_products_from_marine_data_store_catalog_are_available()
        self.then_datasets_variables_are_correct(snapshot)
        self.then_all_dataset_parts_are_filled()

    def test_describe_product_id_dataset_id(self):
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

    def test_describe_contains_option(self):
        self.when_I_run_copernicus_marine_describe_with_contains_option()
        self.then_I_can_read_the_filtered_json()

    def test_describe_with_staging_flag(self):
        self.when_I_use_staging_environment_in_debug_logging_level()
        self.then_I_check_that_the_urls_contains_only_dta()

    def test_describe_function_with_return_fields(self):
        self.when_I_run_copernicus_marine_describe_with_return_fields()
        self.then_stdout_can_be_load_as_json()
        self.then_only_the_queried_fields_are_returned()

    def test_describe_exclude_datasets(self):
        product_id = "GLOBAL_MULTIYEAR_PHY_001_030"
        self.when_I_run_copernicus_marine_describe_with_product_id_and_dataset_id(
            product_id, None, exclude="services"
        )
        json_result = loads(self.output.stdout.decode("utf-8"))
        for product in json_result["products"]:
            for dataset in product["datasets"]:
                for version in dataset["versions"]:
                    for part in version["parts"]:
                        assert "services" not in part

    def when_I_run_copernicus_marine_describe_with_default_arguments(self):
        command = ["copernicusmarine", "describe"]
        self.output = execute_in_terminal(command, timeout_second=30)

    def then_stdout_can_be_load_as_json(self):
        loads(self.output.stdout.decode("utf-8"))

    def then_I_can_read_the_default_json(self):
        json_result = loads(self.output.stdout.decode("utf-8"))
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
            b"Please update to the latest client version."
            not in self.output.stderr
        )

    def then_omi_services_are_not_in_the_catalog(self):
        json_result = loads(self.output.stdout)
        for product in json_result["products"]:
            for dataset in product["datasets"]:
                for version in dataset["versions"]:
                    for part in version["parts"]:
                        assert "omi" not in list(
                            map(
                                lambda x: x["service_name"],
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
            "wmts",
        ]

        json_result = loads(self.output.stdout)
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
                lambda x: x["service_name"],
                dataset["versions"][0]["parts"][0]["services"],
            )
        )
        assert all(
            map(lambda x: x in expected_services, expected_dataset_services)
        )

    def then_datasets_variables_are_correct(self, snapshot):
        expected_product_id = "GLOBAL_MULTIYEAR_PHY_ENS_001_031"
        expected_dataset_id = "cmems_mod_glo_phy-all_my_0.25deg_P1D-m"
        wanted_services = [
            "original-files",
            "arco-geo-series",
            "arco-time-series",
        ]
        json_result = loads(self.output.stdout)
        expected_product = list(
            filter(
                lambda product: product["product_id"] == expected_product_id,
                json_result["products"],
            )
        )
        product = expected_product[0]
        product_datasets = product["datasets"]
        expected_dataset = list(
            filter(
                lambda product: product["dataset_id"] == expected_dataset_id,
                product_datasets,
            )
        )
        dataset = expected_dataset[0]
        wanted_services_in_dataset = list(
            filter(
                lambda x: x["service_name"] in wanted_services,
                dataset["versions"][0]["parts"][0]["services"],
            )
        )
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
        command = [
            "copernicusmarine",
            "describe",
            "--exclude-fields",
            "keywords,description",
        ]
        self.output = execute_in_terminal(command, timeout_second=30)

    def then_I_can_read_it_does_not_contain_weird_symbols(self):
        assert b"__" not in self.output.stdout
        assert b" _" not in self.output.stdout
        assert b"_ " not in self.output.stdout
        assert b'"_' not in self.output.stdout
        assert b'_"' not in self.output.stdout

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
                        has_originalGrid = (
                            len(
                                list(
                                    filter(
                                        lambda x: x["name"] == PART_ORIGINAL,
                                        parts,
                                    )
                                )
                            )
                            > 0
                        )
                        if has_originalGrid:
                            assert len(parts) == 2
                        else:
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
                                lambda x: x["service_name"],
                                services,
                            )
                        )
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
        self.output = execute_in_terminal(command, timeout_second=10)

    def then_I_have_only_one_product(self):
        json_result = loads(self.output.stdout)
        assert len(json_result["products"]) == 1

    def then_I_have_only_one_product_and_one_dataset(self):
        json_result = loads(self.output.stdout)
        assert len(json_result["products"]) == 1
        assert len(json_result["products"][0]["datasets"]) == 1

    def then_I_have_an_error_message_about_dataset_id_and_product_id(self):
        assert self.output.returncode == 1
        assert b"Dataset is not part of the product" in self.output.stderr

    def when_I_use_staging_environment_in_debug_logging_level(self):
        command = [
            "copernicusmarine",
            "describe",
            "--staging",
            "--log-level",
            "DEBUG",
        ]
        self.output = execute_in_terminal(command)

    def then_I_check_that_the_urls_contains_only_dta(self):
        assert (
            b"https://s3.waw3-1.cloudferro.com/mdl-metadata/"
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
                }
                for version in dataset["versions"]:
                    assert set(version.keys()) == {"parts", "label"}
                    for part in version["parts"]:
                        assert "services" not in set(part.keys())

    # ######################
    # Python API tests
    # ######################
    def test_describe_function(self):
        describe_result = describe()
        assert describe_result is not None
        assert isinstance(describe_result, CopernicusMarineCatalogue)

    def test_describe_function_with_contains(self):
        nwshelf_catalog = describe(contains=["NWSHELF"])
        assert len(nwshelf_catalog.products) == 7
