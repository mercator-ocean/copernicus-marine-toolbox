import logging

import pytest
import responses
from requests.exceptions import HTTPError

from copernicusmarine import DatasetNotFound, ProductNotFound, describe, get
from copernicusmarine.core_functions.marine_datastore_config import (
    MARINE_DATASTORE_CONFIG_URL_CDN,
    MARINE_DATASTORE_CONFIG_URL_DIRECT,
)

# noqa: E501


class TestSeveralMetadataCatalogues:
    def test_can_select_dataset_from_two_catalogues_and_well_sorted(
        self, caplog
    ):
        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            dataset_id = "cmems_mod_glo_phy_anfc_0.083deg_P1D-m"

            result = get(dataset_id=dataset_id, dry_run=True, staging=True)

            assert "s3.waw3-1.cloudferro.com" in result.files[0].https_url
            assert (
                "https://s3.waw4-1.cloudferro.com/mdl-metadata/metadata/GLOBAL_ANALYSISFORECAST_PHY_001_024/cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m_202406/dataset.stac.json"
                not in caplog.text
            )

    def test_can_get_something_not_on_the_other_catalogue(self):
        dataset_id = "cmems_mod_arc_phy_anfc_6km_detided_P1D-m"

        result = get(dataset_id=dataset_id, dry_run=True, staging=True)

        assert "s3.waw3-1.cloudferro.com" in result.files[0].https_url

    @responses.activate
    def test_that_if_a_catalogue_fails_to_load_it_does_not_crash(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
            responses.mock.passthru_prefixes = ("http://", "https://")
            responses.add(
                responses.GET,
                "https://s3.waw3-1.cloudferro.com/mdl-metadata-dta/dataset_product_id_mapping.json",
                json=None,
                status=400,
            )
            result = get(dataset_id=dataset_id, dry_run=True, staging=True)
            assert (
                "Error while fetching dataset metadata from https://s3.waw3-1.cloudferro.com/mdl-metadata-dta/metadata: 400 Client Error"
                in caplog.text
            )
            assert "s3.waw4-1.cloudferro.com" in result.files[0].https_url

    @responses.activate
    def test_both_catalogues_failed(self):
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
        responses.mock.passthru_prefixes = ("http://", "https://")
        responses.add(
            responses.GET,
            "https://s3.waw3-1.cloudferro.com/mdl-metadata-dta/dataset_product_id_mapping.json",
            json=None,
            status=400,
        )
        responses.add(
            responses.GET,
            "https://s3.waw4-1.cloudferro.com/mdl-metadata/dataset_product_id_mapping.json",
            json=None,
            status=400,
        )

        with pytest.raises(HTTPError) as e:
            get(dataset_id=dataset_id, dry_run=True, staging=True)
        assert "400" in str(e)

    def test_raises_when_wrong_dataset_id(self):
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m_wrong"

        with pytest.raises(DatasetNotFound):
            get(dataset_id=dataset_id, dry_run=True, staging=True)

    def test_describe_works(self):
        full_describe = describe(staging=True)
        full_describe_json = full_describe.model_dump_json()
        assert "s3.waw4-1.cloudferro.com" in full_describe_json
        assert "s3.waw3-1.cloudferro.com" in full_describe_json
        product_ids = set()
        dataset_ids = set()
        for product in full_describe.products:
            assert product.product_id not in product_ids
            product_ids.add(product.product_id)
            for dataset in product.datasets:
                assert dataset.dataset_id not in dataset_ids
                dataset_ids.add(dataset.dataset_id)

        # dataset only in WAW4-1
        one_dataset_describe = describe(
            dataset_id="cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
            staging=True,
        )
        assert len(one_dataset_describe.products) == 1
        assert len(one_dataset_describe.products[0].datasets) == 1
        assert (
            "s3.waw3-1.cloudferro.com"
            not in one_dataset_describe.model_dump_json(
                exclude={
                    "products": {
                        "__all__": {
                            "thumbnail_url",
                        }
                    }
                }
            )
        )

        one_product_describe = describe(
            product_id="ARCTIC_ANALYSISFORECAST_PHY_002_001",
            staging=True,
        )
        assert len(one_product_describe.products) == 1
        assert one_product_describe.products[0].datasets
        assert (
            "s3.waw4-1.cloudferro.com"
            not in one_product_describe.model_dump_json()
        )

    def test_describe_fails_wrong_ids(self):
        with pytest.raises(ProductNotFound):
            describe(product_id="WRONG_PRODUCT_ID")

        with pytest.raises(DatasetNotFound):
            describe(dataset_id="WRONG_DATASET_ID")

    @responses.activate
    def test_describe_fails_for_any_error(self):
        responses.mock.passthru_prefixes = ("http://", "https://")
        responses.add(
            responses.GET,
            "https://s3.waw4-1.cloudferro.com/mdl-metadata/metadata/catalog.stac.json",
            json=None,
            status=400,
        )
        with pytest.raises(HTTPError) as e:
            describe(staging=True)
        assert "400" in str(e.value)

    @responses.activate
    def test_cdn_works_on_prod(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="copernicusmarine"):
            responses.mock.passthru_prefixes = ("http://", "https://")
            responses.add(
                responses.GET,
                MARINE_DATASTORE_CONFIG_URL_DIRECT,
                json=None,
                status=400,
            )
            _ = describe(
                dataset_id="cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
            )

            assert MARINE_DATASTORE_CONFIG_URL_CDN in caplog.text
