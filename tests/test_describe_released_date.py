from unittest import mock

from copernicusmarine import describe
from tests.resources.mock_stac_catalog.marine_data_store_stac_metadata_mock import (
    mocked_stac_aiohttp_get,
)


class TestDescribe:
    @mock.patch(
        "aiohttp.ClientSession.get",
        side_effect=mocked_stac_aiohttp_get,
    )
    def when_I_describe_the_marine_data_store(
        self,
        mock_get,
        include_versions=False,
    ):
        return describe(
            include_versions=include_versions,
            include_datasets=True,
        )

    def test_only_released_dataset_by_default(self, snapshot):
        describe_result = self.when_I_describe_the_marine_data_store()
        self.then_I_dont_get_the_not_released_products_version_and_datasets(
            describe_result, snapshot
        )

    def then_I_dont_get_the_not_released_products_version_and_datasets(
        self, describe_result, snapshot
    ):
        assert 1 == len(describe_result["products"])
        assert describe_result == snapshot

    def test_describe_all_versions(self, snapshot):
        describe_result = self.when_I_describe_the_marine_data_store(
            include_versions=True
        )
        self.then_I_get_all_products_versions_and_datasets(
            describe_result, snapshot
        )

    def then_I_get_all_products_versions_and_datasets(
        self, describe_result, snapshot
    ):
        assert 2 == len(describe_result["products"])
        assert describe_result == snapshot
