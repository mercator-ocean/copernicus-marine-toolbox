from unittest import mock

import pytest

from copernicusmarine import CopernicusMarineCatalogue, subset
from copernicusmarine.core_functions.fields_query_builder import build_query
from tests.resources.mock_stac_catalog.marine_data_store_stac_metadata_mock import (
    mocked_stac_requests_get,
)

exclude_query = build_query(
    {"description", "keywords"}, CopernicusMarineCatalogue
)


class TestSubsetUpdatingDate:
    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_raise(self, snapshot):
        with pytest.raises(ValueError) as e:
            subset(
                dataset_id="cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
                start_datetime="2024-01-01",
                end_datetime="2024-05-10",
                raise_if_updating=True,
            )
            assert str(e.value) == (
                "The dataset cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"
                ", version '202211', part 'default' is currently being updated."
                " Data after 2024-05-01T00:00:00Z may not be up to date."
            )
