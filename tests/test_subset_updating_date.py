import logging
from unittest import mock

import pytest

from copernicusmarine import subset
from copernicusmarine.core_functions.exceptions import DatasetUpdating
from tests.resources.mock_stac_catalog.marine_data_store_stac_metadata_mock import (
    mocked_stac_requests_get,
)

error_message = (
    "The dataset cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"
    ", version '202406', part 'default' is currently being updated."
    " Data after 2024-05-01T00:00:00Z may not be up to date."
)


class TestSubsetUpdatingDate:
    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_raise(self, snapshot):
        with pytest.raises(DatasetUpdating) as e:
            subset(
                dataset_id="cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
                start_datetime="2024-01-01",
                end_datetime="2024-05-10",
                raise_if_updating=True,
            )
        assert str(e.value) == error_message

    @mock.patch(
        "requests.Session.get",
        side_effect=mocked_stac_requests_get,
    )
    def test_no_raise(self, snapshot, caplog):
        with caplog.at_level(logging.INFO):
            subset(
                dataset_id="cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
                variables=["vo"],
                start_datetime="2024-01-01",
                end_datetime="2024-05-10",
                raise_if_updating=False,
                dry_run=True,
            )
            assert error_message in caplog.text
