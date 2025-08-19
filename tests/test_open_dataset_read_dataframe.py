import inspect
import logging
import os
from datetime import datetime, timezone

import pytest

from copernicusmarine import (
    describe,
    get,
    login,
    open_dataset,
    read_dataframe,
    subset,
)
from copernicusmarine.download_functions.utils import (
    timestamp_or_datestring_to_datetime,
)


class TestOpenDatasetAndReadDataFrame:
    def test_open_dataset(self):
        dataset = open_dataset(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
        )
        assert dataset is not None

    def test_read_dataframe(self):
        dataframe = read_dataframe(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
        )
        assert dataframe is not None

    def test_signature_inspection_is_working(self):
        assert inspect.signature(describe).parameters["contains"]

        common_key_parameter = "username"
        assert inspect.signature(login).parameters[common_key_parameter]
        assert inspect.signature(get).parameters[common_key_parameter]
        assert inspect.signature(get).parameters[common_key_parameter]
        assert inspect.signature(subset).parameters[common_key_parameter]
        assert inspect.signature(open_dataset).parameters[common_key_parameter]
        assert inspect.signature(read_dataframe).parameters[
            common_key_parameter
        ]

    def test_iso8601_datetime_format_as_string(
        self,
    ):
        dataset = open_dataset(
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime="2023-09-15T00:00:00.000Z",
            end_datetime="2023-09-20T00:00:00.000Z",
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
            vertical_axis="elevation",
        )
        assert dataset is not None
        assert (
            dataset.so.sel(
                latitude=0,
                longitude=0.2,
                elevation=0,
                time=datetime.strptime(
                    "2023-09-15T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                method="nearest",
            ).size
            == 1
        )

    def test_open_dataset_with_strict_method(self, caplog):
        dataset_id = "cmems_mod_glo_phy-cur_anfc_0.083deg_PT6H-i"
        start_datetime = "2023-09-15T00:00:00.000Z"
        end_datetime = "2023-09-15T00:00:00.000Z"
        coordinates_selection_method = "strict-inside"

        dataset = open_dataset(
            dataset_id=dataset_id,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            coordinates_selection_method=coordinates_selection_method,
        )

        assert dataset.coords is not None
        assert "ERROR" not in caplog.text

    def test_read_dataframe_with_strict_method(self, caplog):
        dataframe = read_dataframe(
            dataset_id="cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
            start_datetime=datetime(year=2024, month=1, day=1),
            end_datetime=datetime(year=2024, month=1, day=2),
            minimum_latitude=0.0,
            maximum_latitude=0.1,
            minimum_longitude=0.2,
            maximum_longitude=0.3,
            coordinates_selection_method="strict-inside",
        )

        assert dataframe is not None
        assert dataframe.size > 0
        assert "ERROR" not in caplog.text

    def test_open_dataset_with_retention_date(self):
        dataset = open_dataset(
            dataset_id="cmems_obs-oc_atl_bgc-plankton_nrt_l4-gapfree-multi-1km_P1D",
        )
        assert timestamp_or_datestring_to_datetime(
            dataset.time.values.min()
        ) >= datetime(2024, 8, 31, 0, 0, 0, tzinfo=timezone.utc)

    def test_open_dataset_with_retention_date_and_only_values_in_metadata(
        self,
    ):
        dataset = open_dataset(
            dataset_id="cmems_obs-oc_atl_bgc-pp_nrt_l4-multi-1km_P1M",
        )
        assert timestamp_or_datestring_to_datetime(
            dataset.time.values.min()
        ) >= datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    def test_inverted_min_max_raises_error_or_warning(self, caplog):
        dataset_id = "cmems_obs-oc_atl_bgc-pp_nrt_l4-multi-1km_P1M"
        with caplog.at_level(logging.WARNING):
            open_dataset(
                dataset_id=dataset_id,
                minimum_longitude=10.0,
                maximum_longitude=5.0,
            )
            assert (
                "Minimum longitude greater than maximum longitude"
                in caplog.text
            )
        with pytest.raises(
            ValueError,
            match="Minimum latitude greater than maximum latitude",
        ):
            open_dataset(
                dataset_id=dataset_id,
                minimum_latitude=1.0,
                maximum_latitude=0.0,
            )
        with pytest.raises(
            ValueError,
            match="Minimum depth greater than maximum depth",
        ):
            open_dataset(
                dataset_id=dataset_id,
                minimum_depth=10.0,
                maximum_depth=5.0,
            )
        with pytest.raises(
            ValueError,
            match="Start datetime greater than end datetime",
        ):
            open_dataset(
                dataset_id=dataset_id,
                start_datetime="2024-01-01T00:00:00Z",
                end_datetime="2023-12-31T23:59:59Z",
            )
