from datetime import datetime, timezone

from freezegun import freeze_time

from copernicusmarine.catalogue_parser.models import (
    get_version_from_dataset_id,
)
from copernicusmarine.core_functions.utils import (
    datetime_parser,
    human_readable_size,
    timestamp_parser,
)


class TestUtilityFunctions:
    @freeze_time("2012-01-14 03:21:34", tz_offset=-2)
    def test_datetime_parser(self):
        # all parsed dates are in UTC
        assert datetime_parser("now") == datetime(
            2012, 1, 14, 1, 21, 34, tzinfo=timezone.utc
        )
        assert datetime_parser("2012-01-14T03:21:34.000000+02:00") == datetime(
            2012, 1, 14, 1, 21, 34, tzinfo=timezone.utc
        )

        # All format are supported
        assert datetime_parser("2012") == datetime(
            2012, 1, 1, 0, 0, 0, tzinfo=timezone.utc
        )
        assert datetime_parser("2012-01-14") == datetime(
            2012, 1, 14, 0, 0, 0, tzinfo=timezone.utc
        )
        assert datetime_parser("2012-01-14T03:21:34") == datetime(
            2012, 1, 14, 3, 21, 34, tzinfo=timezone.utc
        )
        assert datetime_parser("2012-01-14 03:21:34") == datetime(
            2012, 1, 14, 3, 21, 34, tzinfo=timezone.utc
        )
        assert datetime_parser("2012-01-14T03:21:34.000000") == datetime(
            2012, 1, 14, 3, 21, 34, tzinfo=timezone.utc
        )
        assert datetime_parser("2012-01-14T03:21:34.000000Z") == datetime(
            2012, 1, 14, 3, 21, 34, tzinfo=timezone.utc
        )

    def test_timestamp_parser(self):
        assert timestamp_parser(-630633600000) == datetime(
            1950, 1, 7, 0, 0, 0, tzinfo=timezone.utc
        )
        assert timestamp_parser(0) == datetime(
            1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc
        )
        assert timestamp_parser(1672527600000) == datetime(
            2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc
        )
        assert timestamp_parser(1672527600, unit="s") == datetime(
            2022, 12, 31, 23, 0, 0, tzinfo=timezone.utc
        )

    def test_human_readable_size_bytes(self):
        assert human_readable_size(0) == "0.00 B"
        assert human_readable_size(0.0000001) == "0.10 B"

    def test_human_readable_size_kilobytes(self):
        assert human_readable_size(1 / 1024) == "1.00 KB"
        assert human_readable_size(0.5 / 1024) == "512.00 B"

    def test_human_readable_size_megabytes(self):
        assert human_readable_size(1) == "1.00 MB"
        assert human_readable_size(0.5) == "512.00 KB"
        assert human_readable_size(999) == "999.00 MB"

    def test_human_readable_size_gigabytes(self):
        assert human_readable_size(1024) == "1.00 GB"
        assert human_readable_size(2048) == "2.00 GB"
        assert human_readable_size(1536) == "1.50 GB"

    def test_human_readable_size_terabytes(self):
        assert human_readable_size(1024 * 1024) == "1.00 TB"
        assert human_readable_size(2 * 1024 * 1024) == "2.00 TB"

    def test_human_readable_size_petabytes(self):
        assert human_readable_size(1024 * 1024 * 1024) == "1.00 PB"
        assert (
            human_readable_size(2 * 1024 * 1024 * 1024 * 1024 * 1024)
            == "2097152.00 PB"
        )

    def test_parse_properly_version_from_dataset_id(self):
        dataset_id = "cmems_mod_ibi_phy-temp_my_0.027deg_P1D-m_202511"
        (
            dataset_id_without_version,
            dataset_version,
        ) = get_version_from_dataset_id(dataset_id, raise_on_error=True)
        assert (
            dataset_id_without_version
            == "cmems_mod_ibi_phy-temp_my_0.027deg_P1D-m"
        )
        assert dataset_version == "202511"

        dataset_id_no_version = "cmems_mod_ibi_phy-temp_my_0.027deg_P1D-m"
        (
            dataset_id_without_version,
            dataset_version,
        ) = get_version_from_dataset_id(
            dataset_id_no_version, raise_on_error=True
        )
        assert (
            dataset_id_without_version
            == "cmems_mod_ibi_phy-temp_my_0.027deg_P1D-m"
        )
        assert dataset_version is None

        dataset_id_with_part = (
            "cmems_obs-ins_med_phybgcwav_mynrt_na_irr_202311--ext--monthly"
        )
        (
            dataset_id_without_version,
            dataset_version,
        ) = get_version_from_dataset_id(
            dataset_id_with_part, raise_on_error=True
        )
        assert dataset_id_without_version == dataset_id_with_part
        assert dataset_version is None
