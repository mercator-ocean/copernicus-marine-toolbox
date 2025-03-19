from copernicusmarine.catalogue_parser.models import (
    PART_DEFAULT,
    VERSION_DEFAULT,
    CopernicusMarineDataset,
    CopernicusMarinePart,
    CopernicusMarineVersion,
)

version_default = CopernicusMarineVersion(
    label=VERSION_DEFAULT,
    parts=[
        CopernicusMarinePart(
            name="latest",
            services=[],
            retired_date=None,
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name="latest_to_be_released",
            services=[],
            retired_date=None,
            released_date="2060-01-01",
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name="bathy",
            services=[],
            retired_date=None,
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name="history",
            services=[],
            retired_date=None,
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name="latest_will_be_retired_in_2060",
            services=[],
            retired_date="2060-01-01",
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name="latest_will_be_retired_in_2030",
            services=[],
            retired_date="2030-01-01",
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name="monthly",
            services=[],
            retired_date=None,
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
    ],
)

to_be_released_version = CopernicusMarineVersion(
    label="206011",
    parts=[
        CopernicusMarinePart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date="2060-01-01",
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
    ],
)

will_be_retired_soon_version = CopernicusMarineVersion(
    label="202011",
    parts=[
        CopernicusMarinePart(
            name=PART_DEFAULT,
            services=[],
            retired_date="2025-01-01",
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
    ],
)

will_be_retired_in_a_long_time_version = CopernicusMarineVersion(
    label="202111",
    parts=[
        CopernicusMarinePart(
            name=PART_DEFAULT,
            services=[],
            retired_date="2060-01-01",
            released_date=None,
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
    ],
)

newly_released_version = CopernicusMarineVersion(
    label="202201",
    parts=[
        CopernicusMarinePart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date="2022-01-01",
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
        CopernicusMarinePart(
            name="to_be_released_part",
            services=[],
            retired_date=None,
            released_date="2060-01-01",
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
    ],
)

old_version = CopernicusMarineVersion(
    label="201901",
    parts=[
        CopernicusMarinePart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date="2019-01-01",
            arco_updating_start_date=None,
            arco_updated_date=None,
            url_metadata="https://example.com",
        ),
    ],
)


example_dataset = CopernicusMarineDataset(
    dataset_id="example_dataset",
    dataset_name="Example Dataset",
    versions=[
        version_default,
        to_be_released_version,
        newly_released_version,
        old_version,
        will_be_retired_soon_version,
        will_be_retired_in_a_long_time_version,
    ],
)


class TestVersionsPartsSorting:
    def test_sort_versions(self):
        example_dataset.sort_versions()
        assert [version.label for version in example_dataset.versions] == [
            "202201",
            "201901",
            VERSION_DEFAULT,
            "202111",
            "202011",
            "206011",
        ]

    def test_sort_parts(self):
        version_default.sort_parts()
        assert [part.name for part in version_default.parts] == [
            PART_DEFAULT,
            "latest",
            "bathy",
            "history",
            "monthly",
            "latest_will_be_retired_in_2060",
            "latest_will_be_retired_in_2030",
            "latest_to_be_released",
        ]
