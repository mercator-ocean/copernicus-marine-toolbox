from copernicusmarine.catalogue_parser.catalogue_parser import (
    PART_DEFAULT,
    VERSION_DEFAULT,
    CopernicusMarineDatasetVersion,
    CopernicusMarineProductDataset,
    CopernicusMarineVersionPart,
)

version_default = CopernicusMarineDatasetVersion(
    label=VERSION_DEFAULT,
    parts=[
        CopernicusMarineVersionPart(
            name="latest", services=[], retired_date=None, released_date=None
        ),
        CopernicusMarineVersionPart(
            name="latest_to_be_released",
            services=[],
            retired_date=None,
            released_date="2060-01-01",
        ),
        CopernicusMarineVersionPart(
            name="bathy", services=[], retired_date=None, released_date=None
        ),
        CopernicusMarineVersionPart(
            name="history", services=[], retired_date=None, released_date=None
        ),
        CopernicusMarineVersionPart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date=None,
        ),
        CopernicusMarineVersionPart(
            name="latest_will_be_retired_in_2060",
            services=[],
            retired_date="2060-01-01",
            released_date=None,
        ),
        CopernicusMarineVersionPart(
            name="latest_will_be_retired_in_2030",
            services=[],
            retired_date="2030-01-01",
            released_date=None,
        ),
        CopernicusMarineVersionPart(
            name="monthly", services=[], retired_date=None, released_date=None
        ),
    ],
)

to_be_released_version = CopernicusMarineDatasetVersion(
    label="206011",
    parts=[
        CopernicusMarineVersionPart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date="2060-01-01",
        ),
    ],
)

will_be_retired_soon_version = CopernicusMarineDatasetVersion(
    label="202011",
    parts=[
        CopernicusMarineVersionPart(
            name=PART_DEFAULT,
            services=[],
            retired_date="2025-01-01",
            released_date=None,
        ),
    ],
)

will_be_retired_in_a_long_time_version = CopernicusMarineDatasetVersion(
    label="202111",
    parts=[
        CopernicusMarineVersionPart(
            name=PART_DEFAULT,
            services=[],
            retired_date="2060-01-01",
            released_date=None,
        ),
    ],
)

newly_released_version = CopernicusMarineDatasetVersion(
    label="202201",
    parts=[
        CopernicusMarineVersionPart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date="2022-01-01",
        ),
        CopernicusMarineVersionPart(
            name="to_be_released_part",
            services=[],
            retired_date=None,
            released_date="2060-01-01",
        ),
    ],
)

old_version = CopernicusMarineDatasetVersion(
    label="201901",
    parts=[
        CopernicusMarineVersionPart(
            name=PART_DEFAULT,
            services=[],
            retired_date=None,
            released_date="2019-01-01",
        ),
    ],
)


example_dataset = CopernicusMarineProductDataset(
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
        print([version.label for version in example_dataset.versions])
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
        print([part.name for part in version_default.parts])
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
