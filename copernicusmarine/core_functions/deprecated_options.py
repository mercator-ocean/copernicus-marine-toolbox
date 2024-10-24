from collections.abc import Iterator, Mapping
from typing import Dict, List, Optional


class DeprecatedOption:
    def __init__(
        self,
        old_name,
        new_name,
        replace=True,
        deprecated_for_v2=False,
        deleted_for_v2=True,
        only_for_v2=False,
        targeted_functions: Optional[list[str]] = None,
    ) -> None:
        self.old_name = old_name
        self.new_name = new_name
        self.replace = replace
        self.deprecated_for_v2 = deprecated_for_v2
        self.deleted_for_v2 = deleted_for_v2
        self.only_for_v2 = only_for_v2
        if not targeted_functions:
            self.targeted_functions = [
                "describe",
                "get",
                "subset",
                "login",
                "open_dataset",
                "read_dataframe",
            ]
        else:
            self.targeted_functions = targeted_functions


class DeprecatedOptionMapping(Mapping):
    def __init__(self, deprecated_options: List[DeprecatedOption]) -> None:
        self.deprecated_options_by_old_names: Dict[str, DeprecatedOption] = {}
        for value in deprecated_options:
            if value not in self.deprecated_options_by_old_names:
                self.deprecated_options_by_old_names[value.old_name] = value

    def __getitem__(self, __key: str) -> DeprecatedOption:
        return self.deprecated_options_by_old_names[__key]

    def __iter__(self) -> Iterator:
        return self.deprecated_options_by_old_names.__iter__()

    def __len__(self) -> int:
        return self.deprecated_options_by_old_names.__len__()

    @property
    def dict_old_names_to_new_names(self):
        result_dict = {}
        for (
            old_name,
            deprecated_option,
        ) in self.deprecated_options_by_old_names.items():
            if deprecated_option.replace:
                result_dict[old_name] = deprecated_option.new_name
        return result_dict


DEPRECATED_OPTIONS: DeprecatedOptionMapping = DeprecatedOptionMapping(
    [
        DeprecatedOption(
            old_name="minimal_longitude", new_name="minimum_longitude"
        ),
        DeprecatedOption(
            old_name="maximal_longitude", new_name="maximum_longitude"
        ),
        DeprecatedOption(
            old_name="minimal_latitude", new_name="minimum_latitude"
        ),
        DeprecatedOption(
            old_name="maximal_latitude", new_name="maximum_latitude"
        ),
        DeprecatedOption(old_name="minimal_depth", new_name="minimum_depth"),
        DeprecatedOption(old_name="maximal_depth", new_name="maximum_depth"),
        DeprecatedOption(
            old_name="force_dataset_version", new_name="dataset_version"
        ),
        DeprecatedOption(
            old_name="force_dataset_part", new_name="dataset_part"
        ),
        DeprecatedOption(old_name="force_service", new_name="service"),
        DeprecatedOption(
            old_name="service",
            new_name="service",
            replace=False,
            deleted_for_v2=True,
            only_for_v2=True,
            targeted_functions=["get"],
        ),
        DeprecatedOption(
            old_name="download_file_list",
            new_name="create_file_list",
            replace=False,
        ),
        DeprecatedOption(
            old_name="include_all_versions",
            new_name="include_versions",
        ),
        DeprecatedOption(
            old_name="skip_if_user_logged_in",
            new_name="skip_if_user_logged_in",
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="overwrite_metadata_cache",
            new_name="overwrite_metadata_cache",
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="no_metadata_cache",
            new_name="no_metadata_cache",
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="dataset_url",
            new_name="dataset_url",
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="subset_method",
            new_name="coordinates_selection_method",
            replace=False,
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="netcdf_compression_enabled",
            new_name="netcdf_compression_enabled",
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="disable_progress_bar",
            new_name="disable_progress_bar",
            targeted_functions=["open_dataset", "read_dataframe"],
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="include_datasets",
            new_name="include_datasets",
            deprecated_for_v2=True,
            deleted_for_v2=False,
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="include_description",
            new_name="include_description",
            deprecated_for_v2=True,
            deleted_for_v2=False,
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="include_keywords",
            new_name="include_keywords",
            deprecated_for_v2=True,
            deleted_for_v2=False,
            only_for_v2=True,
        ),
        DeprecatedOption(
            old_name="include_all",
            new_name="include_all",
            deprecated_for_v2=True,
            deleted_for_v2=False,
            only_for_v2=True,
        ),
    ]
)
