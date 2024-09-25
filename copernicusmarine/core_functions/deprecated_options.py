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


DEPRECATED_OPTIONS: DeprecatedOptionMapping = DeprecatedOptionMapping([])
