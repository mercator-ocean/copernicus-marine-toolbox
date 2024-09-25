from collections.abc import Iterator, Mapping
from typing import Dict, List


class DeprecatedOption:
    def __init__(self, old_name, new_name, replace=True) -> None:
        self.old_name = old_name
        self.new_name = new_name
        self.replace = replace


class DeprecatedOptionMapping(Mapping):
    def __init__(self, deprecated_options: List[DeprecatedOption]) -> None:
        self.deprecated_options_by_old_names: Dict = {}
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
