from numpydoc.docscrape import FunctionDoc

import copernicusmarine
from copernicusmarine.core_functions import documentation_utils

LIST_OF_EXCEPTIONS = ["username", "password", "**kwargs"]
PARTS_TO_IGNORE = ["Mutually exclusive", "Requires to set"]


def clean_description(desc: list[str]) -> list[str]:
    cleaned_desc = []
    for line in desc:
        if any(line.startswith(part) for part in PARTS_TO_IGNORE):
            continue
        cleaned_desc.append(line)
    return cleaned_desc


class TestDocumentation:
    def test_subset(self):
        text_subset = FunctionDoc(copernicusmarine.subset)

        for parameter in text_subset["Parameters"]:
            name_of_variable = parameter.name
            parameter_desc = clean_description(parameter.desc)
            if name_of_variable == "variables":
                assert parameter_desc == ["List of variable names to extract."]
                continue
            if name_of_variable == "platform_ids":
                assert parameter_desc == [
                    "List of platform IDs to extract. "
                    "Only available for platform chunked datasets."
                ]
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                documentation_utils.SUBSET[name_of_variable.upper() + "_HELP"]
            ]

    def test_get(self):
        text_get = FunctionDoc(copernicusmarine.get)

        for parameter in text_get["Parameters"]:
            parameter_desc = clean_description(parameter.desc)
            name_of_variable = parameter.name
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                documentation_utils.GET[name_of_variable.upper() + "_HELP"]
            ]

    def test_login(self):
        text_login = FunctionDoc(copernicusmarine.login)

        for parameter in text_login["Parameters"]:
            parameter_desc = clean_description(parameter.desc)
            name_of_variable = parameter.name
            if len(parameter_desc) > 1:
                assert (
                    " ".join(parameter_desc)
                    == documentation_utils.LOGIN[
                        name_of_variable.upper() + "_HELP"
                    ]
                )
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                documentation_utils.LOGIN[name_of_variable.upper() + "_HELP"]
            ]

    def test_describe(self):
        text_describe = FunctionDoc(copernicusmarine.describe)

        for parameter in text_describe["Parameters"]:
            parameter_desc = clean_description(parameter.desc)
            name_of_variable = parameter.name
            assert parameter_desc == [
                documentation_utils.DESCRIBE[
                    name_of_variable.upper() + "_HELP"
                ]
            ]

    def test_open_dataset(self):
        text_open_dataset = FunctionDoc(copernicusmarine.open_dataset)

        for parameter in text_open_dataset["Parameters"]:
            parameter_desc = clean_description(parameter.desc)
            name_of_variable = parameter.name
            if name_of_variable == "variables":
                assert parameter_desc == ["List of variable names to extract."]
                continue
            if name_of_variable == "dataset_id":
                assert parameter_desc == ["The datasetID, required."]
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                documentation_utils.SUBSET[name_of_variable.upper() + "_HELP"]
            ]

    def test_read_dataframe(self):
        text_read_dataframe = FunctionDoc(copernicusmarine.read_dataframe)

        for parameter in text_read_dataframe["Parameters"]:
            parameter_desc = clean_description(parameter.desc)
            name_of_variable = parameter.name
            if name_of_variable == "variables":
                assert parameter_desc == ["List of variable names to extract."]
                continue
            if name_of_variable == "platform_ids":
                assert parameter_desc == [
                    "List of platform IDs to extract. "
                    "Only available for platform chunked datasets."
                ]
                continue
            if name_of_variable == "dataset_id":
                assert parameter_desc == ["The datasetID, required."]
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue

            assert parameter_desc == [
                documentation_utils.SUBSET[name_of_variable.upper() + "_HELP"]
            ]

    def test_subset_split_on(self):
        text_subset_split_on = FunctionDoc(copernicusmarine.subset_split_on)

        for parameter in text_subset_split_on["Parameters"]:
            parameter_desc = clean_description(parameter.desc)
            name_of_variable = parameter.name
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                documentation_utils.SUBSET_SPLIT_ON[
                    name_of_variable.upper() + "_HELP"
                ]
            ]
