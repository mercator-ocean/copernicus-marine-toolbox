from numpydoc.docscrape import FunctionDoc

import copernicusmarine
from copernicusmarine.core_functions import documentation_utils

LIST_OF_EXCEPTIONS = ["username", "password"]


class TestDocumentation:
    def test_subset(self):
        text_subset = FunctionDoc(copernicusmarine.subset)

        for i in range(len(text_subset["Parameters"])):
            name_of_variable = text_subset["Parameters"][i].name
            if name_of_variable in ["start_datetime", "end_datetime"]:
                assert text_subset["Parameters"][i].desc == [
                    documentation_utils.SUBSET[
                        name_of_variable.upper() + "_HELP"
                    ]
                ]
                continue
            if name_of_variable == "variables":
                assert text_subset["Parameters"][i].desc == [
                    "List of variable names to extract."
                ]
                continue
            if name_of_variable == "platform_ids":
                assert text_subset["Parameters"][i].desc == [
                    "List of platform IDs to extract. "
                    "Only available for platform chunked datasets."
                ]
                continue
            if name_of_variable == "netcdf_compression_level":
                assert text_subset["Parameters"][i].desc == [
                    documentation_utils.SUBSET[
                        name_of_variable.upper() + "_HELP"
                    ]
                ]
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert text_subset["Parameters"][i].desc == [
                documentation_utils.SUBSET[name_of_variable.upper() + "_HELP"]
            ]

    def test_get(self):
        text_get = FunctionDoc(copernicusmarine.get)

        for i in range(len(text_get["Parameters"])):
            name_of_variable = text_get["Parameters"][i].name
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert text_get["Parameters"][i].desc == [
                documentation_utils.GET[name_of_variable.upper() + "_HELP"]
            ]

    def test_login(self):
        text_login = FunctionDoc(copernicusmarine.login)

        for i in range(len(text_login["Parameters"])):
            name_of_variable = text_login["Parameters"][i].name
            if len(text_login["Parameters"][i].desc) > 1:
                assert (
                    " ".join(text_login["Parameters"][i].desc)
                    == documentation_utils.LOGIN[
                        name_of_variable.upper() + "_HELP"
                    ]
                )
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert text_login["Parameters"][i].desc == [
                documentation_utils.LOGIN[name_of_variable.upper() + "_HELP"]
            ]

    def test_describe(self):
        text_describe = FunctionDoc(copernicusmarine.describe)

        for i in range(len(text_describe["Parameters"])):
            name_of_variable = text_describe["Parameters"][i].name
            assert text_describe["Parameters"][i].desc == [
                documentation_utils.DESCRIBE[
                    name_of_variable.upper() + "_HELP"
                ]
            ]

    def test_open_dataset(self):
        text_open_dataset = FunctionDoc(copernicusmarine.open_dataset)

        for i in range(len(text_open_dataset["Parameters"])):
            name_of_variable = text_open_dataset["Parameters"][i].name
            if name_of_variable in ["start_datetime", "end_datetime"]:
                assert text_open_dataset["Parameters"][i].desc == [
                    documentation_utils.SUBSET[
                        name_of_variable.upper() + "_HELP"
                    ]
                ]
                continue
            if name_of_variable == "variables":
                assert text_open_dataset["Parameters"][i].desc == [
                    "List of variable names to extract."
                ]
                continue
            if name_of_variable == "dataset_id":
                assert text_open_dataset["Parameters"][i].desc == [
                    "The datasetID, required."
                ]
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert text_open_dataset["Parameters"][i].desc == [
                documentation_utils.SUBSET[name_of_variable.upper() + "_HELP"]
            ]

    def test_read_dataframe(self):
        text_read_dataframe = FunctionDoc(copernicusmarine.read_dataframe)

        for i in range(len(text_read_dataframe["Parameters"])):
            name_of_variable = text_read_dataframe["Parameters"][i].name
            if name_of_variable in ["start_datetime", "end_datetime"]:
                assert text_read_dataframe["Parameters"][i].desc == [
                    documentation_utils.SUBSET[
                        name_of_variable.upper() + "_HELP"
                    ]
                ]
                continue
            if name_of_variable == "variables":
                assert text_read_dataframe["Parameters"][i].desc == [
                    "List of variable names to extract."
                ]
                continue
            if name_of_variable == "platform_ids":
                assert text_read_dataframe["Parameters"][i].desc == [
                    "List of platform IDs to extract. "
                    "Only available for platform chunked datasets."
                ]
                continue
            if name_of_variable == "dataset_id":
                assert text_read_dataframe["Parameters"][i].desc == [
                    "The datasetID, required."
                ]
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue

            assert text_read_dataframe["Parameters"][i].desc == [
                documentation_utils.SUBSET[name_of_variable.upper() + "_HELP"]
            ]
