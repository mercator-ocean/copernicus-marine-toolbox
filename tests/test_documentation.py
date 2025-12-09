from numpydoc.docscrape import FunctionDoc

import copernicusmarine
from copernicusmarine.core_functions import documentation_utils

LIST_OF_EXCEPTIONS = ["username", "password", "**kwargs"]
PARTS_TO_IGNORE = ["Mutually exclusive"]


def clean_description_python_interface(desc: list[str]) -> list[str]:
    cleaned_desc = []
    for line in desc:
        if any(line.startswith(part) for part in PARTS_TO_IGNORE):
            continue
        cleaned_desc.append(line)
    return cleaned_desc


def clean_documenation_utils_cli(
    documentation: dict[str, str],
) -> dict[str, str]:
    def _cli_argument_to_python_argument(cli_description: str) -> str:
        cli_description_parsed = cli_description.split("``")
        if len(cli_description_parsed) <= 1:
            return cli_description
        if len(cli_description_parsed) % 2 == 0:
            raise ValueError(
                f"Cannot parse CLI argument from description: {cli_description}"
            )
        for i in range(1, len(cli_description_parsed), 2):
            cli_argument = cli_description_parsed[i]
            if "create-template" in cli_argument:
                continue
            if cli_argument.startswith("--"):
                python_argument = cli_argument[2:].replace("-", "_")
                cli_description_parsed[i] = python_argument

        return "``".join(cli_description_parsed)

    return {
        key.lower().replace("_help", ""): _cli_argument_to_python_argument(
            value
        )
        for key, value in documentation.items()
    }


get_documentation_cli_cleaned = clean_documenation_utils_cli(
    documentation_utils.GET
)
subset_documentation_cli_cleaned = clean_documenation_utils_cli(
    documentation_utils.SUBSET
)
login_documentation_cli_cleaned = clean_documenation_utils_cli(
    documentation_utils.LOGIN
)
describe_documentation_cli_cleaned = clean_documenation_utils_cli(
    documentation_utils.DESCRIBE
)
split_on_documentation_cli_cleaned = clean_documenation_utils_cli(
    documentation_utils.SUBSET_SPLIT_ON
)


class TestDocumentation:
    def test_subset(self):
        text_subset = FunctionDoc(copernicusmarine.subset)

        for parameter in text_subset["Parameters"]:
            name_of_variable = parameter.name
            parameter_desc = clean_description_python_interface(parameter.desc)
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
                subset_documentation_cli_cleaned[name_of_variable]
            ]

    def test_get(self):
        text_get = FunctionDoc(copernicusmarine.get)

        for parameter in text_get["Parameters"]:
            parameter_desc = clean_description_python_interface(parameter.desc)
            name_of_variable = parameter.name
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                get_documentation_cli_cleaned[name_of_variable]
            ]

    def test_login(self):
        text_login = FunctionDoc(copernicusmarine.login)

        for parameter in text_login["Parameters"]:
            parameter_desc = clean_description_python_interface(parameter.desc)
            name_of_variable = parameter.name
            if len(parameter_desc) > 1:
                assert (
                    " ".join(parameter_desc)
                    == login_documentation_cli_cleaned[name_of_variable]
                )
                continue
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                login_documentation_cli_cleaned[name_of_variable]
            ]

    def test_describe(self):
        text_describe = FunctionDoc(copernicusmarine.describe)

        for parameter in text_describe["Parameters"]:
            parameter_desc = clean_description_python_interface(parameter.desc)
            name_of_variable = parameter.name
            assert parameter_desc == [
                describe_documentation_cli_cleaned[name_of_variable]
            ]

    def test_open_dataset(self):
        text_open_dataset = FunctionDoc(copernicusmarine.open_dataset)

        for parameter in text_open_dataset["Parameters"]:
            parameter_desc = clean_description_python_interface(parameter.desc)
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
                subset_documentation_cli_cleaned[name_of_variable]
            ]

    def test_read_dataframe(self):
        text_read_dataframe = FunctionDoc(copernicusmarine.read_dataframe)

        for parameter in text_read_dataframe["Parameters"]:
            parameter_desc = clean_description_python_interface(parameter.desc)
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
                subset_documentation_cli_cleaned[name_of_variable]
            ]

    def test_subset_split_on(self):
        text_subset_split_on = FunctionDoc(copernicusmarine.subset_split_on)

        for parameter in text_subset_split_on["Parameters"]:
            parameter_desc = clean_description_python_interface(parameter.desc)
            name_of_variable = parameter.name
            if name_of_variable in LIST_OF_EXCEPTIONS:
                continue
            assert parameter_desc == [
                split_on_documentation_cli_cleaned[name_of_variable]
            ]
