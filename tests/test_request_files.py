import fnmatch
import json
import re
from pathlib import Path

from tests.test_command_line_interface import get_all_files_in_folder_tree
from tests.test_utils import execute_in_terminal


def sort_netcdf_string(netcdf_str):
    header_match = re.match(r"netcdf\s+([^{]+)\s*\{", netcdf_str)
    header = f"netcdf {header_match.group(1)} {{\n" if header_match else ""

    dimensions_block = re.search(
        r"dimensions:\s*((?:.|\n)*?)variables:", netcdf_str
    )
    variables_block = re.search(
        r"variables:\s*((?:.|\n)*?)// global attributes:", netcdf_str
    )
    global_attrs_block = re.search(
        r"// global attributes:\s*((?:.|\n)*?)\}", netcdf_str
    )

    def sort_lines_block(block_text):
        lines = [
            line.strip()
            for line in block_text.strip().split("\n")
            if line.strip()
        ]
        return sorted(lines)

    def parse_variable_blocks(text):
        var_blocks = {}
        lines = text.strip().split("\n")
        current_var = None
        current_block = []
        for line in lines:
            if re.match(r"^\s*\w+ \w+\([^\)]*\) ;", line.strip()):
                if current_var:
                    var_blocks[current_var] = current_block
                current_var = line.strip().split()[1].split("(")[0]
                current_block = [line.strip()]
            else:
                current_block.append(line.strip())
        if current_var:
            var_blocks[current_var] = current_block
        return dict(sorted(var_blocks.items()))

    def sort_variable_attrs(block_lines):
        base = block_lines[0]
        attrs = sorted(block_lines[1:])
        return [base] + attrs

    sorted_dimensions = (
        sort_lines_block(dimensions_block.group(1)) if dimensions_block else []
    )
    dimensions_str = "  dimensions:\n" + "".join(
        f"   {line}\n" for line in sorted_dimensions
    )

    variable_blocks = (
        parse_variable_blocks(variables_block.group(1))
        if variables_block
        else {}
    )
    sorted_vars = []
    for block in variable_blocks.values():
        sorted_attrs = sort_variable_attrs(block)
        sorted_vars.extend(f"   {line}" for line in sorted_attrs)
        sorted_vars.append("")  # blank line between variables
    variables_str = "  variables:\n" + "\n".join(sorted_vars).rstrip() + "\n"

    global_attrs = (
        sort_lines_block(global_attrs_block.group(1))
        if global_attrs_block
        else []
    )
    global_attrs_str = "  // global attributes:\n" + "".join(
        f"   {line}\n" for line in global_attrs
    )

    return header + dimensions_str + variables_str + global_attrs_str + "}"


def get_path_to_request_file(filename: str):
    return Path("tests/resources/request_files", filename + ".json")


def build_command(filepath: Path, command: str):
    return [
        "copernicusmarine",
        f"{command}",
        "--request-file",
        f"{filepath}",
        "--overwrite",
    ]


class TestRequestFiles:
    def test_subset_request_with_request_file(self, tmp_path, snapshot):
        filepath = get_path_to_request_file(
            "test_subset_request_with_request_file"
        )

        command = build_command(filepath, "subset")
        command += [
            "--output-directory",
            f"{tmp_path}",
            "-r",
            "all",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        response = json.loads(self.output.stdout)
        assert b'Selected dataset version: "default"' in self.output.stderr
        self.dumped_output = execute_in_terminal(
            ["ncdump", "-h", response["file_path"]]
        )
        assert (
            sort_netcdf_string(self.dumped_output.stdout.decode("utf-8"))
            == snapshot
        )

    def test_subset_request_without_subset(self):
        filepath = get_path_to_request_file(
            "test_subset_request_without_subset"
        )

        command = build_command(filepath, "subset")

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Missing subset option. Try 'copernicusmarine subset --help'."
            in self.output.stderr
        )
        assert (
            b"To retrieve a complete dataset, please use instead: "
            b"copernicusmarine get --dataset-id "
            b"METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2"
        ) in self.output.stderr

    def test_subset_request_with_dataset_not_in_catalog(self):
        filepath = get_path_to_request_file(
            "test_subset_request_with_dataset_not_in_catalog"
        )

        command = build_command(filepath, "subset")

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1

    def test_subset_error_when_forced_service_does_not_exist(self):
        filepath = get_path_to_request_file(
            "test_subset_error_when_forced_service_does_not_exist"
        )

        command = build_command(filepath, "subset")

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Service not available: Available services for dataset: []"
        ) in self.output.stderr

    def test_get_download_s3_with_wildcard_filter_and_regex(self, tmp_path):
        filepath = get_path_to_request_file(
            "test_get_download_s3_with_wildcard_filter_and_regex"
        )

        command = build_command(filepath, "get")
        command += [
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        downloaded_files = get_all_files_in_folder_tree(folder=tmp_path)
        assert self.output.returncode == 0
        assert len(downloaded_files) == 5

        for filename in downloaded_files:
            assert (
                fnmatch.fnmatch(filename, "*_200[45]*.nc")
                or re.match(".*_(2001|2002|2003).*.nc", filename) is not None
            )

    def test_get_download_no_files(self, tmp_path):
        filepath = get_path_to_request_file("test_get_download_no_files")

        command = build_command(filepath, "get")
        command += [
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        assert b"No data to download" in self.output.stderr
        assert self.output.returncode == 0

    def test_get_request_with_request_file(self, tmp_path):
        filepath = get_path_to_request_file(
            "test_get_request_with_request_file"
        )

        command = build_command(filepath, "get")
        command += [
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_get_request_with_one_wrong_attribute(self, tmp_path):
        filepath = get_path_to_request_file(
            "test_get_request_with_one_wrong_attribute"
        )

        command = build_command(filepath, "get")
        command += [
            "--output-directory",
            f"{tmp_path}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
