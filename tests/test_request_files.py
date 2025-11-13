import fnmatch
import re
from pathlib import Path

from tests.test_command_line_interface import get_all_files_in_folder_tree
from tests.test_utils import execute_in_terminal


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
    def test_subset_request_with_request_file(self, tmp_path):
        # TODO: add a snapshot of the result: ncdump or str(dataset)
        # problem is that it is difficult to have sorting
        # and stable snapshots
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
        assert 'Selected dataset version: "default"' in self.output.stderr

    def test_subset_request_without_subset(self):
        filepath = get_path_to_request_file(
            "test_subset_request_without_subset"
        )

        command = build_command(filepath, "subset")

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            "Missing subset option. Try 'copernicusmarine subset --help'."
            in self.output.stderr
        )
        assert (
            "To retrieve a complete dataset, please use instead: "
            "copernicusmarine get --dataset-id "
            "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2"
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
            "Service not available: Available services for dataset: ['geoseries', "
            "'timeseries']"
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
        assert "No data to download" in self.output.stderr
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

    def test_subset_request_wrong_typings(self):
        filepath = get_path_to_request_file("test_subset_wrong_typings")

        command = build_command(filepath, "subset") + ["--dry-run"]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert "Invalid request in file" in self.output.stderr

    def test_subset_works_deprecated_options(self):
        filepath = get_path_to_request_file(
            "test_subset_works_deprecated_options"
        )

        command = build_command(filepath, "subset") + ["--dry-run"]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert "'force_download' has been deprecated" in self.output.stderr
        assert "'motu_api_request' has been deprecated" in self.output.stderr
