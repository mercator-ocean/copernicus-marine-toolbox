import re

from tests.test_utils import execute_in_terminal


class TestCommandLineSplitOn:
    def test_split_on(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-x",
            "-9.0",
            "-X",
            "-8.0",
            "-y",
            "34.0",
            "-Y",
            "35.0",
            "-z",
            "0.5",
            "-Z",
            "2",
            "--split-on",
            "year",
            "--dry-run",
            "-t",
            "2020-01-01",
            "-T",
            "2023-05-01",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        substring = "file_path"
        num_responses = re.finditer(substring, self.output.stdout)
        assert len(list(num_responses)) == 4

    def test_split_on_invalid_value(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-x",
            "-9.0",
            "-X",
            "-8.0",
            "-y",
            "34.0",
            "-Y",
            "35.0",
            "-z",
            "0.5",
            "-Z",
            "2",
            "--split-on",
            "invalid_value",
            "--dry-run",
            "-t",
            "2020-01-01",
            "-T",
            "2023-05-01",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode != 0
        assert "Error: Invalid value for '--split-on'" in self.output.stderr

    def test_split_on_invalid_file_format(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-x",
            "-9.0",
            "-X",
            "-8.0",
            "-y",
            "34.0",
            "-Y",
            "35.0",
            "-z",
            "0.5",
            "-Z",
            "2",
            "--split-on",
            "year",
            "--file-format",
            "zarr",
            "--dry-run",
            "-t",
            "2020-01-01",
            "-T",
            "2023-05-01",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode != 0
        assert (
            "The split on files option is not available for the requested format 'zarr'"
            in self.output.stderr
        )
