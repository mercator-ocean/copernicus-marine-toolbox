import os
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

    def test_split_on_invalid_file_format_sqlite(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_obs-ins_arc_phybgcwav_mynrt_na_irr",
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
            "2025-09-01",
            "-T",
            "2025-09-15",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode != 0
        assert (
            "The split on files option is not available for the requested format 'csv'"
            in self.output.stderr
        )

    def test_split_on_too_many_processes(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
            "-x",
            "-50.0",
            "-X",
            "-50.0",
            "-y",
            "-50.0",
            "-Y",
            "50.0",
            "-z",
            "0.6",
            "-Z",
            "5000",
            "--split-on",
            "year",
            "--dry-run",
            "-t",
            "2020-01-01",
            "-T",
            "2023-05-01",
        ]

        env = {
            **os.environ,
            "COPERNICUSMARINE_SPLIT_MAXIMUM_PROCESSES": "60",
        }
        self.output = execute_in_terminal(command, env=env)
        assert self.output.returncode == 0
        assert (
            "The estimated memory required exceeds the available memory, "
            "lowering the number of parallel processes to"
            in self.output.stderr
        )
