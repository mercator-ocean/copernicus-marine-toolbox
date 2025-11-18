from tests.test_utils import execute_in_terminal


class TestHelpCommandLineInterface:
    def test_help_from_describe_is_as_expected(self, snapshot):
        self.output_long = execute_in_terminal(
            ["copernicusmarine", "describe", "--help"]
        )
        self.output_short = execute_in_terminal(
            ["copernicusmarine", "describe", "-h"]
        )
        assert self.output_long.returncode == 0
        assert self.output_short.returncode == 0
        stdout_long = str(self.output_long.stdout).split("\n")
        stdout_short = str(self.output_short.stdout).split("\n")
        assert stdout_long == snapshot
        assert stdout_short == stdout_long

    def test_help_from_get_is_as_expected(self, snapshot):
        self.output_long = execute_in_terminal(
            ["copernicusmarine", "get", "--help"]
        )
        self.output_short = execute_in_terminal(
            ["copernicusmarine", "get", "-h"]
        )
        assert self.output_long.returncode == 0
        assert self.output_short.returncode == 0
        stdout_long = str(self.output_long.stdout).split("\n")
        stdout_short = str(self.output_short.stdout).split("\n")
        assert stdout_long == snapshot
        assert stdout_short == stdout_long

    def test_help_from_subset_is_as_expected(self, snapshot):
        self.output_long = execute_in_terminal(
            ["copernicusmarine", "subset", "--help"]
        )
        self.output_short = execute_in_terminal(
            ["copernicusmarine", "subset", "-h"]
        )
        assert self.output_long.returncode == 0
        assert self.output_short.returncode == 0
        stdout_long = str(self.output_long.stdout).split("\n")
        stdout_short = str(self.output_short.stdout).split("\n")
        assert stdout_long == snapshot
        assert stdout_short == stdout_long

    def test_help_from_login_is_as_expected(self, snapshot):
        self.output_long = execute_in_terminal(
            ["copernicusmarine", "login", "--help"]
        )
        self.output_short = execute_in_terminal(
            ["copernicusmarine", "login", "-h"]
        )
        assert self.output_long.returncode == 0
        assert self.output_short.returncode == 0
        stdout_long = str(self.output_long.stdout).split("\n")
        stdout_short = str(self.output_short.stdout).split("\n")
        assert stdout_long == snapshot
        assert stdout_short == stdout_long

    def test_help_from_copernicusmarin_is_as_expected(self, snapshot):
        self.output_long = execute_in_terminal(["copernicusmarine", "--help"])
        self.output_short = execute_in_terminal(["copernicusmarine", "-h"])
        assert self.output_long.returncode == 0
        assert self.output_short.returncode == 0
        stdout_long = str(self.output_long.stdout).split("\n")
        stdout_short = str(self.output_short.stdout).split("\n")
        assert stdout_long == snapshot
        assert stdout_short == stdout_long

    def test_help_from_subset_split_on_is_as_expected(self, snapshot):
        self.output_long = execute_in_terminal(
            ["copernicusmarine", "subset", "split-on", "--help"]
        )
        self.output_short = execute_in_terminal(
            ["copernicusmarine", "subset", "split-on", "-h"]
        )
        assert self.output_long.returncode == 0
        assert self.output_short.returncode == 0
        stdout_long = str(self.output_long.stdout).split("\n")
        stdout_short = str(self.output_short.stdout).split("\n")
        assert stdout_long == snapshot
        assert stdout_short == stdout_long
