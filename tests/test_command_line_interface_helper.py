from tests.test_utils import execute_in_terminal


class TestCommandLineInterfaceHelper:
    def test_main_helper(self):
        command = [
            "copernicusmarine",
            "--help",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_describe_helper(self):
        command = [
            "copernicusmarine",
            "describe",
            "--help",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_login_helper(self):
        command = [
            "copernicusmarine",
            "login",
            "--help",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_get_helper(self):
        command = [
            "copernicusmarine",
            "get",
            "--help",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

    def test_subset_helper(self):
        command = [
            "copernicusmarine",
            "subset",
            "--help",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
