import subprocess


class TestCommandLineInterfaceHelper:
    def test_main_helper(self):
        command = [
            "copernicusmarine",
            "--help",
        ]

        self.output = subprocess.run(command)
        assert self.output.returncode == 0

    def test_describe_helper(self):
        command = [
            "copernicusmarine",
            "describe",
            "--help",
        ]

        self.output = subprocess.run(command)
        assert self.output.returncode == 0

    def test_login_helper(self):
        command = [
            "copernicusmarine",
            "login",
            "--help",
        ]

        self.output = subprocess.run(command)
        assert self.output.returncode == 0

    def test_get_helper(self):
        command = [
            "copernicusmarine",
            "get",
            "--help",
        ]

        self.output = subprocess.run(command)
        assert self.output.returncode == 0

    def test_subset_helper(self):
        command = [
            "copernicusmarine",
            "subset",
            "--help",
        ]

        self.output = subprocess.run(command)
        assert self.output.returncode == 0
