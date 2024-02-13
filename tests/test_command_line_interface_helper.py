import subprocess


class TestCommandLineInterfaceHelper:
    def test_main_helper(self):
        command = [
            "copernicusmarine",
            "--help",
        ]

        output = subprocess.run(command)
        assert output.returncode == 0

    def test_describe_helper(self):
        command = [
            "copernicusmarine",
            "describe",
            "--help",
        ]

        output = subprocess.run(command)
        assert output.returncode == 0

    def test_login_helper(self):
        command = [
            "copernicusmarine",
            "login",
            "--help",
        ]

        output = subprocess.run(command)
        assert output.returncode == 0

    def test_get_helper(self):
        command = [
            "copernicusmarine",
            "get",
            "--help",
        ]

        output = subprocess.run(command)
        assert output.returncode == 0

    def test_subset_helper(self):
        command = [
            "copernicusmarine",
            "subset",
            "--help",
        ]

        output = subprocess.run(command)
        assert output.returncode == 0
