import os
import shutil
from pathlib import Path

from copernicusmarine import login
from tests.test_utils import execute_in_terminal


def get_environment_without_crendentials():
    environment_without_crendentials = os.environ.copy()
    environment_without_crendentials.pop(
        "COPERNICUSMARINE_SERVICE_USERNAME", None
    )
    environment_without_crendentials.pop(
        "COPERNICUSMARINE_SERVICE_PASSWORD", None
    )
    return environment_without_crendentials


class TestLogin:
    def test_process_is_stopped_when_credentials_are_invalid(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

        command = [
            "copernicusmarine",
            "subset",
            "--username",
            "toto",
            "--password",
            "tutu",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--force-download",
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 1
        assert b"Invalid username or password" in self.output.stderr

    def test_login_is_prompt_when_configuration_file_doest_not_exist(
        self, tmp_path
    ):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"
        credentials_file = Path(tmp_path, "i_do_not_exist")

        environment_without_crendentials = (
            get_environment_without_crendentials()
        )

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-9.9",
            "--maximum-longitude",
            "-9.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "34.2",
            "--minimum-depth",
            "0.5",
            "--maximum-depth",
            "1.6",
            "--credentials-file",
            f"{credentials_file}",
        ]

        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 1
        assert (
            b"Downloading Copernicus Marine data requires a Copernicus Marine username "
            b"and password, sign up for free at:"
            b" https://data.marine.copernicus.eu/register"
        ) in self.output.stderr
        assert b"Copernicus Marine username:" in self.output.stdout

    def test_login_command_prompt_with_other_commands(self, tmp_path):
        assert os.getenv("COPERNICUSMARINE_SERVICE_USERNAME") is not None
        assert os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD") is not None

        environment_without_crendentials = (
            get_environment_without_crendentials()
        )

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--start-datetime",
            "2023-04-26 00:00:00",
            "--end-datetime",
            "2023-04-28 23:59:59",
            "--minimum-longitude",
            "-9.8",
            "--maximum-longitude",
            "-4.8",
            "--minimum-latitude",
            "33.9",
            "--maximum-latitude",
            "38.0",
            "--minimum-depth",
            "9.573",
            "--maximum-depth",
            "11.4",
            "--username",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}",
            "--force-download",
            "-o",
            f"{tmp_path}",
        ]
        password = os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD")
        assert password is not None
        self.output = execute_in_terminal(
            command,
            env=environment_without_crendentials,
            input=bytes(password, "utf-8"),
        )
        assert self.output.returncode == 0, self.output.stderr
        shutil.rmtree(Path(tmp_path))

    def test_login_command_with_username_and_password(self, tmp_path):
        non_existing_directory = Path(tmp_path, "i_dont_exist")
        command = [
            "copernicusmarine",
            "login",
            "--overwrite-configuration-file",
            "--configuration-file-directory",
            f"{non_existing_directory}",
            "--username",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}",
            "--password",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert non_existing_directory.is_dir()

    def test_login_check_credentials_are_valid(self, tmp_path):
        self.check_credentials_username_password_specified_are_invalid()
        self.check_credentials_username_specified_password_are_valid()
        self.check_credentials_username_password_env_var_are_wrong()
        self.check_credentials_username_password_env_var_are_valid()
        self.check_credentials_file_is_invalid()
        self.check_credentials_file_is_valid(tmp_path)

    def check_credentials_username_specified_password_are_valid(self):
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
            "--username",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}",
            "--password",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            b"Valid credentials from input username and password"
            in self.output.stderr
        )

    def check_credentials_username_password_specified_are_invalid(self):
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
            "--username",
            "toto",
            "--password",
            "tutu",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 1
        assert (
            b"Invalid credentials from input username and password"
            in self.output.stderr
        )

    def check_credentials_username_password_env_var_are_wrong(self):
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
        ]

        environment_without_crendentials = (
            get_environment_without_crendentials()
        )

        environment_without_crendentials[
            "COPERNICUSMARINE_SERVICE_USERNAME"
        ] = "toto"
        environment_without_crendentials[
            "COPERNICUSMARINE_SERVICE_PASSWORD"
        ] = "tutu"
        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 1
        assert (
            b"Invalid credentials from environment variables"
            in self.output.stderr
        )

    def check_credentials_username_password_env_var_are_valid(self):
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert (
            b"Valid credentials from environment variables"
            in self.output.stderr
        )

    def check_credentials_file_is_invalid(self):
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
            "--configuration-file-directory",
            "/toto",
        ]

        environment_without_crendentials = (
            get_environment_without_crendentials()
        )

        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 1
        assert (
            b"Invalid credentials from configuration file"
            in self.output.stderr
        )

    def check_credentials_file_is_valid(self, tmp_path):
        non_existing_directory = Path(tmp_path, "lolololo")

        command = [
            "copernicusmarine",
            "login",
            "--username",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}",
            "--password",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}",
            "--configuration-file-directory",
            f"{non_existing_directory}",
        ]

        self.output = execute_in_terminal(command)

        environment_without_crendentials = (
            get_environment_without_crendentials()
        )

        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
            "--configuration-file-directory",
            f"{non_existing_directory}",
        ]

        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 0
        assert (
            b"Valid credentials from configuration file" in self.output.stderr
        )

    def test_login_python_interface(self, tmp_path):
        folder = Path(tmp_path, "lololo12")
        assert not login(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password="FAKEPASSWORD",
            configuration_file_directory=folder,
            overwrite_configuration_file=True,
        )

        assert folder.is_dir() is False
        assert login(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            configuration_file_directory=folder,
            overwrite_configuration_file=True,
        )

        assert (folder / ".copernicusmarine-credentials").is_file()
        assert login(
            check_credentials_valid=True,
        )

        assert not login(
            username="toto",
            password="tutu",
        )
