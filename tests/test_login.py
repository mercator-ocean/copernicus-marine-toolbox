import os
from pathlib import Path

from copernicusmarine import login
from copernicusmarine.core_functions.credentials_utils import (
    ACCEPTED_HOSTS_NETRC_FILE,
    DEFAULT_CLIENT_CREDENTIALS_FILENAME,
    DEPRECATED_HOSTS,
)
from tests.test_utils import execute_in_terminal

# WARNING: To test locally delete your .copernicusmarine-credentials file
# and set the environment variables COPERNICUSMARINE_SERVICE_USERNAME
# and COPERNICUSMARINE_SERVICE_PASSWORD to your Copernicus Marine credentials.


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
        ]

        self.output = execute_in_terminal(command)

        assert self.output.returncode == 1
        assert "Invalid username or password" in self.output.stderr

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
            command, env=environment_without_crendentials, user_input=""
        )
        assert self.output.returncode == 1
        assert (
            "Downloading Copernicus Marine data requires a Copernicus Marine username "
            "and password, sign up for free at:"
            " https://data.marine.copernicus.eu/register"
        ) in self.output.stderr
        assert "Copernicus Marine username:" in self.output.stdout

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
            "-o",
            f"{tmp_path}",
            "--dry-run",
        ]
        password = os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD")
        assert password is not None
        self.output = execute_in_terminal(
            command,
            env=environment_without_crendentials,
            user_input=password,
        )
        assert self.output.returncode == 0, self.output.stderr

    def test_login_command_with_username_and_password(self, tmp_path):
        non_existing_directory = Path(tmp_path, "i_dont_exist")
        command = [
            "copernicusmarine",
            "login",
            "--force-overwrite",
            "--configuration-file-directory",
            f"{non_existing_directory}",
            "--username",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}",
            "--password",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}",
        ]

        self.output = execute_in_terminal(command, safe_quoting=True)
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
        self.output = execute_in_terminal(command, safe_quoting=True)
        assert self.output.returncode == 0
        assert (
            "Valid credentials from input username and password"
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
            "Invalid credentials from input username and password"
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
            "Invalid credentials from environment variables"
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
            "Valid credentials from environment variables"
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
        assert "No credentials found." in self.output.stderr

    def check_credentials_file_is_valid(self, tmp_path):
        non_existing_directory = Path(tmp_path, "lolololo")

        command = [
            "copernicusmarine",
            "login",
            "--configuration-file-directory",
            f"{non_existing_directory.as_posix()}",
            "--username",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}",
            "--password",
            f"{os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}",
        ]

        self.output = execute_in_terminal(command, safe_quoting=True)

        assert self.output.returncode == 0
        environment_without_crendentials = (
            get_environment_without_crendentials()
        )
        credentials_file_path = Path(
            non_existing_directory, DEFAULT_CLIENT_CREDENTIALS_FILENAME
        )

        command = [
            "copernicusmarine",
            "login",
            "--credentials-file",
            f"{credentials_file_path.as_posix()}",
            "--check-credentials-valid",
        ]
        assert credentials_file_path.exists()

        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 0
        assert (
            "Valid credentials from configuration file" in self.output.stderr
        )

    def test_login_falls_back_to_old_system(self):
        environment_without_crendentials = (
            get_environment_without_crendentials()
        )
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
            "--username",
            "toto",
            "--password",
            "tutu",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(
            command=command, env=environment_without_crendentials
        )
        assert self.output.returncode == 1
        assert (
            "Could not connect with new authentication system"
            in self.output.stderr
        )
        assert (
            " Trying with old authentication system..." in self.output.stderr
        )

    def test_login_python_interface(self, tmp_path):
        folder = Path(tmp_path, "lololo12")
        assert not login(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password="FAKEPASSWORD",
            configuration_file_directory=folder,
            force_overwrite=True,
        )

        assert folder.is_dir() is False
        assert login(
            username=os.getenv("COPERNICUSMARINE_SERVICE_USERNAME"),
            password=os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD"),
            configuration_file_directory=folder,
            force_overwrite=True,
        )

        assert (folder / ".copernicusmarine-credentials").is_file()
        assert login(
            check_credentials_valid=True,
        )
        assert not login(
            username="toto",
            password="tutu",
        )

    def test_login_with_netrc_file(self, tmp_path):
        for host in ACCEPTED_HOSTS_NETRC_FILE:
            self.create_netrc_file(tmp_path, host)
            self.check_validity_of_credentials_in_netrc_file(tmp_path)
            if host in DEPRECATED_HOSTS:
                assert (
                    "The following hosts are deprecated and will be removed"
                    " in future versions: ['nrt.cmems-du.eu', 'my.cmems-du.eu']"
                    in self.output.stderr
                )
            (tmp_path / ".netrc").unlink()

    def check_validity_of_credentials_in_netrc_file(self, tmp_path):
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
            "--credentials-file",
            f"{tmp_path}/.netrc",
        ]
        environment_without_crendentials = (
            get_environment_without_crendentials()
        )
        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 0
        assert (
            "Valid credentials from configuration file" in self.output.stderr
        )

    def create_netrc_file(self, tmp_path, host: str) -> None:
        netrc_file = Path(tmp_path, ".netrc")
        netrc_file.write_text(
            f"machine {host}\n"
            f"   login {os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}\n"
            f"   password {os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}\n"
        )

    def test_can_get_data_with_netrc_file(self, tmp_path):
        for host in ACCEPTED_HOSTS_NETRC_FILE:
            self.create_netrc_file(tmp_path, host)
            credentials_file = tmp_path / ".netrc"
            self.simple_get_command(credentials_file)
            assert "netrc" in self.output.stderr
            if host in DEPRECATED_HOSTS:
                assert (
                    "The following hosts are deprecated and will be removed"
                    " in future versions: ['nrt.cmems-du.eu', 'my.cmems-du.eu']"
                    in self.output.stderr
                )
            (credentials_file).unlink()

    def simple_get_command(self, credentials_file):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
            "--filter",
            "*glo12_rg_1d-m_20230831-20230831_2D_hcst_R20230913*",
            "--dry-run",
            "--log-level",
            "DEBUG",
            "--credentials-file",
            credentials_file,
        ]
        environment_without_crendentials = (
            get_environment_without_crendentials()
        )
        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 0

    def test_motuclient_file(self, tmp_path):
        self.create_motuclient_file(tmp_path)
        command = [
            "copernicusmarine",
            "login",
            "--check-credentials-valid",
            "--credentials-file",
            f"{tmp_path}/motuclient/motuclient-python.ini",
        ]
        environment_without_crendentials = (
            get_environment_without_crendentials()
        )
        self.output = execute_in_terminal(
            command, env=environment_without_crendentials
        )
        assert self.output.returncode == 0
        assert (
            "Valid credentials from configuration file" in self.output.stderr
        )
        assert (
            "The motuclient configuration file is deprecated"
            " and will be removed in future versions" in self.output.stderr
        )
        ((tmp_path / "motuclient") / "motuclient-python.ini").unlink()

    def create_motuclient_file(self, tmp_path):
        file_directory = tmp_path / "motuclient"
        file_directory.mkdir(exist_ok=True)
        motuclient_file = file_directory / "motuclient-python.ini"
        motuclient_file.write_text(
            f"[Main]\n\n"
            f"user = {os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')}\n"
            f"pwd = {os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')}\n"
        )

    def test_get_data_with_motuclient_file(self, tmp_path):
        self.create_motuclient_file(tmp_path)
        credentials_file = tmp_path / "motuclient/motuclient-python.ini"
        self.simple_get_command(credentials_file)
        assert "motuclient-python.ini" in self.output.stderr
        assert (
            "The motuclient configuration file is deprecated"
            " and will be removed in future versions" in self.output.stderr
        )
        (credentials_file).unlink()
