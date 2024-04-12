import os
import subprocess
from pathlib import Path


class TestBasicCommands:
    def test_describe(self):
        command = [
            "copernicusmarine",
            "describe",
            "--overwrite-metadata-cache",
        ]
        self.output = subprocess.run(command, capture_output=True)

    def test_subset(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-hcmr-wav-rean-h",
            "-x",
            "13.723",
            "-X",
            "13.724",
            "-y",
            "38.007",
            "-Y",
            "40.028",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-01T06:00:00",
            "-v",
            "VHM0",
            "--force-download",
        ]

        output = subprocess.run(command, capture_output=True)
        assert output.returncode == 0

    def test_get(self):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
            "--filter",
            "*/2023/08/*",
        ]
        output = subprocess.run(command, capture_output=True)

        assert output.returncode == 1
        assert not (b"No data to download") in output.stdout

    def test_login(self, tmp_path):
        assert os.getenv("COPERNICUSMARINE_SERVICE_USERNAME") is not None
        assert os.getenv("COPERNICUSMARINE_SERVICE_PASSWORD") is not None

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

        output = subprocess.run(command)
        assert output.returncode == 0
        assert non_existing_directory.is_dir()
