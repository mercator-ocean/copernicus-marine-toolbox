from copernicusmarine import get, subset
from tests.test_utils import execute_in_terminal


class TestDeprecatedOptions:
    def test_motu_api_request_deprecated_cli(self):
        command = [
            "copernicusmarine",
            "subset",
            "--motu-api-request",
            "some_request",
        ]
        self.output = execute_in_terminal(command)
        assert (
            b"'--motu-api-request' has been deprecated." in self.output.stderr
        )

    def test_motu_api_request_deprecated_api(self, caplog):
        try:
            subset(
                motu_api_request="some_request",
            )
        except Exception:
            pass
        assert "'motu_api_request' has been deprecated." in caplog.text

    def test_force_download_deprecated_subset(self):
        command = [
            "copernicusmarine",
            "subset",
            "--force-download",
            "-i",
            "cmems_mod_glo_phy-cur_anfc_0.083deg_P1M-m",
            "--dry-run",
            "-t",
            "2001",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert b"'--force-download' has been deprecated." in self.output.stderr

    def test_force_download_deprecated_get(self):
        command = [
            "copernicusmarine",
            "get",
            "--force-download",
            "-i",
            "cmems_mod_glo_phy-cur_anfc_0.083deg_P1M-m",
            "--dry-run",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert b"'--force-download' has been deprecated." in self.output.stderr

    def test_force_download_deprecated_subset_python_interface(self, caplog):
        subset(
            dataset_id="cmems_mod_glo_phy-cur_anfc_0.083deg_P1M-m",
            start_datetime="2001",
            dry_run=True,
            force_download=True,
        )
        assert "'force_download' has been deprecated." in caplog.text

    def test_force_download_deprecated_get_python_interface(self, caplog):
        get(
            dataset_id="cmems_mod_glo_phy-cur_anfc_0.083deg_P1M-m",
            dry_run=True,
            force_download=False,
        )

        assert "'force_download' has been deprecated." in caplog.text
