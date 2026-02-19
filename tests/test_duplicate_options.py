from tests.test_utils import execute_in_terminal


class TestDuplicateOptions:
    def test_duplicate_option_raises_error(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m",
            "--minimum-longitude",
            "0",
            "--minimum-longitude",
            "1",
            "--dry-run",
        ]

        output = execute_in_terminal(command)
        assert output.returncode != 0
        assert "provided multiple times" in output.stderr
