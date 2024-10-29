from copernicusmarine import describe
from tests.test_utils import execute_in_terminal


class TestDeprecatedOptions:
    def test_describe_include_options_are_deprecated_cli(self):
        command = [
            "copernicusmarine",
            "describe",
            "--include-description",
            "--include-datasets",
            "--include-keywords",
            "--include-all",
            "--dataset-id",
            "cmems_mod_glo_phy_my_0.083deg_P1D-m",
        ]
        self.output = execute_in_terminal(command)

        assert (
            b"'--include-datasets' has been deprecated, use "
            b"'--returned-fields datasets' instead" in self.output.stderr
        )
        assert (
            b"'--include-keywords' has been deprecated, use "
            b"'--returned-fields keywords' instead" in self.output.stderr
        )
        assert (
            b"'--include-all' has been deprecated, use "
            b"'--returned-fields all' instead" in self.output.stderr
        )
        assert (
            b"'--include-description' has been deprecated, use "
            b"'--returned-fields description' instead" in self.output.stderr
        )
        assert self.output.returncode == 0

    def test_describe_include_options_are_deprecated_python_api(self, caplog):
        describe(
            dataset_id="cmems_mod_glo_phy_my_0.083deg_P1D-m",
            include_description=True,
            include_datasets=True,
            include_keywords=True,
            include_all=True,
        )

        assert "'include_datasets' has been deprecated" in caplog.text
        assert "'include_keywords' has been deprecated" in caplog.text
        assert "'include_all' has been deprecated" in caplog.text
        assert "'include_description' has been deprecated" in caplog.text
