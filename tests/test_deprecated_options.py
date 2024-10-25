from copernicusmarine import describe, open_dataset
from tests.test_utils import execute_in_terminal


class TestDeprecatedOptions:
    def test_get_command_line_works_with_deprecated_options(
        self,
    ):
        command = [
            "copernicusmarine",
            "get",
            "--dataset-id",
            "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            "--force-service",
            "files",
            "--force-dataset-version",
            "202311",
            "--force-dataset-part",
            "latest",
        ]
        self.output = execute_in_terminal(command)
        assert b"Downloading" in self.output.stderr

    def test_subset_command_line_works_with_deprecated_options(
        self,
    ):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            "--variable",
            "thetao",
            "--minimal-longitude",
            "-9.9",
            "--maximal-longitude",
            "-9.6",
            "--minimal-latitude",
            "33.96",
            "--maximal-latitude",
            "34.2",
            "--force-service",
            "arco-time-series",
            "--force-dataset-version",
            "202211",
            "--force-dataset-part",
            "default",
        ]
        self.output = execute_in_terminal(command)
        assert b"Downloading" in self.output.stderr

    def test_get_python_works_and_shows_preferred_options_over_deprecated(
        self,
    ):
        dataset = open_dataset(
            dataset_id="cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
            variables=["thetao"],
            minimal_longitude=-9.9,
            maximal_longitude=-9.6,
            minimal_latitude=33.96,
            maximal_latitude=34.2,
            force_service="arco-time-series",
            force_dataset_version="202211",
            force_dataset_part="default",
        )
        assert dataset

    def test_describe_include_all_dataset_versions_deprecated(self):
        command = [
            "copernicusmarine",
            "describe",
            "--contains",
            "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2",
            "--include-all-versions",
        ]
        self.output = execute_in_terminal(command)
        assert b"WARNING" in self.output.stderr
        assert (
            b"'--include-all-versions' has been deprecated. "
            b"Use '--include-versions' instead"
        ) in self.output.stderr
        assert self.output.returncode == 0

    def test_describe_include_all_dataset_versions_python_interface(self):
        describe_result = describe(
            contains=["lkshdflkhsdlfksdflhh"],
            include_all_versions=True,
        )
        assert describe_result == {}

    def test_should_fail(self):
        raise Exception("This test should fail")
