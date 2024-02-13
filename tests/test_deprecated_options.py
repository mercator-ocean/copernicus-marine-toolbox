from copernicusmarine import open_dataset
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
        output = execute_in_terminal(command)
        assert b"Downloading" in output.stdout

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
        output = execute_in_terminal(command)
        assert b"Downloading" in output.stdout

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
