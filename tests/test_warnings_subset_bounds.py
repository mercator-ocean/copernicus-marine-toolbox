from datetime import datetime, timedelta

from copernicusmarine import core_functions, subset
from tests.test_utils import execute_in_terminal


class TestWarningsSubsetBounds:
    def _build_custom_command(
        self,
        dataset_id,
        variable,
        min_longitude,
        max_longitude,
        coordinates_selection_method="nearest",
    ):
        return [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            f"{variable}",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
            "--coordinates-selection-method",
            f"{coordinates_selection_method}",
        ]

    def test_subset_send_warning_with_method_nearest(self):
        dataset_id = (
            "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D"
        )
        command = self._build_custom_command(
            dataset_id, "CHL", -180, 180, "nearest"
        )
        self.output = execute_in_terminal(command, input=b"n")

        assert b"WARNING" in self.output.stderr
        assert (
            b"Some of your subset selection [-180.0, 180.0]"
            b" for the longitude dimension exceed the dataset"
            b" coordinates [-179.9791717529297, 179.9791717529297]"
        ) in self.output.stderr

    def test_subset_warnings_differently(self):
        # Dataset with longitude bounds from -180 to 179.91668701171875
        # The first call should return a warning, the second one should not
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"

        command = self._build_custom_command(
            dataset_id, "thetao", -179.9, 179.9, "nearest"
        )
        self.output = execute_in_terminal(command, input=b"n")

        assert (
            b"Some or all of your subset selection [-179.9, 179.9] for the longitude "
            b"dimension  exceed the dataset coordinates [-180.0, 179.91668701171875]"
        ) not in self.output.stderr

    def test_subset_warnings_when_surpassing(self):
        # Dataset with longitude bounds from [-179.9791717529297, 179.9791717529297]
        # Both calls should return the same warning
        dataset_id = (
            "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D"
        )

        command1 = self._build_custom_command(
            dataset_id, "CHL", -180, 180, "nearest"
        )
        command2 = self._build_custom_command(
            dataset_id, "CHL", -179.99, 179.99, "nearest"
        )
        self.output1 = execute_in_terminal(command1, input=b"n")
        self.output2 = execute_in_terminal(command2, input=b"n")

        assert (
            b"Some of your subset selection [-180.0, 180.0] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in self.output1.stderr
        assert (
            b"Some of your subset selection [-179.99, 179.99] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in self.output2.stderr

    def test_subset_strict_error(self):
        dataset_id = (
            "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D"
        )

        command1 = self._build_custom_command(
            dataset_id, "CHL", -180, 180, "strict"
        )
        command2 = self._build_custom_command(
            dataset_id, "CHL", -179.9, 179.9, "strict"
        )
        self.output1 = execute_in_terminal(command1, input=b"n")
        self.output2 = execute_in_terminal(command2, input=b"n")
        assert (
            b"""one was selected: "arco-geo-series"\nERROR"""
        ) in self.output1.stderr
        assert (
            b"Some of your subset selection [-180.0, 180.0] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in self.output1.stderr
        assert (
            b"""one was selected: "arco-geo-series"\nERROR"""
        ) not in self.output2.stderr
        assert (
            b"Some of your subset selection [-179.9, 179.9] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) not in self.output2.stderr

    def test_subset_handle_180_point_correctly(self):
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"

        command = self._build_custom_command(
            dataset_id, "thetao", -150, 180, "strict"
        )
        self.output = execute_in_terminal(command, input=b"n")
        assert (
            b"""one was selected: "arco-geo-series"\nERROR"""
        ) not in self.output.stderr
        assert (
            b"Some or all of your subset selection"
        ) not in self.output.stderr

    def test_warn_depth_out_of_dataset_bounds(self, tmp_path):
        output_filename = "output.nc"
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
        min_longitude = 29.0
        max_longitude = 30.0
        min_latitude = 30
        max_latitude = 32
        min_depth = 0.4
        max_depth = 50.0
        start_datetime = "2023-11-03"
        end_datetime = "2023-11-03"
        command = self._build_custom_command(
            dataset_id, "thetao", min_longitude, max_longitude, "nearest"
        )
        command.extend(
            [
                "--minimum-latitude",
                f"{min_latitude}",
                "--maximum-latitude",
                f"{max_latitude}",
                "--start-datetime",
                f"{start_datetime}",
                "--end-datetime",
                f"{end_datetime}",
                "--minimum-depth",
                f"{min_depth}",
                "--maximum-depth",
                f"{max_depth}",
                "-o",
                f"{tmp_path}",
                "-f",
                f"{output_filename}",
            ]
        )
        output = execute_in_terminal(command, input=b"n")

        assert (
            b"Some of your subset selection [0.4, 50.0] for the depth "
            b"dimension exceed the dataset coordinates "
            b"[0.49402499198913574, 5727.9169921875]"
        ) in output.stderr

    def test_warn_elevation_out_of_dataset_bounds(self, tmp_path):
        output_filename = "output.nc"
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"
        min_longitude = 29.0
        max_longitude = 30.0
        min_latitude = 30
        max_latitude = 32
        min_depth = 0.4
        max_depth = 50.0
        start_datetime = "2023-11-03"
        end_datetime = "2023-11-03"
        command = self._build_custom_command(
            dataset_id, "thetao", min_longitude, max_longitude, "nearest"
        )
        command.extend(
            [
                "--minimum-latitude",
                f"{min_latitude}",
                "--maximum-latitude",
                f"{max_latitude}",
                "--start-datetime",
                f"{start_datetime}",
                "--end-datetime",
                f"{end_datetime}",
                "--minimum-depth",
                f"{min_depth}",
                "--maximum-depth",
                f"{max_depth}",
                "--vertical-dimension-output",
                "elevation",
                "-o",
                f"{tmp_path}",
                "-f",
                f"{output_filename}",
            ]
        )
        output = execute_in_terminal(command, input=b"n")

        assert (
            b"Some of your subset selection [0.4, 50.0] for the depth "
            b"dimension exceed the dataset coordinates "
            b"[0.49402499198913574, 5727.9169921875]"
        ) in output.stderr
        assert (
            b"* elevation  (elevation) float32 72B -47.37 -40.34"
        ) in output.stderr

    def test_error_Coord_out_of_dataset_bounds(self):
        try:
            _ = subset(
                dataset_id="cmems_mod_glo_phy_anfc_0.083deg_P1D-m",
                start_datetime=datetime.today() + timedelta(10),
                force_download=True,
                end_datetime=datetime.today()
                + timedelta(days=10, hours=23, minutes=59),
            )
        except core_functions.exceptions.CoordinatesOutOfDatasetBounds as e:
            assert "Some of your subset selection" in e.__str__()

    def when_I_request_a_dataset_with_coordinates_selection_method_option(
        self, coordinates_selection_method
    ):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "med-hcmr-wav-rean-h",
            "-x",
            "-19",
            "-X",
            "-17",
            "-y",
            "38.007",
            "-Y",
            "38.028",
            "-t",
            "1993-01-01T00:00:00",
            "-T",
            "1993-01-01T06:00:00",
            "-v",
            "VHM0",
            "--force-download",
            "--coordaintes-selection-method",
            f"{coordinates_selection_method}",
        ]

        self.output = execute_in_terminal(command)

    def then_I_can_read_an_error_in_stdout(self):
        assert self.output.returncode == 1
        assert b"ERROR" in self.output.stderr
        assert (
            b"Some of your subset selection [-19.0, -17.0] for "
            b"the longitude dimension exceed the dataset coordinates"
        ) in self.output.stderr

    def then_I_can_read_a_warning_in_stdout(self):
        assert self.output.returncode == 0
        assert b"WARNING" in self.output.stderr
        assert (
            b"Some of your subset selection [-19.0, -17.0] for "
            b"the longitude dimension exceed the dataset coordinates"
        ) in self.output.stderr

    def test_subset_strict_method(self):
        self.when_I_request_a_dataset_with_coordinates_selection_method_option(
            "strict"
        )
        self.then_I_can_read_an_error_in_stdout()

    def test_subset_nearest_method(self):
        self.when_I_request_a_dataset_with_coordinates_selection_method_option(
            "nearest"
        )
        self.then_I_can_read_a_warning_in_stdout()
