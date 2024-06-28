from tests.test_utils import execute_in_terminal


class TestWarningsSubsetBounds:
    def _build_custom_command(
        self,
        dataset_id,
        variable,
        min_longitude,
        max_longitude,
        subset_method="nearest",
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
            "--subset-method",
            f"{subset_method}",
        ]

    def test_subset_warning_properly(self):
        # Dataset with longitude bounds from -179.97... to 179.91...
        # The call should return a warning (and correctly the bounds)
        dataset_id = (
            "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D"
        )
        command = self._build_custom_command(
            dataset_id, "CHL", -180, 180, "nearest"
        )
        output = execute_in_terminal(command, input=b"n")

        assert b"WARNING" in output.stdout
        assert (
            b"Some of your subset selection [-180.0, 180.0]"
            b" for the longitude dimension exceed the dataset"
            b" coordinates [-179.9791717529297, 179.9791717529297]"
        ) in output.stdout

    def test_subset_warnings_differently(self):
        # Dataset with longitude bounds from -180 to 179.91668701171875
        # The first call should return a warning, the second one should not
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"

        command1 = self._build_custom_command(
            dataset_id, "thetao", -180, 180, "nearest"
        )
        command2 = self._build_custom_command(
            dataset_id, "thetao", -179.9, 179.9, "nearest"
        )
        output1 = execute_in_terminal(command1, input=b"n")
        output2 = execute_in_terminal(command2, input=b"n")

        assert (
            b"Some of your subset selection [-180.0, 180.0] for the longitude "
            b"dimension exceed the dataset coordinates [-180.0, 179.91668701171875]"
        ) in output1.stdout
        assert (
            b"Some of your subset selection [-179.9, 179.9] for the longitude "
            b"dimension exceed the dataset coordinates [-180.0, 179.91668701171875]"
        ) not in output2.stdout  # Here they don't have to appear

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
        output1 = execute_in_terminal(command1, input=b"n")
        output2 = execute_in_terminal(command2, input=b"n")

        assert (
            b"Some of your subset selection [-180.0, 180.0] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in output1.stdout
        assert (
            b"Some of your subset selection [-179.99, 179.99] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in output2.stdout

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
        output1 = execute_in_terminal(command1, input=b"n")
        output2 = execute_in_terminal(command2, input=b"n")
        assert (
            b"""one was selected: "arco-geo-series"\nERROR"""
        ) in output1.stdout
        assert (
            b"Some of your subset selection [-180.0, 180.0] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in output1.stdout
        assert (
            b"""one was selected: "arco-geo-series"\nERROR"""
        ) not in output2.stdout
        assert (
            b"Some of your subset selection [-179.9, 179.9] for the longitude "
            b"dimension exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) not in output2.stdout

    def test_warn_depth_out_of_dataset_bounds(self, tmp_path):
        output_filename = "output.nc"
        min_longitude = 29.0
        max_longitude = 30.0
        min_latitude = 30
        max_latitude = 32
        min_depth = 0.4
        max_depth = 50.0
        start_datetime = "2021-11-03"
        end_datetime = "2021-11-03"
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
            "--variable",
            "thetao",
            "--minimum-longitude",
            f"{min_longitude}",
            "--maximum-longitude",
            f"{max_longitude}",
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
            "--force-download",
        ]
        output = subprocess.run(command, capture_output=True)

        assert (
            b"Some of your subset selection [0.4, 50.0] for the depth "
            b"dimension exceed the dataset coordinates "
            b"[0.49402499198913574, 5727.9169921875]"
        ) in output.stdout
