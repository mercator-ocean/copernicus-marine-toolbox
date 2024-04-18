import subprocess


class TestWarningsSubsetBounds:
    def _build_custom_command(
        self, dataset_id, variable, min_longitude, max_longitude
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
        ]

    def test_subset_warning_properly(self):
        # Dataset with longitude bounds from -179.97... to 179.91...
        # The call should return a warning (and correctly the bounds)
        dataset_id = (
            "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D"
        )
        command = self._build_custom_command(dataset_id, "CHL", -180, 180)

        output = subprocess.run(command, capture_output=True)

        assert b"WARNING" in output.stdout
        assert (
            b"ome or all of your subset selection [-180.0, 180.0]"
            b" for the longitude dimension  exceed the dataset"
            b" coordinates [-179.9791717529297, 179.9791717529297]"
        ) in output.stdout

    def test_subset_warnings_differently(self):
        # Dataset with longitude bounds from -180 to 179.91668701171875
        # The first call should return a warning, the second one should not
        dataset_id = "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m"

        command1 = self._build_custom_command(dataset_id, "thetao", -180, 180)
        command2 = self._build_custom_command(dataset_id, "thetao", -179, 179)

        output1 = subprocess.run(command1, capture_output=True)
        output2 = subprocess.run(command2, capture_output=True)

        assert (
            b"ome or all of your subset selection [-180.0, 180.0] for the longitude "
            b"dimension  exceed the dataset coordinates [-180.0, 179.91668701171875]"
        ) in output1.stdout
        assert (
            b"ome or all of your subset selection [-179.9, 179.9] for the longitude "
            b"dimension  exceed the dataset coordinates [-180.0, 179.91668701171875]"
        ) not in output2.stdout  # Here they don't have to appear

    def test_subset_warnings_when_surpassing(self):
        # Dataset with longitude bounds from [-179.9791717529297, 179.9791717529297]
        # Both calls should return the same warning
        dataset_id = (
            "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D"
        )

        command1 = self._build_custom_command(dataset_id, "CHL", -180, 180)
        command2 = self._build_custom_command(
            dataset_id, "CHL", -179.99, 179.99
        )

        output1 = subprocess.run(command1, capture_output=True)
        output2 = subprocess.run(command2, capture_output=True)

        assert (
            b"ome or all of your subset selection [-180.0, 180.0] for the longitude "
            b"dimension  exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in output1.stdout
        assert (
            b"ome or all of your subset selection [-179.99, 179.99] for the longitude "
            b"dimension  exceed the dataset coordinates "
            b"[-179.9791717529297, 179.9791717529297]"
        ) in output2.stdout
