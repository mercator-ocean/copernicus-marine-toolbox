import subprocess


class TestArcoServiceSelection:
    def test_with_no_geographical_nor_temporal_subset(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
        ]

        output = subprocess.run(command, capture_output=True)

        assert b"Downloading using service arco-geo-series..." in output.stdout

    def test_with_only_geographical_subset(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

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
        ]

        output = subprocess.run(command, capture_output=True)

        assert (
            b"Downloading using service arco-time-series..." in output.stdout
        )

    def test_with_only_temporal_subset(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--start-datetime",
            "2001-01-01 00:00:00",
            "--end-datetime",
            "2005-01-01 00:00:00",
        ]

        output = subprocess.run(command, capture_output=True)

        assert b"Downloading using service arco-geo-series..." in output.stdout

    def test_with_a_mix_of_geographical_and_temporal_subset_with_single_geo_point(
        self,
    ):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

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
            "-9.9",
            "--minimum-latitude",
            "34.2",
            "--maximum-latitude",
            "34.2",
            "--start-datetime",
            "2001-01-01 00:00:00",
            "--end-datetime",
            "2005-01-01 00:00:00",
        ]

        output = subprocess.run(command, capture_output=True)

        assert (
            b"Downloading using service arco-time-series..." in output.stdout
        )

    def test_with_a_mix_of_geographical_and_temporal_subset_with_single_temporal_point(
        self,
    ):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "thetao",
            "--minimum-longitude",
            "-59.9",
            "--maximum-longitude",
            "29.6",
            "--minimum-latitude",
            "33.96",
            "--maximum-latitude",
            "75.2",
            "--start-datetime",
            "2001-01-01 00:00:00",
            "--end-datetime",
            "2001-01-01 00:00:00",
        ]

        output = subprocess.run(command, capture_output=True)

        assert b"Downloading using service arco-geo-series..." in output.stdout

    def test_with_a_mix_of_geographical_and_temporal_subset(self):
        dataset_id = "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m"

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
            "--start-datetime",
            "2001-01-01 00:00:00",
            "--end-datetime",
            "2005-01-01 00:00:00",
        ]

        output = subprocess.run(command, capture_output=True)

        assert (
            b"Downloading using service arco-time-series..." in output.stdout
        )

    def test_dataset_has_interdependant_coordinates(self):
        dataset_id = (
            "cmems_obs-sl_eur_phy-ssh_nrt_allsat-l4-duacs-0.125deg_P1D"
        )

        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            f"{dataset_id}",
            "--variable",
            "adt",
            "--minimum-longitude",
            "3.0625",
            "--maximum-longitude",
            "8.9375",
            "--minimum-latitude",
            "38.0625",
            "--maximum-latitude",
            "40.9375",
            "--start-datetime",
            "2023-11-26T00:00:00",
            "--end-datetime",
            "2023-11-28T23:59:59",
        ]

        output = subprocess.run(command, capture_output=True)

        assert b"Downloading using service arco-geo-series..." in output.stdout
