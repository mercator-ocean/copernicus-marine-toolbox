from tests.test_utils import execute_in_terminal


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
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        assert b'Selected service: "arco-geo-series"' in self.output.stderr

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
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        assert b'Selected service: "arco-time-series"' in self.output.stderr

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
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        assert b'Selected service: "arco-geo-series"' in self.output.stderr

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
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        assert b'Selected service: "arco-time-series"' in self.output.stderr

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
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        assert b'Selected service: "arco-geo-series"' in self.output.stderr

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
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        assert b'Selected service: "arco-time-series"' in self.output.stderr

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
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0

        assert b'Selected service: "arco-geo-series"' in self.output.stderr

    def test_time_series_service_originalGrid(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--dataset-part",
            "originalGrid",
            "--maximum-x",
            "1",
            "--minimum-x",
            "-1",
            "--maximum-y",
            "1",
            "--minimum-y",
            "-1",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert b'Selected service: "arco-time-series"' in self.output.stderr

    def test_geo_series_service_originalGrid(self):
        command = [
            "copernicusmarine",
            "subset",
            "-i",
            "cmems_mod_arc_bgc_my_ecosmo_P1D-m",
            "--dataset-part",
            "originalGrid",
            "-t",
            "2020",
            "-T",
            "2020",
            "--dry-run",
            "--log-level",
            "DEBUG",
        ]
        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert b'Selected service: "arco-geo-series"' in self.output.stderr

    def test_should_be_geo_series_service(self):
        """
        Legacy code for the service selection was wrong on this one.
        It was selecting the time series service instead of the geo series.
        """
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m",
            "--variable",
            "zos",
            "--start-datetime",
            "2023-08-01T21:00:00",
            "--end-datetime",
            "2023-08-01T23:00:00",
            "--minimum-longitude",
            "183.383677",
            "--maximum-longitude",
            "350.828210",
            "--minimum-latitude",
            "-78.271870",
            "--maximum-latitude",
            "78.000000",
            "--minimum-depth",
            "0.49402499198913574",
            "--maximum-depth",
            "0.49402499198913574",
            "--log-level",
            "DEBUG",
            "--dry-run",
        ]

        self.output = execute_in_terminal(command)
        assert self.output.returncode == 0
        assert b'Selected service: "arco-geo-series"' in self.output.stderr


def test_close_call_between_services():
    """
    This test is to check the close call between the geo series and time series services.
    The old code was selecting the geo series service instead of the (correct) time series.
    """  # noqa
    command = [
        "copernicusmarine",
        "subset",
        "-i",
        "cmems_mod_glo_phy-all_my_0.25deg_P1D-m",
        "-v",
        "thetao_oras",
        "-v",
        "uo_oras",
        "-v",
        "vo_oras",
        "-v",
        "so_oras",
        "-v",
        "zos_oras",
        "--minimum-longitude",
        "50",
        "--maximum-longitude",
        "110",
        "--minimum-latitude",
        "-10",
        "--maximum-latitude",
        "30",
        "--start-datetime",
        "2010-03-01T00:00:00",
        "--end-datetime",
        "2010-06-30T00:00:00",
        "--minimum-depth",
        "0.5057600140571594",
        "--maximum-depth",
        "500",
        "--output-filename",
        "ocean_subsurface_2010_0_500.nc",
        "--dry-run",
        "--log-level",
        "DEBUG",
    ]
    output = execute_in_terminal(command)
    assert output.returncode == 0
    assert b'Selected service: "arco-time-series"' in output.stderr
