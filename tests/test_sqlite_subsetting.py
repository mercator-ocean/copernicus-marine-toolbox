from tests.test_utils import execute_in_terminal


class TestSqliteSubsetting:
    def test_sqlite_subsetting_not_supported_yet(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT1S",
            "--variable",
            "VAVH",
            "--start-datetime",
            "2024-01-01T00:00:00",
            "--end-datetime",
            "2024-01-01T03:00:00",
        ]
        self.output = execute_in_terminal(command)
        assert (
            b"Format not supported: Subsetting format type sqlite not supported yet."
            in self.output.stderr
        )
        assert self.output.returncode == 1

    def test_sqlite_subsetting_not_supported_yet_even_when_force_service(self):
        command = [
            "copernicusmarine",
            "subset",
            "--dataset-id",
            "cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT1S",
            "--variable",
            "VAVH",
            "--start-datetime",
            "2024-01-01T00:00:00",
            "--end-datetime",
            "2024-01-01T03:00:00",
            "--service",
            "geoseries",
        ]
        self.output = execute_in_terminal(command)
        assert (
            b"Format not supported: Subsetting format type sqlite not supported yet."
            in self.output.stderr
        )
        assert self.output.returncode == 1
