from tests.test_utils import execute_in_terminal

DATASETS_IDS = {
    # [-180, 180[ v = thetao
    "full_lon": "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
    # [-179.9791717529297, 179.9791717529297] v = CHL
    "not_centered": "cmems_obs-oc_glo_bgc-plankton_nrt_l4-gapfree-multi-4km_P1D",
    "mediterranean": "med-cmcc-cur-rean-h",  # [-6.0, 36.29166793823242] v = uo
    # [-30.0625, 42.0625] v = sla
    "euro": "cmems_obs-sl_eur_phy-ssh_my_allsat-l4-duacs-0.125deg_P1D",
    # [-20.975, 12.975] v = analysed_sst
    "IBI": "cmems-IFREMER-ATL-SST-L4-REP-OBS_FULL_TIME_SERIE",
    # [-179.75, 180] v = eastward_sea_ice_velocity
    "antartic": "cmems_obs-si_ant_physic_my_drift-amsr_P2D",
}


DATASETS_VARS = {
    "full_lon": "thetao",
    "not_centered": "CHL",
    "mediterranean": "uo",
    "euro": "sla",
    "IBI": "analysed_sst",
    "antartic": "eastward_sea_ice_velocity",
}


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

    def test_subset_longitude_warns(
        self, key, min_lon, max_lon, expected_output
    ):
        dataset_id = DATASETS_IDS[key]

        command1 = self._build_custom_command(
            dataset_id, DATASETS_VARS[key], min_lon, max_lon, "nearest"
        )
        command2 = self._build_custom_command(
            dataset_id, DATASETS_VARS[key], min_lon, max_lon, "strict"
        )
        self.output1 = execute_in_terminal(command1, input=b"n")
        self.output2 = execute_in_terminal(command2, input=b"n")

        string = (
            f"Some or all of your subset selection [{min_lon:.{1}f},"
            f" {max_lon:.{1}f}] for the longitude dimension  exceed the dataset"
        ).encode(encoding="utf-8")
        if expected_output == "no_warns":
            assert (
                string not in self.output1.stderr
            )  # should there still be some kind of warning?
            assert string not in self.output2.stderr
        else:
            assert b"WARNING" in self.output1.stderr
            assert (
                string in self.output1.stderr
            )  # should there still be some kind of warning?
            assert b"ERROR" in self.output2.stderr
            assert string in self.output2.stderr

    def test_subset_all_longitudes_prova(self):
        self.test_subset_longitude_warns(
            "mediterranean", -180.0, 180.0, "warn"
        )

    def test_subset_all_longitudes(self):
        self.test_subset_longitude_warns("full_lon", -180.0, 180.0, "no_warns")
        self.test_subset_longitude_warns("full_lon", -40.0, 70.0, "no_warns")
        self.test_subset_longitude_warns("full_lon", -150.0, 180.0, "no_warns")
        self.test_subset_longitude_warns(
            "full_lon", -149.99, 180.0, "no_warns"
        )
        self.test_subset_longitude_warns("full_lon", -179.9, 179.9, "no_warns")
        self.test_subset_longitude_warns(
            "not_centered", -180.0, 180.0, "no_warns"
        )
        self.test_subset_longitude_warns(
            "not_centered", -179.9, 179.9, "no_warns"
        )
        self.test_subset_longitude_warns("antartic", -180.0, 180.0, "no_warns")
        self.test_subset_longitude_warns(
            "mediterranean", -180.0, 180.0, "warn"
        )
        self.test_subset_longitude_warns("mediterranean", -7.0, 40.0, "warn")
        self.test_subset_longitude_warns(
            "mediterranean", -5.0, 30.0, "no_warns"
        )
        self.test_subset_longitude_warns("euro", -180.0, 170.0, "warn")
        self.test_subset_longitude_warns("euro", -20.0, 170.0, "warn")
        self.test_subset_longitude_warns("euro", -25.0, 30.0, "no_warns")
        self.test_subset_longitude_warns("IBI", -179.9, 170.0, "warn")
        self.test_subset_longitude_warns("IBI", -25.0, 5.0, "warn")
        self.test_subset_longitude_warns("IBI", 5.0, 13.0, "warn")
        self.test_subset_longitude_warns("IBI", -20.0, 13.0, "no_warns")
