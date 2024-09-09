import json

from copernicusmarine import subset
from tests.test_utils import execute_in_terminal


class TestCFCompliance:
    def test_subset_open(self, tmp_path, snapshot):
        dataset_id = "cmems_mod_nws_bgc-pft_my_7km-3D-pico_P1M-m"
        self.if_I_subset_a_dataset(dataset_id, tmp_path, "output_1.nc", "pico")
        self.then_it_is_cf_compliant(
            dataset_id, tmp_path, snapshot, "output_1"
        )

    def test_subset_with_warns(self, tmp_path, snapshot):
        dataset_id = "cmems_obs-sst_med_phy_my_l3s_P1D-m"
        self.if_I_subset_a_dataset(
            dataset_id,
            tmp_path,
            "output_2.nc",
            "adjusted_sea_surface_temperature",
        )
        self.then_it_is_cf_compliant(
            dataset_id, tmp_path, snapshot, "output_2"
        )

    def if_I_subset_a_dataset(
        self, dataset_id, tmp_path, output_filename, variable
    ):
        subset(
            dataset_id=dataset_id,
            variables=[variable],
            output_directory=tmp_path,
            output_filename=output_filename,
            start_datetime="2022-01-01T00:00:00",
            end_datetime="2022-01-05T00:00:00",
            force_download=True,
        )
        assert (tmp_path / output_filename).exists()

    def then_it_is_cf_compliant(
        self, dataset_id, tmp_path, snapshot, output_filename
    ):
        dataset_id = dataset_id
        CF_convention = "1.7"
        command = [
            "compliance-checker",
            f"--test=cf:{CF_convention}",
            f"{tmp_path}/{output_filename}.nc",
            "-f",
            "json",
            "-o",
            f"{tmp_path}/{output_filename}_checked.json",
        ]
        execute_in_terminal(command)

        f = open(f"{tmp_path}/{output_filename}_checked.json")
        data = json.load(f)

        assert data["cf:1.7"]["all_priorities"] == snapshot
        assert data["cf:1.7"]["high_priorities"] == snapshot
