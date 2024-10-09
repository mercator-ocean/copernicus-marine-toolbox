import json

import xarray

from copernicusmarine import subset
from tests.test_utils import execute_in_terminal


class TestCFCompliance:
    def test_subset_open(self, tmp_path, snapshot):
        dataset_id = "cmems_mod_nws_bgc-pft_my_7km-3D-pico_P1M-m"
        output_filename = "output_1.nc"
        self.if_I_subset_a_dataset(
            dataset_id, tmp_path, output_filename, "pico"
        )
        self.then_it_is_cf_compliant(
            dataset_id, tmp_path, snapshot, output_filename
        )

    def test_subset_with_warns(self, tmp_path, snapshot):
        dataset_id = (
            "cmems_obs-sst_med_phy-sst_nrt_diurnal-oi-0.0625deg_PT1H-m"
        )
        output_filename = "output_2.nc"
        self.if_I_subset_a_dataset(
            dataset_id,
            tmp_path,
            output_filename,
            "analysed_sst",
        )
        self.then_it_is_cf_compliant(
            dataset_id, tmp_path, snapshot, output_filename
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
        dataset = xarray.open_dataset(f"{tmp_path}/{output_filename}")
        cf_convention = dataset.attrs.get("Conventions")
        if cf_convention:
            cf_convention = cf_convention[-3:]
            if cf_convention < "1.6":
                cf_convention = "1.6"
        else:
            cf_convention = "1.6"
        command = [
            "compliance-checker",
            f"--test=cf:{cf_convention}",
            f"{tmp_path}/{output_filename}",
            "-f",
            "json",
        ]
        self.output = execute_in_terminal(command)

        data = json.loads(self.output.stdout)

        list_msgs = []
        for dictionary in data[f"cf:{cf_convention}"]["all_priorities"]:
            if len(dictionary["msgs"]) > 0:
                list_msgs.append(dictionary["name"])
                list_msgs.append(dictionary["msgs"])

        assert dataset_id == snapshot
        assert data[f"cf:{cf_convention}"]["scored_points"] == snapshot
        assert data[f"cf:{cf_convention}"]["possible_points"] == snapshot
        assert list_msgs == snapshot
