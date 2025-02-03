import json

import xarray

from copernicusmarine import subset
from tests.test_utils import (
    execute_in_terminal,
    main_checks_when_file_is_downloaded,
)


class TestCFCompliance:
    def test_subset_open(self, tmp_path, snapshot):
        dataset_id = "cmems_mod_nws_bgc-pft_my_7km-3D-pico_P1M-m"
        self.if_I_subset_a_dataset(dataset_id, tmp_path, "output_1.nc", "pico")
        self.then_it_is_cf_compliant(
            dataset_id, tmp_path, snapshot, "output_1"
        )

    def test_subset_with_warns(self, tmp_path, snapshot):
        dataset_id = (
            "cmems_obs-sst_med_phy-sst_nrt_diurnal-oi-0.0625deg_PT1H-m"
        )
        self.if_I_subset_a_dataset(
            dataset_id,
            tmp_path,
            "output_2.nc",
            "analysed_sst",
        )
        self.then_it_is_cf_compliant(
            dataset_id, tmp_path, snapshot, "output_2"
        )

    def if_I_subset_a_dataset(
        self, dataset_id, tmp_path, output_filename, variable
    ):
        response = subset(
            dataset_id=dataset_id,
            variables=[variable],
            output_directory=tmp_path,
            output_filename=output_filename,
            start_datetime="2022-01-01T00:00:00",
            end_datetime="2022-01-05T00:00:00",
        )
        assert (tmp_path / output_filename).exists()
        main_checks_when_file_is_downloaded(
            tmp_path / output_filename, dict(response)
        )

    def then_it_is_cf_compliant(
        self, dataset_id, tmp_path, snapshot, output_filename
    ):
        dataset_id = dataset_id
        dataset = xarray.open_dataset(f"{tmp_path}/{output_filename}.nc")
        CF_convention = dataset.attrs["Conventions"][-3:]
        if CF_convention < "1.6":
            CF_convention = "1.6"
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

        list_msgs = []
        for diccionari in data[f"cf:{CF_convention}"]["all_priorities"]:
            if len(diccionari["msgs"]) > 0:
                list_msgs.append(diccionari["name"])
                list_msgs.append(diccionari["msgs"])

        assert dataset_id == snapshot
        assert data[f"cf:{CF_convention}"]["scored_points"] == snapshot
        assert data[f"cf:{CF_convention}"]["possible_points"] == snapshot
        assert list_msgs == snapshot
