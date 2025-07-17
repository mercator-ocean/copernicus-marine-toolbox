import json

import xarray

from copernicusmarine import ResponseSubset, subset
from tests.test_utils import (
    execute_in_terminal,
    main_checks_when_file_is_downloaded,
)


class TestCFCompliance:
    def test_subset_open_cfcompliant(self, tmp_path, snapshot):
        dataset_id = "cmems_mod_nws_bgc-pft_my_7km-3D-pico_P1M-m"
        response = self.if_i_subset_a_dataset(dataset_id, tmp_path, "pico")
        self.then_it_is_cf_compliant(dataset_id, response, snapshot)

    def test_subset_with_warns_cfcompliant(self, tmp_path, snapshot):
        dataset_id = (
            "cmems_obs-sst_med_phy-sst_nrt_diurnal-oi-0.0625deg_PT1H-m"
        )
        response = self.if_i_subset_a_dataset(
            dataset_id,
            tmp_path,
            "analysed_sst",
        )
        self.then_it_is_cf_compliant(
            dataset_id,
            response,
            snapshot,
        )

    def test_valid_ranges_correct(self, tmp_path, snapshot):
        dataset_id = "cmems_obs-sst_glo_phy_l3s_pir_P1D-m"
        response = self.if_i_subset_a_dataset(
            dataset_id,
            tmp_path,
            "sea_surface_temperature",
            start_datetime="2025-05-02",
            end_datetime="2025-05-03",
        )
        self.then_it_is_cf_compliant(
            dataset_id,
            response,
            snapshot,
        )

    def if_i_subset_a_dataset(
        self,
        dataset_id,
        tmp_path,
        variable,
        start_datetime="2022-01-01T00:00:00",
        end_datetime="2022-01-05T00:00:00",
    ) -> ResponseSubset:
        response = subset(
            dataset_id=dataset_id,
            variables=[variable],
            output_directory=tmp_path,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        assert (response.file_path).exists()
        main_checks_when_file_is_downloaded(
            response.file_path, response.model_dump()
        )
        return response

    def then_it_is_cf_compliant(
        self,
        dataset_id: str,
        response_subset: ResponseSubset,
        snapshot,
    ) -> None:
        dataset = xarray.open_dataset(response_subset.file_path)
        cf_convention = dataset.attrs["Conventions"][-3:]
        if cf_convention < "1.6":
            cf_convention = "1.6"
        result_compliance_path = f"{response_subset.output_directory}"
        f"/{dataset_id}_cf_complicance_checked.json"
        command = [
            "compliance-checker",
            f"--test=cf:{cf_convention}",
            str(response_subset.file_path),
            "-f",
            "json",
            "-o",
            result_compliance_path,
        ]
        execute_in_terminal(command)

        with open(result_compliance_path) as f:
            data = json.load(f)
            list_msgs = []
            for diccionari in data[f"cf:{cf_convention}"]["all_priorities"]:
                if len(diccionari["msgs"]) > 0:
                    list_msgs.append(diccionari["name"])
                    list_msgs.append(diccionari["msgs"])

            assert dataset_id == snapshot
            assert data[f"cf:{cf_convention}"]["scored_points"] == snapshot
            assert data[f"cf:{cf_convention}"]["possible_points"] == snapshot
            assert list_msgs == snapshot
