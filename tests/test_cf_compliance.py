import json
import re
from pathlib import Path

import xarray

from copernicusmarine import ResponseSubset, subset
from tests.test_utils import (
    execute_in_terminal,
    main_checks_when_file_is_downloaded,
)


def get_cf_convention(dataset: xarray.Dataset) -> str:
    conventions = dataset.attrs.get("Conventions")
    if conventions:
        pattern = r"CF-(\d+\.\d+)"
        match = re.search(pattern, conventions)
        if match:
            cf_convention = match.group(1)
        else:
            cf_convention = "1.6"
    else:
        cf_convention = "1.6"
    if cf_convention < "1.6":
        cf_convention = "1.6"
    return cf_convention


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
            delete_attributes=[
                "instrument",
                "instrument_type",
                "platform",
                "platform_type",
                "band",
            ],
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
        delete_attributes: list[str] = [],
    ) -> None:
        dataset = xarray.open_dataset(response_subset.file_path)
        # delete attributes is a fix for this issue:
        # https://github.com/ioos/compliance-checker/issues/1082
        for attr in delete_attributes:
            if attr in dataset.attrs:
                del dataset.attrs[attr]
        if delete_attributes:
            response_subset.file_path = Path(
                f"{response_subset.file_path}tmp.nc"
            )
            dataset.to_netcdf(
                response_subset.file_path,
                format="NETCDF4",
                engine="netcdf4",
            )
        cf_convention = get_cf_convention(dataset)
        result_compliance_path = (
            f"{response_subset.output_directory}"
            f"/{dataset_id}_cf_complicance_checked.json"
        )
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
