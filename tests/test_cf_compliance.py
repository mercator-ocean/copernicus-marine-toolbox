import json

from copernicusmarine import subset
from tests.test_utils import execute_in_terminal


class TestCFCompliance:
    def test_subset_open(self, tmp_path, snapshot):
        dataset_id = "cmems_mod_nws_bgc-pft_my_7km-3D-pico_P1M-m"
        self.if_I_subset_a_dataset(dataset_id, tmp_path)
        self.then_it_is_cf_compliant(dataset_id, tmp_path, snapshot)

    def if_I_subset_a_dataset(self, dataset_id, tmp_path):
        subset(
            dataset_id=dataset_id,
            variables=["pico"],
            output_directory=tmp_path,
            output_filename="output_1.nc",
            start_datetime="2020-01-01T00:00:00",
            end_datetime="2020-01-05T00:00:00",
            force_download=True,
        )
        assert (tmp_path / "output_1.nc").exists()

    def then_it_is_cf_compliant(self, dataset_id, tmp_path, snapshot):
        dataset_id = dataset_id
        CF_convention = "1.7"
        command = [
            "compliance-checker",
            f"--test=cf:{CF_convention}",
            f"{tmp_path}/output_1.nc",
            "--criteria",
            "lenient",
            "-f",
            "json",
            "-o",
            f"{tmp_path}/output_1_checked.json",
        ]
        execute_in_terminal(command)

        f = open(f"{tmp_path}/output_1_checked.json")
        data = json.load(f)

        assert data == snapshot
